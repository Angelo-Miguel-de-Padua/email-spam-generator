import os
import asyncio
import aiohttp
import time
import logging
import json
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import Optional
from email_generator.database.supabase_client import db
from email_generator.utils.prompt_template import build_prompt
from email_generator.utils.domain_utils import normalize_domain
from email_generator.utils.text_filters import useless_text

logger = logging.getLogger(__name__)

load_dotenv()

@dataclass
class ClassificationResult:
    domain: str
    category: str
    subcategory: str = "unknown"
    confidence: int = 0
    explanation: str = ""
    source: str = ""
    classifier_error: Optional[str] = None
    last_classified: Optional[float] = None

    def to_dict(self):
        return {
            "domain": self.domain,
            "category": self.category,
            "subcategory": self.subcategory,
            "confidence": self.confidence,
            "explanation": self.explanation,
            "source": self.source,
            "last_classified": self.last_classified or time.time(),
            **({"classifier_error": self.classifier_error} if self.classifier_error else {})
        }

OLLAMA_MODEL_NAME = os.getenv("OLLAMA_MODEL_NAME")
OLLAMA_ENDPOINT = os.getenv("OLLAMA_ENDPOINT")

session = None
session_lock = asyncio.Lock()

async def initialize_session():
    global session
    async with session_lock:
        if session is None:
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60),
                connector=aiohttp.TCPConnector(limit=100)
            )

async def close_session():
    global session
    if session:
        logger.info("Closing HTTP Session")
        await session.close()
        session = None

async def call_qwen(prompt: str, retries: int = 2) -> str:
    global session
    if session is None:
        await initialize_session()

    for attempt in range(retries + 1):
        try:
            async with session.post(
                OLLAMA_ENDPOINT,
                json={
                    "model": OLLAMA_MODEL_NAME,
                    "prompt": prompt,
                    "stream": False
                }
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data["response"]
                else:
                    error_text = await response.text()
                    logger.warning(f"Qwen API returned status {response.status}: {error_text}")
                    raise Exception (f"Status {response.status}: {error_text}")
        except Exception as e:
            if attempt == retries:
                logger.error(f"Qwen failed after {retries + 1} tries: {e}")
                raise Exception(f"Qwen failed after {retries + 1} tries: {e}")
            logger.warning(f"Qwen attempt {attempt + 1} failed: {e}, retrying...")
            await asyncio.sleep(0.5)

async def ask_qwen(text: str, domain: str) -> dict:
    prompt = (
        build_prompt(text, domain) +
        "\n\nRespond strictly in this JSON format:\n"
        '{\n'
        '  "category": "<category>",\n'
        '  "subcategory": "<subcategory>",\n'
        '  "confidence": <1-10>,\n'
        '  "explanation": "<why this category>"\n'
        '}'
    )

    response = await call_qwen(prompt)

    try:
        result = json.loads(response)
        if not all(key in result for key in ["category", "subcategory", "confidence", "explanation"]):
            raise ValueError("Missing expected fields in Qwen response")
        return result
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(f"Qwen returned invalid JSON: {e}")
        return {
            "category": "unknown",
            "subcategory": "unknown",
            "confidence": 0,
            "explanation": "Failed to parse JSON"
        }

async def classify_domain_fallback(domain: str) -> dict:
    logger.info(f"Using fallback classification for domain: {domain}")
    prompt = f"""
You are a domain classification expert.

Your task is to classify a domain based ONLY on its visible components.
You MUST NOT guess, hallucinate, or infer meanings from words that are NOT explicitly present in the domain string.

### VERY IMPORTANT RULES ###
- Only use letters, tokens, or words that are ACTUALLY PRESENT in the domain.
- Never imagine or hallucinate words that are not there. For example, "adult-machiko.com" does NOT contain "shop" — you cannot pretend it does.
- Break the domain into visible parts first (e.g., "adult", "machiko"), and ONLY use those parts for classification.
- Do NOT assume that common words like "shop", "hub", "pro", "zone", etc. always imply a specific category.
- Be extremely cautious with branded-looking terms or Latin/foreign-derived words (e.g., 'libra', 'memoria', 'lystit').
    - If a word has multiple possible meanings, DO NOT assume one interpretation without strong supporting context.
    - Words like 'libra' and 'memoria' might look tech-related but could just as easily refer to memorial or unrelated services.
- Do NOT rely on single-word cues unless their meaning is clearly unambiguous and well-known.
- Only classify if multiple domain components clearly and consistently point to the same category.
    - "techshop" → okay (tech + shop makes sense)
    - "brainshop" or "machikoshop" → unclear → mark as unknown
- If the domain’s purpose is ambiguous, possibly misleading, or only weakly inferred, mark it as unknown.
- If you do NOT recognize the domain or cannot be confident about its purpose, respond with:
  - category: unknown
  - subcategory: unknown
  - confidence: 0

Only assign a category if you are highly confident (confidence ≥ 8) and can clearly justify it using only the visible domain components.

### Allowed Categories (choose ONE):
ecommerce, education, news, jobs, finance, tech, travel, health, media, social,
forum, sports, gaming, cloud, ai, crypto, security, real_estate, government, adult

### Subcategory examples:
- tech → "search", "hardware", "software", "developer tools"
- ecommerce → "retail", "fashion", "electronics", "marketplace"
- health → "medicine", "fitness", "mental health"
- jobs → "job board", "freelancing", "company career page"
- media → "video", "streaming", "music", "news"

### Response format (JSON only):
{{
    "category": "<category>",
    "subcategory": "<subcategory>",
    "confidence": <1-10>,
    "explanation": "<brief and clear justification>"
}}

### Examples:
Input domain: "adult-machiko.com"  
→ Valid response:  
{{
    "category": "unknown",
    "subcategory": "unknown",
    "confidence": 0,
    "explanation": "The domain does not contain any recognizable keywords or components to confidently classify it."
}}

Input domain: "tech-hardwarehub.com"  
→ Valid response:  
{{
    "category": "tech",
    "subcategory": "hardware",
    "confidence": 9,
    "explanation": "The domain contains 'tech' and 'hardware', which strongly suggest it is a technology-related hardware site."
}}

Now classify the domain: {domain}
"""
    try:
        response = await call_qwen(prompt, retries=1)
        return json.loads(response)
    except Exception as e:
        logger.error(f"Fallback classification failed for {domain}: {e}")
        return {
            "category": "unknown",
            "subcategory": "unknown",
            "confidence": 0,
            "explanation": f"Fallback failed: {e}"
        }

def get_scraped_data(domain: str) -> dict | None:
    domain = normalize_domain(domain)
    domain_data = db.get_domain_data(domain)

    if domain_data and "scraped_text" in domain_data:
        return {
            "domain": domain,
            "scraped_text": domain_data["scraped_text"],
            "error": domain_data.get("scrape_error")
        }
    return None

def is_domain_labeled(domain: str) -> bool:
    domain = normalize_domain(domain)
    is_labeled = db.is_domain_classified(domain)
    return is_labeled

async def label_domain(domain: str, force: bool = False) -> ClassificationResult:
    domain = normalize_domain(domain)
    logger.info(f"Starting classification for domain: {domain}")

    # Skip if already labeled (unless forcing)
    if not force and is_domain_labeled(domain):
        logger.info(f"Domain {domain} is already labeled, skipping")
        return ClassificationResult(
            domain=domain,
            category="error",
            classifier_error="Already labeled"
        )

    # Get scraped content from DB
    result = get_scraped_data(domain)
    if result is None:
        logger.warning(f"Domain {domain} not found in scraped data")
        classification_result = ClassificationResult(
            domain=domain,
            category="error",
            classifier_error="Domain not found or not scraped"
        )
        db.store_classification_results(
            domain=classification_result.domain,
            category=classification_result.category,
            subcategory=classification_result.subcategory,
            confidence=classification_result.confidence,
            explanation=classification_result.explanation,
            source=classification_result.source,
            scraped_text=""
        )
        return classification_result

    scrape_error = result.get("scrape_error")
    scraped_text = result.get("scraped_text", "")

    # Decide whether to use fallback or normal classification
    if scrape_error is not None or "Both protocols failed" in scraped_text or useless_text(scraped_text):
        classification = await classify_domain_fallback(domain)
        source = "qwen-fallback"
    else:
        classification = await ask_qwen(scraped_text, domain)
        source = "qwen"

    try:
        confidence = int(float(classification.get("confidence", 0)))
    except (ValueError, TypeError):
        confidence = 0

    result_obj = ClassificationResult(
        domain=domain,
        category=classification.get("category", "unknown"),
        subcategory=classification.get("subcategory", "unknown"),
        confidence=confidence,
        explanation=classification.get("explanation", ""),
        source=source,
        last_classified=time.time()
    )

    logger.info(
        f"Classified {domain} -> {result_obj.category} "
        f"(subcategory: {result_obj.subcategory}, "
        f"confidence: {result_obj.confidence}, "
        f"source: {source})"
    )

    success = db.store_classification_results(
        domain=result_obj.domain,
        category=result_obj.category,
        subcategory=result_obj.subcategory,
        confidence=result_obj.confidence,
        explanation=result_obj.explanation,
        source=result_obj.source,
        scraped_text=scraped_text
    )
    if not success:
        logger.error(f"Failed to store classification for {domain} in database")
        result_obj.classifier_error = "Failed to store classification in database"

    return result_obj

async def label_domains_in_batches(domains: list[str], batch_size: int = 20, max_concurrent: int = 10, force: bool = False) -> list[ClassificationResult]:
    logger.info(f"Starting batch processing of {len(domains)} domains (batch_size: {batch_size}, max_concurrent: {max_concurrent})")
    await initialize_session()
    all_results = []

    for i in range(0, len(domains), batch_size):
        batch = domains[i:i + batch_size]
        batch_num = i//batch_size + 1
        total_batches = (len(domains) + batch_size - 1)//batch_size

        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} domains)")

        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_domain(domain):
            async with semaphore:
                return await label_domain(domain, force=force)
            
        batch_results = await asyncio.gather(*[process_domain(d) for d in batch], return_exceptions=True)

        for j, result in enumerate(batch_results):
            if isinstance(result, Exception):
                logger.error(f"Exception processing domain {batch[j]}: {result}")
                all_results.append(ClassificationResult(domain=batch[j], category="error", classifier_error=str(result)))
            else:
                all_results.append(result)
        
        logger.info(f"Completed batch {batch_num}/{total_batches}")
        
        if i + batch_size < len(domains):
            logger.debug("Sleeping 1 second between batches")
            await asyncio.sleep(1)

    await close_session()
    logger.info(f"Completed processing all {len(domains)} domains")
    return all_results

async def classify_unclassified_domains(limit: int = 10000) -> list[ClassificationResult]:
    logger.info(f"Getting unclassified domains (limit: {limit})")
    unclassified_domains = db.get_unclassified_domains(limit)
    domain_names = [d["domain"] for d in unclassified_domains]

    if not domain_names:
        logger.info("No unclassified domains found")
        return []
    
    logger.info(f"Found {len(domain_names)} unclassified domains")
    return await label_domains_in_batches(domain_names)

async def retry_failed_classifications(limit: int = 1000, batch_size: int = 20, max_concurrent: int = 10) -> list[ClassificationResult]:
    failed = db.retry_failed_domains(limit=limit)  # Now only protocol-failed
    domain_names = [d["domain"] for d in failed]

    if not domain_names:
        return []

    return await label_domains_in_batches(
        domain_names,
        batch_size=batch_size,
        max_concurrent=max_concurrent,
        force=True  # Force reclassification
    )

async def retry_low_confidence_classifications(
        limit: int = 1000,
        batch_size: int = 20,
        max_concurrent: int = 10,
        min_confidence: int = 8
) -> list[ClassificationResult]:
    low_conf_domains = db.get_low_confidence_domains(limit=limit)
    if not low_conf_domains:
        logger.info("No low confidence domains to retry")
        return []
    
    domain_names = [row["domain"] for row in low_conf_domains]

    results = await label_domains_in_batches(domain_names, batch_size=batch_size, max_concurrent=max_concurrent, force=True)

    final_results = []
    for res in results:
        if res.category in ("error", "unknown"):
            continue
        if res.confidence < min_confidence:
            logger.info(f"{res.domain} still below confidence threshold ({res.confidence}), will retry later")
        final_results.append(res)
    
    return final_results

def get_classification_stats():
    logger.info("Getting classification statistics")
    stats = db.get_classification_stats()
    logger.info(f"Classification stats: "
               f"Total: {stats['total_domains']}, "
               f"Scraped: {stats['scraped_domains']}, "
               f"Classified: {stats['classified_domains']}, "
               f"Pending: {stats['pending_classification']}")
    return stats