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
from email_generator.utils.prompt_template import label_domain_prompt, fallback_label_domain_prompt
from email_generator.utils.domain_utils import normalize_domain
from email_generator.utils.text_filters import useless_text
from email_generator.utils.qwen_utils import initialize_session, close_session, call_qwen

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

async def ask_qwen(text: str, domain: str) -> dict:
    prompt = label_domain_prompt(text, domain)

    try:
        response = await call_qwen(prompt)
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
    prompt = fallback_label_domain_prompt(domain)

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

    if not force and is_domain_labeled(domain):
        logger.info(f"Domain {domain} is already labeled, skipping")
        return ClassificationResult(
            domain=domain,
            category="error",
            classifier_error="Already labeled"
        )

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
        force=True 
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