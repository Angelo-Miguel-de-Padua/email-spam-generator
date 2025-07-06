import os
import json
import asyncio
import aiohttp
from aiohttp import TCPConnector
import requests
import time
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import Optional
from email_generator.classifier.qwen_classifier.qwen_scraper import scrape_and_extract
from email_generator.database.supabase_client import db
from email_generator.utils.prompt_template import build_prompt
from email_generator.utils.domain_utils import normalize_domain
from email_generator.utils.text_filters import useless_text
from email_generator.utils.file_utils import append_json_safely

load_dotenv()

@dataclass
class ClassificationResult:
    domain: str
    category: str
    subcategory: str = "unknown"
    confidence: int = 0
    explanation: str = ""
    source: str = ""
    text: str = ""
    error: Optional[str] = None
    last_classified: Optional[float] = None

    def to_dict(self):
        return {
            "domain": self.domain,
            "category": self.category,
            "subcategory": self.subcategory,
            "confidence": self.confidence,
            "explanation": self.explanation,
            "source": self.source,
            "text": self.text,
            "last_classified": self.last_classified or time.time(),
            **({"error": self.error} if self.error else {})
        }

OLLAMA_MODEL_NAME = "qwen:7b-chat-q4_0"
OLLAMA_ENDPOINT = os.getenv("OLLAMA_ENDPOINT")

session = None

async def initialize_session():
    global session
    if session is None:
        session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),  
            connector=aiohttp.TCPConnector(limit=100) 
        )

async def close_session():
    global session
    if session:
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
                    return data["response"].strip().lower()
                else:
                    error_text = await response.text()
                    raise Exception (f"Status {response.status}: {error_text}")
        except Exception as e:
            if attempt == retries:
                raise Exception(f"Qwen failed after {retries + 1} tries: {e}")
            await asyncio.sleep(0.5)

async def ask_qwen(text: str, domain: str) -> dict:
    prompt = (
        build_prompt(text, domain) +
        "\n\nRespond in this format:\n"
        "category: <category>\n"
        "subcategory: <subcategory>\n"
        "confidence: <1-10>\n"
        "explanation: <why this category>"
    )
    response = await call_qwen(prompt)

    lines = response.splitlines()
    result = {
        "category": "unknown", 
        "subcategory": "unknown",
        "confidence": "low", 
        "explanation": ""
    }
    for line in lines:
        if line.startswith("category:"):
            result["category"] = line.split(":", 1)[1].strip()
        elif line.startswith("subcategory:"):
            result["subcategory"] = line.split(":", 1)[1].strip()
        elif line.startswith("confidence:"):
            conf = line.split(":", 1)[1].strip()
            result["confidence"] = conf if conf.isdigit() else "low"
        elif line.startswith("explanation:"):
            result["explanation"] = line.split(":", 1)[1].strip()
    return result

async def classify_domain_fallback(domain: str) -> dict:
    prompt = f"""
You are a domain classification expert.

Classify the website based on its domain name:
Domain: {domain}

Choose only one of the following categories:
ecommerce, education, news, jobs, finance, tech, travel, health, media, social, forum, sports, gaming, cloud, ai, crypto, security, real_estate, government, adult

For each main category, also include a specific **subcategory**. Examples:
- tech → "search", "hardware", "software", "developer tools"
- ecommerce → "retail", "fashion", "electronics", "marketplace"
- health → "medicine", "fitness", "mental health"
- jobs → "job board", "freelancing", "company career page"
- media → "video", "streaming", "music", "news"

If you're unsure about the correct category or subcategory, respond with:
- category: unknown
- subcategory: unknown

Respond strictly in this format:
category: <category>
subcategory: <subcategory>
confidence: <1-10>
explanation: <why this category>
"""
    response = await call_qwen(prompt)

    result = {
        "category": "unknown",
        "subcategory": "unknown", 
        "confidence": "low", 
        "explanation": ""
    }
    lines = response.splitlines()
    for line in lines:
        if line.startswith("category:"):
            result["category"] = line.split(":", 1)[1].strip()
        elif line.startswith("subcategory:"):
            result["subcategory"] = line.split(":", 1)[1].strip()
        elif line.startswith("confidence:"):
            conf = line.split(":", 1)[1].strip()
            result["confidence"] = conf if conf.isdigit() else "low"
        elif line.startswith("explanation:"):
            result["explanation"] = line.split(":", 1)[1].strip()
    return result

def get_scraped_data(domain: str) -> dict | None:
    domain = normalize_domain(domain)
    domain_data = db.get_domain_data(domain)

    if domain_data and domain_data.get("scraped_text"):
        return {
            "domain": domain,
            "text": domain_data["scraped_text"],
            "error": domain_data.get("scrape_error")
        }
    return None

def is_domain_labeled(domain: str) -> bool:
    domain = normalize_domain(domain)
    return db.is_domain_classified(domain)

async def label_domain(domain: str) -> ClassificationResult:
    domain = normalize_domain(domain)

    if is_domain_labeled(domain):
        return ClassificationResult(
            domain=domain,
            category="error",
            error="Already labeled"
        )

    result = get_scraped_data(domain)
    if result is None:
        return ClassificationResult(
            domain=domain,
            category="error",
            error="Domain not found or not scraped"
        )
    
    try:
        error = result.get("error")
        text = result.get("text", "")

        if error or useless_text(text):
            classification = await classify_domain_fallback(domain)
            source = "qwen-fallback"
        else:
            classification = await ask_qwen(result["text"], domain)
            source = "qwen"

        result_obj = ClassificationResult(
            domain=domain,
            text=text,
            category=classification["category"],
            subcategory=classification.get("subcategory", "unknown"),
            confidence=int(classification["confidence"]) if str(classification["confidence"]).isdigit() else 0,
            explanation=classification.get("explanation", ""),
            source=source,
            last_classified=time.time()
        )

        print(
            f"[Labeled] {domain} -> {result_obj.category} "
            f"subcategory: {result_obj.subcategory} "
            f"confidence: {result_obj.confidence} "
            f"explanation: {result_obj.explanation} ({source})"
        )

        success = db.store_classification_results(
            domain=domain,
            category=result_obj.category,
            subcategory=result_obj.subcategory,
            confidence=result_obj.confidence,
            explanation=result_obj.explanation,
            source=source,
            text=text
        )

        if not success:
            result_obj.error = "Failed to store classification in database"
        
        return result_obj

    except Exception as e:
        error_msg = str(e)

        db.store_classification_results(
            domain=domain,
            category="error",
            error=error_msg
        )

        return ClassificationResult(
            domain=domain,
            category="error",
            error=error_msg,
            last_classified=time.time()
        )
    
async def label_domains_in_batches(domains: list[str], batch_size: int = 20, max_concurrent: int = 10) -> list[ClassificationResult]:
    await initialize_session()
    all_results = []

    for i in range(0, len(domains), batch_size):
        batch = domains[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(domains) + batch_size - 1)//batch_size}")

        semaphore = asyncio.Semaphore(max_concurrent)

        async def process_domain(domain):
            async with semaphore:
                return await label_domain(domain)
            
        batch_results = await asyncio.gather(*[process_domain(d) for d in batch], return_exceptions=True)

        for j, result in enumerate(batch_results):
            if isinstance(result, Exception):
                all_results.append(ClassificationResult(domain=batch[j], category="error", error=str(result)))
            else:
                all_results.append(result)
        
        if i + batch_size < len(domains):
            await asyncio.sleep(1)

    await close_session()
    return all_results

async def classify_unclassified_domains(limit: int = 100) -> list[ClassificationResult]:
    unclassified_domains = db.get_unclassified_domains(limit)
    domain_names = [d["domain"] for d in unclassified_domains]

    if not domain_names:
        print("No unclassified domains found")
        return []
    
    print(f"Found {len(domain_names)} unclassified domains")
    return await label_domains_in_batches(domain_names)

def get_classification_stats():
    stats = db.get_classification_stats()
    print(f"Classification stats:")
    print(f"  Total domains: {stats['total_domains']}")
    print(f"  Scraped domains: {stats['scraped_domains']}")
    print(f"  Classified domains: {stats['classified_domains']}")
    print(f"  Pending classification: {stats['pending_classification']}")
    return stats