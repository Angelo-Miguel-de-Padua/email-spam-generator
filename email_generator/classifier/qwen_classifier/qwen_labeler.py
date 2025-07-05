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
scraped_file = "resources/scraped_data.jsonl"
labeled_file = "resources/labeled_data.jsonl"

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
                if response.status_code == 200:
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
        if line.startswith("subcategory:"):
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

def get_scraped_data(domain: str, scraped_file=scraped_file) -> dict | None:
    if not os.path.exists(scraped_file):
        return None
    with open(scraped_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    if normalize_domain(entry["domain"]) == domain:
                        return entry 
                except json.JSONDecodeError:
                    continue
    return None

def is_domain_labeled(domain: str, labeled_file=labeled_file) -> bool:
    if not os.path.exists(labeled_file):
        return False
    with open(labeled_file, "r", encoding="utf-8") as f:
        for line in f:
            try:
                entry = json.loads(line)
                if normalize_domain(entry["domain"]) == domain:
                    return True
            except json.JSONDecodeError:
                continue
    return False

async def label_domain(domain: str, labeled_file=labeled_file, scraped_file=scraped_file) -> ClassificationResult:
    domain = normalize_domain(domain)

    if is_domain_labeled(domain, labeled_file):
        return ClassificationResult(
            domain=domain,
            category="error",
            error="Already labeled"
        )

    result = get_scraped_data(domain, scraped_file)
    if result is None:
        return ClassificationResult(
            domain=domain,
            category="error",
            error="Domain not found in scraped_data.json"
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

        append_json_safely(result_obj.to_dict(), labeled_file)
        return result_obj

    except Exception as e:
        return ClassificationResult(
            domain=domain,
            category="error",
            error=str(e),
            last_classified=time.time()
        )