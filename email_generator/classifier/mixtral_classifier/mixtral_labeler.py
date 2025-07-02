import os
import json
import requests
from dotenv import load_dotenv
from email_generator.classifier.mixtral_classifier.mixtral_scraper import scrape_and_extract
from email_generator.utils.prompt_template import build_prompt
from email_generator.utils.domain_utils import normalize_domain

load_dotenv()

OLLAMA_MODEL_NAME = "mixtral:8x7b-instruct-v0.1-q4_K_M"
OLLAMA_ENDPOINT = os.getenv("OLLAMA_ENDPOINT")
scraped_file = "../resources/scraped_data.json"
labeled_file = "../resources/labeled_data.json"

def call_mixtral(prompt: str, retries: int = 2) -> str:
    for attempt in range(retries + 1):
        try:
            response = requests.post(
                OLLAMA_ENDPOINT,
                json={
                    "model": OLLAMA_MODEL_NAME,
                    "prompt": prompt,
                    "stream": False
                }
            )
            if response.status_code == 200:
                return response.json()["response"].strip().lower()
            else:
                raise Exception(f"Status {response.status_code}: {response.text}")
        except Exception as e:
            if attempt == retries:
                raise Exception(f"Mixtral failed after {retries + 1} tries: {e}")

def ask_mixtral(text: str) -> dict:
    prompt = (
        build_prompt(text) +
        "\n\nRespond in this format:\ncategory: <category>\nconfidence: <1-10>"
    )
    response = call_mixtral(prompt)

    lines = response.splitlines()
    result = {"category": "unknown", "confidence": "low"}
    for line in lines:
        if line.startswith("category:"):
            result["category"] = line.split(":", 1)[1].strip()
        elif line.startswith("confidence:"):
            conf = line.split(":", 1)[1].strip()
            result["confidence"] = conf if conf.isdigit() else "low"

    return result

def classify_domain_fallback(domain: str) -> dict:
    prompt = f"""
You are a domain classification expert.

Classify the website based on its domain name:
Domain: {domain}

Choose only one of the following categories:
ecommerce, education, news, jobs, finance, tech, travel, health, media, social, forum, sports, gaming, cloud, ai, crypto, security, real_estate, government, adult

If you are unsure or cannot determine the category, respond with: unknown

Respond in this format:
category: <category>
confidence: <1-10>
"""
    
    response = call_mixtral(prompt)

    result = {"category": "unknown", "confidence": "low"}
    lines = response.splitlines()
    for line in lines:
        if line.startswith("category:"):
            result["category"] = line.split(":", 1)[1].strip()
        elif line.startswith("confidence:"):
            conf = line.split(":", 1)[1].strip()
            result["confidence"] = conf if conf.isdigit() else "low"
    
    return result

def get_scraped_data(domain: str, scraped_file=scraped_file) -> dict | None:
    if not os.path.exists(scraped_file):
        return None
    with open(scraped_file, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return None
        return next((entry for entry in data if normalize_domain(entry["domain"]) == domain), None)
    
def is_domain_labeled(domain: str, labeled_file=labeled_file) -> bool:
    if not os.path.exists(labeled_file):
        return False
    with open(labeled_file, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return False
        return any(normalize_domain(entry["domain"]) == domain for entry in data)