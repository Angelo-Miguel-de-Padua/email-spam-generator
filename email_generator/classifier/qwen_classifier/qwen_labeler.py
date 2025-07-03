import os
import json
import requests
from dotenv import load_dotenv
from email_generator.classifier.qwen_classifier.qwen_scraper import scrape_and_extract
from email_generator.utils.prompt_template import build_prompt
from email_generator.utils.domain_utils import normalize_domain
from email_generator.utils.text_filters import useless_text

load_dotenv()

OLLAMA_MODEL_NAME = "qwen:7b-chat-q4_0"
OLLAMA_ENDPOINT = os.getenv("OLLAMA_ENDPOINT")
scraped_file = "resources/scraped_data.json"
labeled_file = "resources/labeled_data.json"

def call_qwen(prompt: str, retries: int = 2) -> str:
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
                raise Exception(f"Qwen failed after {retries + 1} tries: {e}")

def ask_qwen(text: str) -> dict:
    prompt = (
        build_prompt(text) +
        "\n\nRespond in this format:\n"
        "category: <category>\n"
        "confidence: <1-10>\n"
        "explanation: <why this category>"
    )
    response = call_qwen(prompt)

    lines = response.splitlines()
    result = {"category": "unknown", "confidence": "low", "explanation": ""}
    for line in lines:
        if line.startswith("category:"):
            result["category"] = line.split(":", 1)[1].strip()
        elif line.startswith("confidence:"):
            conf = line.split(":", 1)[1].strip()
            result["confidence"] = conf if conf.isdigit() else "low"
        elif line.startswith("explanation:"):
            result["explanation"] = line.split(":", 1)[1].strip()

    return result

def classify_domain_fallback(domain: str) -> dict:
    print(f"[Qwen fallback] Classifying domain only: {domain}")
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
explanation: <why this category>
"""
    
    response = call_qwen(prompt)

    result = {"category": "unknown", "confidence": "low", "explanation": ""}
    lines = response.splitlines()
    for line in lines:
        if line.startswith("category:"):
            result["category"] = line.split(":", 1)[1].strip()
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
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return None
        return next((entry for entry in data if normalize_domain(entry["domain"]) == domain), None)
    
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
    
def label_domain(domain: str, labeled_file=labeled_file, scraped_file=scraped_file) -> dict | None:
    domain = normalize_domain(domain)

    if is_domain_labeled(domain, labeled_file):
        print(f"[Skip] {domain} already labeled.")
        return None
    
    result = get_scraped_data(domain, scraped_file)
    if result is None:
        print(f"[Error] {domain} not found in scraped data.")
        return {
            "domain": domain,
            "category": "error",
            "error": "Domain not found in scraped_data.json"    
        }
    
    try:
        error = result.get("error")
        text = result.get("text", "")

        if error or useless_text(text):
            print (f"[Fallback] Classifying with domain only: {domain}")
            classification = classify_domain_fallback(domain)
            source = "qwen-fallback"
        else:
            print(f"[Qwen] Prompting with extracted text: {domain}")
            classification = ask_qwen(result["text"])
            source = "qwen"

        data = {
            "domain": domain,
            "text": text,
            "category": classification["category"],
            "confidence": classification["confidence"],
            "explanation": classification.get("explanation", ""),
            "source": source
        }

        print (f"[Labeled] {domain} -> {data['category']} ({source})")

        with open(labeled_file, "a", encoding="utf-8") as f:
            json.dump(data, f)
            f.write("\n")
        
        return data
    
    except Exception as e:
        print (f"[Exception] {domain}: {str(e)}")
        return {
            "domain": domain,
            "category": "error",
            "error": str(e)
        }
