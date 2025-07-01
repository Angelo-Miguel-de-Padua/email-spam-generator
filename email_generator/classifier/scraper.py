import requests
import random
from bs4 import BeautifulSoup
from email_generator.classifier.classifier import classify_text
from email_generator.classifier.text_extractor import extract_text

def random_user_agent() -> str:
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"
    ]
    return random.choice(user_agents)

def scraper(domain: str) -> dict:
    url = f"http://{domain}"

    try:
        response = requests.get(url, timeout=5, headers={"User-Agent": random_user_agent()})
        soup = BeautifulSoup(response.text, "html.parser")

        base_text = extract_text(soup, max_paragraphs=1)
        category, info = classify_text(base_text)

        if info["is_tied"] or info["confidence"] == "low":
            expanded_text = extract_text(soup, max_paragraphs=5)
            category, info = classify_text(expanded_text)

        return {
            "domain": domain,
            "category": category,
            "confidence": info["confidence"],
            "is_tied": info["is_tied"],
            "scores": info["scores"]
        }
    
    except Exception as e:
        return {
            "domain": domain,
            "category": "error",
            "error": str(e)
        }

