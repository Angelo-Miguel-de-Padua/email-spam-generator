import requests
from bs4 import BeautifulSoup

def scraper(domain: str) -> dict:
    url = f"http://{domain}"

    try:
        response = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
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

