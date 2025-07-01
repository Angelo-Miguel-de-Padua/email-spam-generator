import requests
from bs4 import BeautifulSoup

def scraper(domain: str) -> dict:
    url = f"http://{domain}"

    try:
        response = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(response.text, "html.parser")

        base_text = extract_text(soup, max_paragraphs=1)
        category, info = classify_text(base_text)
        