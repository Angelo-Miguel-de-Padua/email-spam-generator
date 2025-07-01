from bs4 import BeautifulSoup

def extract_text(soup, max_paragraphs=3) -> str:
    parts = []

    if soup.title:
        parts.append(soup.title.get_text(strip=True))