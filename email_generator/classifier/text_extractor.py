from bs4 import BeautifulSoup

def extract_text(soup, max_paragraphs=3) -> str:
    parts = []

    if soup.title:
        parts.append(soup.title.get_text(strip=True))

    meta = soup.find("meta", attrs={"name": "description"}) 
    if meta and meta.get("content"):
        parts.append(meta["content"])
    
    h1 = soup.find("h1")
    if h1:
        parts.append(h1.get_text(strip=True))

    for p in soup.find_all("p")[:max_paragraphs]:
        text = p.get_text(strip=True)
        if len(text) > 30 and "cookie" not in text.lower():
            parts.append(text)