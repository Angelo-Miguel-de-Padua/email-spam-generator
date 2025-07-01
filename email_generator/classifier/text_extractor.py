from bs4 import BeautifulSoup

def extract_text(soup, max_paragraphs=3) -> str:
    parts = []