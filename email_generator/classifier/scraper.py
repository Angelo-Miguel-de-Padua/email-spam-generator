import requests

def scraper(domain: str) -> dict:
    url = f"http://{domain}"