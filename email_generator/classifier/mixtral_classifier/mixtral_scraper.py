import random
import os
import json
from bs4 import BeautifulSoup
from email_generator.utils.text_extractor import extract_text
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

SCRAPED_DOMAINS_FILE = "labeled_domains.json"

def random_user_agent() -> str:
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"
    ]
    return random.choice(user_agents)

def scraped_domains(domain: str) -> bool:
    if not os.path.exists(SCRAPED_DOMAINS_FILE):
        return False
    with open(SCRAPED_DOMAINS_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return False
        return any(entry["domain"] == domain for entry in data)

def store_scrape_results(result: dict):
    if not os.path.exists(SCRAPED_DOMAINS_FILE):
        with open(SCRAPED_DOMAINS_FILE, "w", encoding="utf-8") as f:
            json.dump([result], f, indent=2)
    else:
        with open(SCRAPED_DOMAINS_FILE, "r+", encoding="utf+8") as f:
            data = json.load(f)
            data.append(result)
            f.seek(0)
            json.dump(data, f, indent=2)

def scrape_and_extract(domain: str) -> dict:
    if scraped_domains(domain):
        return None

    last_error = None

    for protocol in ["https", "http"]:
        url = f"{protocol}://{domain}"

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=random_user_agent(),
                    viewport={"width": random.randint(1280, 1600), "height": random.randint(720, 1000)},
                    locale="en-US",
                    timezone_id="America/New_York"
                )
                page = context.new_page()

                try:
                    page.goto(url, timeout=10000)
                    page.wait_for_timeout(random.randint(1000, 2500))
                    page.mouse.wheel(0, 3000)
                    html = page.content()
                except PlaywrightTimeout:
                    browser.close()
                    continue
                except Exception as e:
                    last_error = str(e)
                    browser.close()
                    continue

                browser.close()

                if len(html) < 300 or "captcha" in html.lower() or "cloudflare" in html.lower():
                    return {
                        "domain": domain,
                        "text": "",
                        "error": f"{protocol.upper()} suspicious or protected content"
                    }

                soup = BeautifulSoup(html, "html.parser")
                extracted_text = extract_text(soup)

                result = {
                    "domain": domain,
                    "text": extracted_text,
                    "error": None
                }
                store_scrape_results(result)
                return result

        except Exception as e:
            last_error = str(e)
            continue

    result = {
        "domain": domain,
        "text": "",
        "error": f"Both HTTPS and HTTP failed: {last_error}"
    }
    store_scrape_results(result)
    return (result)
