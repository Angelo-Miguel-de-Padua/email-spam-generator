import random
import os
import json
from bs4 import BeautifulSoup
from email_generator.utils.text_extractor import extract_text
from email_generator.classifier.security.cloud_metadata import check_domain_safety
from email_generator.utils.domain_utils import is_valid_domain, normalize_domain
from email_generator.utils.file_utils import append_json_safely
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

SCRAPED_DOMAINS_FILE = "resources/scraped_data.jsonl"
MAX_REDIRECTS = 5

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
    append_json_safely(result, SCRAPED_DOMAINS_FILE)
    

def scrape_and_extract(domain: str) -> dict:
    normalized = normalize_domain(domain)

    if not is_valid_domain(normalized):
        result = {
            "domain": normalized,
            "text": "",
          "error": "Invalid domain format"
        }
        store_scrape_results(result)
        return result

    if scraped_domains(normalized):
        return None
    
    if not check_domain_safety(normalized):
        result = {
            "domain": normalized,
            "text": "",
            "error": "Blocked: Domain resolved to dangerous internal or metadata IP"
        }
        store_scrape_results(result)
        return result 

    last_error = None

    for protocol in ["https", "http"]:
        url = f"{protocol}://{normalized}"

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

                redirect_count = 0
                redirect_exceeded = False

                def handle_response(response):
                    nonlocal redirect_count, redirect_exceeded
                    if 300 <= response.status < 400:
                        redirect_count += 1
                        if redirect_count > MAX_REDIRECTS:
                            redirect_exceeded = True
                            page.close()
                
                page.on("response", handle_response)

                try:
                    page.goto(url, timeout=10000)
                    
                    if redirect_exceeded:
                        result = {
                            "domain": normalized,
                            "text": "",
                            "error": f"{protocol.upper()} too many redirects (>{MAX_REDIRECTS})"
                        }
                        store_scrape_results(result)
                        browser.close()
                        return result

                    page.wait_for_timeout(random.randint(1000, 2500))
                    page.mouse.wheel(0, 3000)
                    html = page.content()

                    max_html_size = 1_000_000 #1MB
                    if len(html) > max_html_size:
                        result = {
                            "domain": normalized,
                            "text": "",
                            "error": f"{protocol.upper()} HTML too large ({len(html)} bytes)"
                        }
                        store_scrape_results(result)
                        browser.close()
                        return result
                    
                except PlaywrightTimeout:
                    browser.close()
                    continue
                except Exception as e:
                    if redirect_exceeded:
                        result = {
                            "domain": normalized,
                            "text": "",
                            "error": f"{protocol.upper()} too many redirects (>{MAX_REDIRECTS})"
                        }
                        store_scrape_results(result)
                        browser.close()
                        return result
                    else:
                        last_error = str(e)
                        browser.close()
                        continue

                browser.close()

                if len(html) < 300 or "captcha" in html.lower() or "cloudflare" in html.lower():
                    result = {
                        "domain": normalized,
                        "text": "",
                        "error": f"{protocol.upper()} suspicious or protected content"
                    }
                    store_scrape_results(result)
                    return result

                soup = BeautifulSoup(html, "html.parser")
                extracted_text = extract_text(soup)

                result = {
                    "domain": normalized,
                    "text": extracted_text,
                    "error": None
                }
                store_scrape_results(result)
                return result

        except Exception as e:
            last_error = str(e)
            continue

    result = {
        "domain": normalized,
        "text": "",
        "error": f"Both HTTPS and HTTP failed: {last_error}"
    }
    store_scrape_results(result)
    return result