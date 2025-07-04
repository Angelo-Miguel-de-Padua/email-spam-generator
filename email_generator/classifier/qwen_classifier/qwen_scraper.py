import random
import os
import json
import time
from contextlib import contextmanager
from bs4 import BeautifulSoup
from email_generator.utils.text_extractor import extract_text
from email_generator.classifier.security.cloud_metadata import check_domain_safety
from email_generator.utils.domain_utils import is_valid_domain, normalize_domain
from email_generator.utils.file_utils import append_json_safely
from email_generator.utils.rate_limiter import apply_rate_limit, get_adaptive_delay
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

@contextmanager
def get_browser_page():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                user_agent=random_user_agent(),
                viewport={"width": random.randint(1280, 1600), "height": random.randint(720, 1000)},
                locale="en-US",
                timezone_id="America/New York"
            )
            try:
                page = context.new_page()
                try:
                    yield page
                finally:
                    page.close()
            finally:
                context.close()
        finally:
            browser.close()

def scraped_domains(domain: str) -> bool:
    if not os.path.exists(SCRAPED_DOMAINS_FILE):
        return False
    with open(SCRAPED_DOMAINS_FILE, "r", encoding="utf-8") as f:
        for line in f:   
            try:
                entry = json.loads(line.strip())
                if entry.get("domain") == domain:
                    return True
            except json.JSONDecodeError:
                continue
    return False

def store_scrape_results(result: dict):
    append_json_safely(result, SCRAPED_DOMAINS_FILE)

def try_scrape_protocol(url: str, normalized: str, protocol: str) -> dict:
    try:
        with get_browser_page() as page:
            redirect_count = 0
            redirect_exceeded = False

            def handle_response(response):
                nonlocal redirect_count, redirect_exceeded
                if 300 <= response.status < 400:
                    redirect_count += 1
                    if redirect_count > MAX_REDIRECTS:
                        redirect_exceeded = True

            page.on("response", handle_response)

            try:
                start_time = time.time()
                page.goto(url, timeout=10000)
                response_time = time.time() - start_time

                if redirect_exceeded:
                    return {
                        "domain": normalized,
                        "text": "",
                        "error": f"{protocol.upper()} too many redirects (>{MAX_REDIRECTS})"
                    }

                page.wait_for_timeout(random.randint(1000, 2500))
                page.mouse.wheel(0, 3000)
                html = page.content()

                time.sleep(get_adaptive_delay(had_error=False, response_time=response_time))

                max_html_size = 1_000_000 # 1MB
                if len(html) > max_html_size:
                    return {
                        "domain": normalized,
                        "text": "",
                        "error": f"{protocol.upper()} HTML too large ({len(html)} bytes)"
                    }

            except PlaywrightTimeout:
                time.sleep(get_adaptive_delay(had_error=True))
                return None
            except Exception as e:
                time.sleep(get_adaptive_delay(had_error=True))
                if redirect_exceeded:
                    return {
                        "domain": normalized,
                        "text": "",
                        "error": f"{protocol.upper()} too many redirects (>{MAX_REDIRECTS})"
                    }
                else:
                    return None

            if len(html) < 300 or "captcha" in html.lower() or "cloudflare" in html.lower():
                return {
                    "domain": normalized,
                    "text": "",
                    "error": f"{protocol.upper()} suspicious or protected content"
                }

            soup = BeautifulSoup(html, "html.parser")
            extracted_text = extract_text(soup)

            return {
                "domain": normalized,
                "text": extracted_text,
                "error": None
            }

    except Exception as e:
        return None    

def scrape_and_extract(domain: str) -> dict:
    normalized = normalize_domain(domain)

    apply_rate_limit(normalized)

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
        result = try_scrape_protocol(url, normalized, protocol)

        if result is not None:
            store_scrape_results(result)
            return result
    
    result = {
        "domain": normalized,
        "text": "",
        "error": f"Both HTTPS and HTTP failed: Connection/timeout errors"
    }
    store_scrape_results(result)
    return result
       