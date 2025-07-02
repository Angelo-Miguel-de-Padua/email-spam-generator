import random
from bs4 import BeautifulSoup
from email_generator.classifier.keyword_classifier.classifier import classify_text
from email_generator.utils.text_extractor import extract_text
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

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
                    timezone_id="America/New-York",
                    extra_http_headers={
                        "Accept-Language": "en-US,en;q=0.9",
                        "DNT": "1",
                        "Upgrade-Insecure-Requests": "1",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "none",
                        "Sec-Fetch-User": "?1"
                    }
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

                html = page.content()
                browser.close()

                if len(html) < 300 or "captcha" in html.lower() or "cloudflare" in html.lower():
                    return {
                        "domain": domain,
                        "category": "blocked",
                        "error": f"{protocol.upper()} suspicious or protected content"
                    }

            soup = BeautifulSoup(html, "html.parser")

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
            last_error = str(e)
            continue
  
    return {
        "domain": domain,
        "category": "error",
        "error": f"Both HTTPS and HTTP failed: {last_error}"
    }


