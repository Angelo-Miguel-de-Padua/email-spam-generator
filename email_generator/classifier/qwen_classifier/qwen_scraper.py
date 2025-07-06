import random
import asyncio
import logging
from dataclasses import dataclass
from contextlib import asynccontextmanager
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from typing import Optional
from email_generator.utils.text_extractor import extract_text
from email_generator.classifier.security.cloud_metadata import check_domain_safety
from email_generator.utils.domain_utils import is_valid_domain, normalize_domain
from email_generator.utils.rate_limiter import apply_rate_limit, get_adaptive_delay
from email_generator.utils.robots_util import is_scraping_allowed
from email_generator.database.supabase_client import db
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

logger = logging.getLogger(__name__)

@dataclass
class ScrapeResult:
    domain: str
    text: str
    error: Optional[str] = None
    skipped: bool = False
    response_time: float = 0.0
    final_url: Optional[str] = None

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

@asynccontextmanager
async def get_browser_page():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor',
            '--disable-extensions',
            '--disable-plugins',
            '--disable-images',
            '--disable-javascript', 
            '--no-sandbox',
            '--disable-dev-shm-usage'
        ]
    )
        try:
            context = await browser.new_context(
                user_agent=random_user_agent(),
                viewport={"width": random.randint(1280, 1600), "height": random.randint(720, 1000)},
                locale="en-US",
                timezone_id="America/New_York"
            )
            try:
                page = await context.new_page()
                try:
                    yield page
                finally:
                    await page.close()
            finally:
                await context.close()
        finally:
            await browser.close()

def scraped_domains(domain: str) -> bool:
    return db.is_domain_scraped(domain)

def store_scrape_results(result: ScrapeResult):
    domain = result.domain
    text = result.text
    error = result.error

    try:
        success = db.store_scrape_results(domain, text, error)
        if success:
            logger.info(f"Successfully stored scrape results for {domain}")
        else:
            logger.warning(f"Failed to store scrape results for {domain}")
    except Exception as e:
        logger.error(f"Error storing scrape results for {domain}: {e}") 

def validate_redirect_target(redirect_url: str, current_url: str) -> bool:
    try:
        if redirect_url.startswith(('/', '?', '#')):
            absolute_url = urljoin(current_url, redirect_url)
        elif redirect_url.startswith(('http://', 'https://')):
            absolute_url = redirect_url
        else:
            return False

        parsed = urlparse(absolute_url)

        if not parsed.hostname:
            return False
        
        normalized_domain = normalize_domain(parsed.hostname)

        if not is_valid_domain(normalized_domain):
            return False
        
        return check_domain_safety(normalized_domain)
    
    except Exception:
        return False

async def try_scrape_protocol(url: str, normalized: str, protocol: str) -> ScrapeResult:
    try:
        async with get_browser_page() as page:
            redirect_count = 0
            redirect_exceeded = False
            current_url = url

            def handle_response(response):
                nonlocal redirect_count, redirect_exceeded, current_url

                status = response.status
                location = response.headers.get('location')

                if status == 0:
                    redirect_exceeded = True
                    return
                
                if 300 <= status < 400 and location:
                    redirect_count += 1

                    if redirect_count > MAX_REDIRECTS:
                        redirect_exceeded = True
                        return
                    
                    if not validate_redirect_target(location, current_url):
                        redirect_exceeded = True
                        return
                    
                    if location.startswith(('http://', 'https://')):
                        new_url = location
                    else:
                        new_url = urljoin(current_url, location)

                    parsed_redirect = urlparse(new_url)
                    if parsed_redirect.hostname:
                        redirect_domain = normalize_domain(parsed_redirect.hostname)
                        if not check_domain_safety(redirect_domain):
                            redirect_exceeded = True
                            return

                    current_url = new_url
                            
            page.on("response", handle_response)

            try:
                start_time = asyncio.get_event_loop().time()
                await page.goto(url, timeout=10000)
                response_time = asyncio.get_event_loop().time() - start_time

                if redirect_exceeded:
                    return ScrapeResult(
                        domain=normalized,
                        text="",
                        error=f"{protocol.upper()} timeout after 10000ms",
                        response_time=response_time,
                        final_url=current_url
                    )
                        
                await page.wait_for_timeout(random.randint(1000, 2500))
                await page.mouse.wheel(0, 3000)
                html = await page.content()

                await asyncio.sleep(get_adaptive_delay(had_error=False, response_time=response_time))

                max_html_size = 1_000_000 # 1MB
                if len(html) > max_html_size:
                    return ScrapeResult(
                        domain=normalized,
                        text="",
                        error=f"{protocol.upper()} HTML too large ({len(html)} bytes)",
                        response_time=response_time,
                        final_url=current_url
                    )

            except PlaywrightTimeout:
                await asyncio.sleep(get_adaptive_delay(had_error=True))
                return ScrapeResult(
                    domain=normalized,
                    text="",
                    error=f"{protocol.upper()} timeout after 10000ms",
                    response_time=response_time,
                    final_url=current_url
                )
            except Exception as e:
                await asyncio.sleep(get_adaptive_delay(had_error=True))
                if redirect_exceeded:
                    return ScrapeResult(
                        domain=normalized,
                        text="",
                        error=f"{protocol.upper()} too many redirects (>{MAX_REDIRECTS})",
                        response_time=response_time,
                        final_url=current_url
                    )
                else:
                    return ScrapeResult(
                        domain=normalized,
                        text="",
                        error=f"{protocol.upper()} page load error: {str(e)}",
                        response_time=response_time,
                        final_url=current_url
                    )

            if len(html) < 300 or "captcha" in html.lower() or "cloudflare" in html.lower():
                return ScrapeResult(
                    domain=normalized,
                    text="",
                    error=f"{protocol.upper()} suspicious or protected content",
                    response_time=response_time,
                    final_url=current_url
                )

            try:
                soup = BeautifulSoup(html, "html.parser")
                extracted_text = extract_text(soup)
            except Exception as e:
                extracted_text = ""
                return ScrapeResult(
                    domain=normalized,
                    text=extracted_text,
                    error=f"{protocol.upper()} text extraction failed: {str(e)}",
                    response_time=response_time,
                    final_url=current_url
                )
            
            return ScrapeResult(
                domain=normalized,
                text=extracted_text,
                error=None,
                response_time=response_time,
                final_url=current_url
            )

    except Exception as e:
        return ScrapeResult(
            domain=normalized,
            text="",
            error=f"{protocol.upper()} browser error: {str(e)}",
            response_time=0.0,
            final_url=url
        )

async def scrape_and_extract(domain: str) -> ScrapeResult:
    normalized = normalize_domain(domain)

    apply_rate_limit(normalized)

    if not is_valid_domain(normalized):
        result = ScrapeResult(
            domain=normalized,
            text="",
            error="Invalid domain format"
        )
        store_scrape_results(result)
        return result

    if scraped_domains(normalized):
        return ScrapeResult(
        domain=normalized,
        text="",
        error="Already scraped",
        skipped=True
    )
    
    if not check_domain_safety(normalized):
        result = ScrapeResult(
            domain=normalized,
            text="",
            error="Blocked: Domain resolved to dangerous internal or metadata IP"
        )
        store_scrape_results(result)
        return result 

    if not is_scraping_allowed(normalized):
        result = ScrapeResult(
            domain=normalized,
            text="",
            error="Blocked: Disallowed by robots.txt"
        )
        store_scrape_results(result)
        return result

    failed_attempts = []

    for protocol in ["https", "http"]:
        url = f"{protocol}://{normalized}"
        result = await try_scrape_protocol(url, normalized, protocol)

        if result.error is None:
            store_scrape_results(result)
            return result
        else:
            failed_attempts.append(f"{protocol.upper()}: {result.error}")
    
    result = ScrapeResult(
        domain=normalized,
        text="",
        error=f"Both protocols failed - {': '.join(failed_attempts)}"
    )
    store_scrape_results(result)
    return result

def get_scraping_stats():
    """Get scraping statistics from database"""
    try:
        return db.get_classification_stats()
    except Exception as e:
        logger.error(f"Error getting scraping stats: {e}")
        return None       