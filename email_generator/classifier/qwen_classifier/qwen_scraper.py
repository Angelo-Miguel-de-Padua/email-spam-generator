import random
import asyncio
import logging
from dataclasses import dataclass
from contextlib import asynccontextmanager
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from typing import Optional, Protocol
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

class StorageInterface(Protocol):
    def is_domain_scraped(self, domain: str) -> bool: ...
    def store_scrape_results(self, domain: str, text: str, error: Optional[str]) -> bool: ...

class ValidationInterface(Protocol):
    def is_valid_domain(self, domain: str) -> bool: ...
    def check_domain_safety(self, domain: str) -> bool: ...
    def is_scraping_allowed(self, domain: str) -> bool: ...

class RateLimitInterface(Protocol):
    def apply_rate_limit(self, domain: str) -> None: ...
    def get_adaptive_delay(self, had_error: bool, response_time: float = 0.0) -> float: ...

class BrowserPool:
    def __init__(self, pool_size: int = 3):
        self.pool_size = pool_size
        self._browsers = []
        self._available = asyncio.Queue()
        self._initialized = False
        self._playwright = None

    async def initialize(self):
        if self._initialized:
            return
        
        self._playwright = await async_playwright().start()

        try:
            for _ in range(self.pool_size):
                browser = await self._playwright.chromium.launch(
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
                self._browsers.append(browser)
                await self._available.put(browser)

            self._initialized = True
        except Exception:
            await self.close()
            raise

    @asynccontextmanager
    async def get_page(self):
        if not self._initialized:
            await self.initialize()
            
        browser = await self._available.get()
        context = await browser.new_context(
            user_agent=self._random_user_agent(),
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            timezone_id="America/New York"
        )

        try:
            page = await context.new_page()
            try:
                yield page
            finally:
                await page.close()
        finally:
            await context.close()
            await self._available.put(browser)

    def random_user_agent() -> str:
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"
        ]
        return random.choice(user_agents)
        
    async def close(self):
        for browser in self._browsers:
            try:
                await browser.close()
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")
            
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception as e:
                logger.warning(f"Error stopping playwright: {e}")
            
        self._browsers.clear()
        self._playwright = None
        self._initialized = False
    
class AdaptiveTimeoutManager:
    def __init__(self, base_timeout: float = 15.0, max_timeout: float = 30.0):
        self.base_timeout = base_timeout
        self.max_timeout = max_timeout
        self._domain_stats = {}

    def get_timeout(self, domain: str) -> float:
        stats = self._domain_stats.get(domain, {})
        avg_response_time = stats.get('avg_response_time', 0)

        if avg_response_time > 0:
            timeout = min(avg_response_time * 3, self.max_timeout)
            return max(timeout, self.base_timeout)
        
        return self.base_timeout
    
    def update_stats(self, domain: str, response_time: float):
        if domain not in self._domain_stats:
            self._domain_stats[domain] = {'avg_response_time': response_time, 'count': 1}
        else:
            stats = self._domain_stats[domain]
            stats['avg_response_time'] = (stats['avg_response_time'] * stats['count'])
            stats['count'] += 1

class WebScraper:
    def __init__(
        self,
        storage: StorageInterface,
        validator: ValidationInterface,
        rate_limiter: RateLimitInterface,
        browser_pool: Optional[BrowserPool] = None,
        max_retries: int = 2
    ):
        self.storage = storage
        self.validator = validator
        self.rate_limiter = rate_limiter
        self.browser_pool = browser_pool or BrowserPool()
        self.timeout_manager = AdaptiveTimeoutManager()
        self.max_retries = max_retries
        self.max_redirects = 5
        self.max_html_size = 1_000_000
    
    async def scrape_domain(self, domain: str) -> ScrapeResult:
        normalized = self._normalize_domain(domain)

        if not self.validator.is_valid_domain(normalized):
            return ScrapeResult(normalized, "", "Invalid domain format")
        
        if self.storage.is_domain_scraped(normalized):
            return ScrapeResult(normalized, "", "Already scraped", skipped=True)
        
        self.rate_limiter.apply_rate_limit(normalized)

        if not self.validator.check_domain_safety(normalized):
            result = ScrapeResult(normalized, "", "Blocked: Domain resolved to dangerous internal or metadata IP")
            self._store_result(result)
            return result
        
        if not self.validator.is_scraping_allowed(normalized):
            result = ScrapeResult(normalized, "", "Blocked: Disallowed by robots.txt")
            self._store_result(result)
            return result
        
        for attempt in range(self.max_retries + 1):
            try:
                result = await self._scrape_with_protocols(normalized)
                if result.error is None:
                    self._store_result(result)
                    return result
                
                if attempt == self.max_retries():
                    self._store_result(result)
                    return result
                
                await asyncio.sleep(2 ** attempt)
            
            except Exception as e:
                if attempt == self.max_retries:
                    result = ScrapeResult(normalized, "", f"Scraping failed after {self.max_retries + 1} attempts: {str(e)}")
                    self._store_result(result)
                    return result
                
                await asyncio.sleep(2 ** attempt)
        
        result = ScrapeResult(normalized, "", "Unexpected error: max retries exceeded")
        self._store_result(result)
        return result
    
    async def _scrape_with_protocols(self, domain: str) -> ScrapeResult:
        failed_attempts = []

        for protocol in ["https", "http"]:
            url = f"{protocol}://{domain}"
            result = await self._scrape_url(url, domain, protocol)

            if result.error is None:
                return result
            else:
                failed_attempts.append(f"{protocol.upper()}: {result.error}")

        return ScrapeResult(
            domain,
            "",
            f"Both protocols failed - {'; '.join(failed_attempts)}"
        )
    
    async def _scrape_url(self, url: str, domain: str, protocol: str) -> ScrapeResult:
        timeout = self.timeout_manager.get_timeout(domain)
        start_time = asyncio.get_event_loop().time()

        try:
            async with self.browser_pool.get_page() as page:
                redirect_count = 0
                current_url = url

                def handle_response(response):
                    nonlocal redirect_count, current_url

                    if 300 <= response.status < 400:
                        redirect_count += 1
                        if redirect_count > self.max_redirects:
                            raise Exception(f"Too many redirects (>{self.max_redirects})")
                        
                        location = response.headers.get('location')
                        if location:
                            current_url = urljoin(current_url, location)
                
                page.on("response", handle_response)

                await page.goto(url, timeout=timeout * 1000)

                html = await page.content()
                response_time = asyncio.get_event_loop().time() - start_time

                self.timeout_manager.update_stats(domain, response_time)

                delay = self.rate_limiter.get_adaptive_delay(False, response_time)
                await asyncio.sleep(delay)

                if len(html) > self.max_html_size:
                    return ScrapeResult(domain, "", f"{protocol.upper()} HTML too large ({len(html)} bytes)")
                
                if len(html) < 300:
                    return ScrapeResult(domain, "", f"{protocol.upper()} content too small")
                
                if any(keyword in html.lower() for keyword in {"captcha", "cloudflare", "bot detection"}):
                    return ScrapeResult(domain, "", f"{protocol.upper()} suspicious or protected content")
                
                try:
                    soup = BeautifulSoup(html, "html.parser")
                    extracted_text = extract_text(soup)

                    return ScrapeResult(
                        domain,
                        extracted_text,
                        None,
                        response_time=response_time,
                        final_url=current_url
                    )
                
                except Exception as e:
                    return ScrapeResult(domain, "", f"{protocol.upper()} text extraction failed: {str(e)}")

        except PlaywrightTimeout:
            delay = self.rate_limiter.get_adaptive_delay(True)
            await asyncio.sleep(delay)
            return ScrapeResult(domain, "", f"{protocol.upper()} timeout after {timeout}s")

        except Exception as e:
            delay = self.rate_limiter.get_adaptive_delay(True)
            await asyncio.sleep(delay)
            return ScrapeResult(domain, "", f"{protocol.upper()} error: {str(e)}")

    def _normalize_domain(self, domain: str) -> str:
        return normalize_domain(domain)

    def _store_result(self, result: ScrapeResult):
        try:
            success = self.storage.store_scrape_results(result.domain, result.text, result.error)
            if success:
                logger.info(f"Successfully stored scrape results for {result.domain}")
            else:
                logger.warning(f"Failed to store scrape results for {result.domain}")
        except Exception as e:
            logger.error(f"Error storing scrape results for {result.domain}: {e}")

    async def close(self):
        await self.browser_pool.close() 

