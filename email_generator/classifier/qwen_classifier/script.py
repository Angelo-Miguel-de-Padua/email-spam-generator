import logging
import threading
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from email_generator.classifier.qwen_classifier.qwen_scraper import WebScraper
from email_generator.database.supabase_client import db
from email_generator.classifier.qwen_classifier.interfaces import DefaultValidator, DefaultRateLimiter
from email_generator.utils.load_tranco import load_tranco_domains
from email_generator.classifier.qwen_classifier.qwen_labeler import label_domain

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('domain_classification.log'),
        logging.StreamHandler()
    ]
)

MAX_DOMAINS = 10000
THREADS = 5
stop_event = threading.Event()

active_scrapers = []

def scrape_task(domain):
    if stop_event.is_set():
        return domain, "Aborted"
    
    scraper = WebScraper(
        storage=db,
        validator=DefaultValidator(),
        rate_limiter=DefaultRateLimiter()
    )
    active_scrapers.append(scraper)

    try:
        result = scraper.scrape_domain(domain)
        return domain, result.error
    finally:
        scraper.close()
        active_scrapers.remove(scraper)

def handle_shutdown(signum, frame):
    logging.warning("Shutdown signal received. Stopping gracefully...")
    stop_event.set()

def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    domains = load_tranco_domains("resources/top-1m.csv", limit=MAX_DOMAINS)
    scraped_domains = db.get_scraped_domains_from_list(domains)
    unscraped = [d for d in domains if d not in scraped_domains]

    try:
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = [executor.submit(scrape_task, d) for d in unscraped]

            for i, future in enumerate(as_completed(futures), 1):
                if stop_event.is_set():
                    break

                domain, error = future.result()
                if error:
                    logging.warning(f"[{i}/{len(unscraped)}] Failed: {domain} - {error}")
                else:
                    logging.info(f"[{i}/{len(unscraped)}] Success: {domain}")
    finally:
        for scraper in list(active_scrapers):
            scraper.close()

if __name__ == "__main__":
    main()

