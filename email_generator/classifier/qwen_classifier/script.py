import asyncio
import logging
import sys
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

validator = DefaultValidator()
rate_limiter = DefaultRateLimiter()
scraper = WebScraper(
    storage=db,
    validator=validator,
    rate_limiter=rate_limiter
)

SCRAPE_LIMIT = 1

def scrape_and_extract(domain: str):
    scraper.scrape_domain(domain)

def main():
    validator = DefaultValidator()
    rate_limiter = DefaultRateLimiter()
    scraper = WebScraper(storage=db, validator=validator, rate_limiter=rate_limiter)

    domains = load_tranco_domains("resources/top-1m.csv", limit=500)

    for i, domain in enumerate(domains, 1):
        try:
            print(f"[{i}/500] Scraping: {domain}")
            scraper.scrape_domain(domain)
        except Exception as e:
            logging.error(f"Failed to scrape {domain}: {e}")

    scraper.close()

if __name__ == "__main__":
    main()

