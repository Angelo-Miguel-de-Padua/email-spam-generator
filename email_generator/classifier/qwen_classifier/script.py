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

SCRAPE_LIMIT = 500

def scrape_and_extract(domain: str):
    scraper.scrape_domain(domain)

async def main():
    domains = load_tranco_domains("resources/top-1m.csv", limit=SCRAPE_LIMIT)

    for i, domain in enumerate(domains, 1):
        try:
            print(f"[{i}/{SCRAPE_LIMIT}] Processing: {domain}")
            
            scrape_and_extract(domain)  
            await label_domain(domain)
        except Exception as e:
            logging.error(f"Failed to process {domain}: {e}")

    scraper.close()  

if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
