import asyncio
import logging
from email_generator.classifier.qwen_classifier.qwen_scraper import WebScraper
from email_generator.database.supabase_client import db
from email_generator.classifier.qwen_classifier.interfaces import DefaultValidator, DefaultRateLimiter
from email_generator.utils.load_tranco import load_tranco_domains
from email_generator.classifier.qwen_classifier.qwen_scraper import scrape_and_extract
from email_generator.classifier.qwen_classifier.qwen_labeler import label_domain

validator = DefaultValidator()
rate_limiter = DefaultRateLimiter()

scraper = WebScraper(
    storage=db,
    validator=validator,
    rate_limiter=rate_limiter
)

scrape_limit = 10

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('domain_classification.log'),
        logging.StreamHandler()
    ]
)

async def main():
    domains = load_tranco_domains("resources/top-1m.csv", limit=scrape_limit)

    for i, domain in enumerate(domains, 1):
        print(f"[{i}/{scrape_limit}] Processing: {domain}")
        
        scrape_and_extract(domain)
        
        await label_domain(domain)

if __name__ == "__main__":
    asyncio.run(main())