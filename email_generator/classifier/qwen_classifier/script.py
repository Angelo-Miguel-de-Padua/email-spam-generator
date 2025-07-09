import logging
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

SCRAPE_LIMIT = 10000
THREADS = 3

def main():
    validator = DefaultValidator()
    rate_limiter = DefaultRateLimiter()

    scraper = WebScraper(
        storage=db, 
        validator=validator, 
        rate_limiter=rate_limiter
        )

    domains = load_tranco_domains("resources/top-1m.csv", limit=SCRAPE_LIMIT)

    def scrape_task(domain):
        try:
            result = scraper.scrape_domain(domain)
            return domain, result.error
        except Exception as e:
            return domain, str(e)
    
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(scrape_task, domain) for domain in domains]

        for i, future in enumerate(as_completed(futures), 1):
            domain, error = future.result()
            if error:
                logging.warning(f"[{i}/{SCRAPE_LIMIT}] Failed: {domain} - {error}")
            else:
                logging.info(f"[{i}/{SCRAPE_LIMIT}] Success: {domain}")
    
    scraper.close()

if __name__ == "__main__":
    main()

