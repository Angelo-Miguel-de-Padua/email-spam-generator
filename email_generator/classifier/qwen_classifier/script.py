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

MAX_DOMAINS = 10000
THREADS = 5

def scrape_task(scraper, domain):
    result = scraper.scrape_domain(domain)
    return domain, result.error

def main():
    validator = DefaultValidator()
    rate_limiter = DefaultRateLimiter()
    scraper = WebScraper(
        storage=db, 
        validator=validator, 
        rate_limiter=rate_limiter
        )

    domains = load_tranco_domains("resources/top-1m.csv", limit=MAX_DOMAINS)

    scraped_domains = db.get_scraped_domains_from_list(domains)
    unscraped = [d for d in domains if d not in scraped_domains]

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(scrape_task, scraper, d) for d in unscraped]

        for i, future in enumerate(as_completed(futures), 1):
            domain, error = future.result()
            if error:
                logging.warning(f"[{i}/{len(unscraped)}] Failed: {domain} - {error}")
            else:
                logging.info(f"[{i}/{len(unscraped)}] Success: {domain}")
    
    scraper.close()

if __name__ == "__main__":
    main()

