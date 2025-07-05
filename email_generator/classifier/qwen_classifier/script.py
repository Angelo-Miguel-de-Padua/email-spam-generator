import asyncio
from email_generator.utils.load_tranco import load_tranco_domains
from email_generator.classifier.qwen_classifier.qwen_scraper import scrape_and_extract
from email_generator.classifier.qwen_classifier.qwen_labeler import label_domain

scrape_limit = 10

async def main():
    domains = load_tranco_domains("resources/top-1m.csv", limit=scrape_limit)

    for i, domain in enumerate(domains, 1):
        print(f"[{i}/{scrape_limit}] Processing: {domain}")
        
        scrape_and_extract(domain)
        
        await label_domain(domain)

if __name__ == "__main__":
    asyncio.run(main())