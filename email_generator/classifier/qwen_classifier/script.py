from email_generator.utils.load_tranco import load_tranco_domains
from email_generator.classifier.qwen_classifier.qwen_scraper import scrape_and_extract, scraped_domains
from email_generator.classifier.qwen_classifier.qwen_labeler import label_domain, is_domain_labeled

scrape_limit = 10

def main():
    domains = load_tranco_domains("resources/top-1m.csv", limit=scrape_limit)

    for i, domain in enumerate(domains, 1):
        print(f"[{i}/{scrape_limit}] Processing: {domain}")
        
        if not scraped_domains(domain):
            scrape_and_extract(domain)
        
        if is_domain_labeled(domain):
            print(f"Already labeled: {domain}")
            continue

        result = label_domain(domain)
        if result:
            print(f"Labeled: {result['domain']} -> {result['category']}")
        else:
            print(f"Skipped (already labeled or error): {domain}")

if __name__ == "__main__":
    main()
