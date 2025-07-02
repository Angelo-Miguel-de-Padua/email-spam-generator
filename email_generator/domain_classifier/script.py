import os
import json
from email_generator.domain_classifier.scraper import scraper
from email_generator.domain_classifier.load_tranco import load_tranco_domains

output_file = "resources/classified_domains.json"
csv_source = "resources/top-1m.csv"
LIMIT = 10

def load_previous_results() -> dict:
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                return {entry["domain"]: entry for entry in data}
            except json.JSONDecodeError:
                return {}
    
    return {}

def save_result(result: dict, is_first: bool = False):
    with open(output_file, "a", encoding="utf-8") as f:
        if not is_first:
            f.write(",\n")
        f.write(json.dumps(result, ensure_ascii=False))

def run_scraper():
    domains = load_tranco_domains(csv_source, limit=LIMIT)
    previous = load_previous_results()
    processed = set(previous.keys())

    is_first_result = not os.path.exists(output_file)
    if is_first_result:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("[\n]")
    
    for i, domain in enumerate(domains, start=1):
        if domain in processed:
            print(f"[{i}] Skipping {domain} (already done)")
            continue

        result = scraper(domain)
        save_result(result, is_first=is_first_result)
        is_first_result = False

        print(f"[{i}] {domain} -> {result['category']} "
              f"(confidence: {result.get('confidence')}, error: {result.get('error')})")

    with open(output_file, "a", encoding="utf-8") as f:
        f.write("\n]")

if __name__ == "__main__":
    run_scraper()