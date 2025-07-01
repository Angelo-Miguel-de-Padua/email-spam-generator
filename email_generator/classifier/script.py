import os
import json
from email_generator.classifier.scraper import scraper
from email_generator.classifier.load_tranco import load_tranco_domains

output_file = "resources/classified_domains.json"
csv_source = "resources/top-1m.csv"
LIMIT = 5000

def load_previous_results() -> dict:
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                return {entry["domain"]: entry for entry in data}
            except json.JSONDecodeError:
                return {}
    
    return {}