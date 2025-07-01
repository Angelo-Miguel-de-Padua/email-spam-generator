import os
import json
from email_generator.classifier.scraper import scraper
from email_generator.classifier.load_tranco import load_tranco_domains

output_file = "resources/classified_domains.json"
csv_source = "resources/top-1m.csv"
LIMIT = 5000

