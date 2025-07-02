import os
import json
import requests
from dotenv import load_dotenv
from email_generator.classifier.mixtral_classifier.mixtral_scraper import scrape_and_extract
from email_generator.utils.prompt_template import build_prompt
from email_generator.utils.domain_utils import normalize_domain

load_dotenv()

OLLAMA_MODEL_NAME = "mixtral:8x7b-instruct-v0.1-q4_K_M"
scraped_file = "../resources/scraped_data.json"
labeled_file = "../resources/labeled_data.json"
