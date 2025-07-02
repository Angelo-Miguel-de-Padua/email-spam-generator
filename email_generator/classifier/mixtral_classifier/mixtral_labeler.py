import os
import json
import requests
from dotenv import load_dotenv
from email_generator.classifier.mixtral_classifier.mixtral_scraper import scrape_and_extract
from email_generator.utils.prompt_template import build_prompt
from email_generator.utils.domain_utils import normalize_domain

load_dotenv()

OLLAMA_MODEL_NAME = "mixtral:8x7b-instruct-v0.1-q4_K_M"
OLLAMA_ENDPOINT = os.getenv("OLLAMA_ENDPOINT")
scraped_file = "../resources/scraped_data.json"
labeled_file = "../resources/labeled_data.json"

def call_mixtral(prompt: str, retries: int = 2) -> str:
    for attempt in range(retries + 1):
        try:
            response = requests.post(
                OLLAMA_ENDPOINT,
                json={
                    "model": OLLAMA_MODEL_NAME,
                    "prompt": prompt,
                    "stream": False
                }
            )
            if response.status_code == 200:
                return response.json()["response"].strip().lower()
            else:
                raise Exception(f"Status {response.status_code}: {response.text}")
        except Exception as e:
            if attempt == retries:
                raise Exception(f"Mixtral failed after {retries + 1} tries: {e}")