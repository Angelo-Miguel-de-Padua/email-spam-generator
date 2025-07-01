import csv

def load_tranco_domains(path: str = "resources/top-1m.csv", limit: int = 5000) -> list[str]:
    domains = []

    with open(path, newline="", encoding="utf-8") as csvfile: