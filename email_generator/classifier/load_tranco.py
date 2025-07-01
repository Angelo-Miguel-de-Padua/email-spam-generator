import csv

def load_tranco_domains(path: str = "resources/top-1m.csv", limit: int = 5000) -> list[str]:
    