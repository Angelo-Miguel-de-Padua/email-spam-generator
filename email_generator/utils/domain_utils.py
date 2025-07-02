import pandas as pd

def load_tranco_domains(csv_path, limit=500):
    df = pd.read_csv(csv_path, header=None, names=["rank", "domain"])
    return df["domain"].head(limit).tolist()

def normalize_domain(domain: str) -> str:
    domain = domain.strip().lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain