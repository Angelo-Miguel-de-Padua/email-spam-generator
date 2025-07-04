import pandas as pd
import re
import hashlib
from urllib.parse import urlparse

def load_tranco_domains(csv_path, limit=500):
    df = pd.read_csv(csv_path, header=None, names=["rank", "domain"])
    return df["domain"].head(limit).tolist()

def normalize_domain(domain: str) -> str:
    domain = domain.strip().lower()

    if not domain.startswith(("http://", "https://")):
        domain = f"http://{domain}"
    
    parsed = urlparse(domain)
    hostname = parsed.hostname or parsed.netloc

    if hostname.startswith("www."):
        hostname = hostname[4:]

    return hostname

def is_valid_domain(domain: str) -> bool:
    if len(domain) > 253:
        return False
    
    pattern = r"^(?!-)[A-Za-z0-9-]{1,63}(?<!-)(\.(?!-)[A-Za-z0-9-]{1,63}(?<!-))*$"
    return re.fullmatch(pattern, domain) is not None

def sanitize_domain_filename(domain: str, extension: str = "json") -> str:
    domain = normalize_domain(domain)

    domain_clean = re.sub(r"[^\w.-]", "_", domain)

    domain_hash = hashlib.sha256(domain.encode()).hexdigest()[:8]

    return f"{domain_clean}_{domain_hash}.{extension}"