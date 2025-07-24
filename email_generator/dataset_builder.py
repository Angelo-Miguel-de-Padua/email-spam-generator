import pandas as pd
import random
from typing import Optional, Dict

def get_random_domain_by_category(
    csv_path: str,
    category: str
) -> Dict[str, str]:
    
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    df = df.dropna(subset=["domain", "category"])

    filtered = df[df["category"].str.lower() == category.lower()]

    if filtered.empty:
        raise ValueError(f"No domains found in category '{category}'")
    
    row = filtered.sample(1).iloc[0]

    result = {
        "domain": row["domain"],
        "category": row["category"],
    }

    if "subcategory" in df.columns and pd.notna(row["subcategory"]):
        subcategory = str(row["subcategory"]).strip()
        if subcategory:
            result["subcategory"] = subcategory
    
    return result