import os
import pandas as pd
from typing import Optional, Dict, Callable, Awaitable
from email_generator.utils.qwen_utils import call_qwen

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

async def generate_random_email_by_category(
    csv_path: str,
    category: str,
    prompt_fn: Callable[[str, Optional[str]], str],
    model: Optional[str] = None,
    retries: int = 2,
    output_csv: Optional[str] = None
) -> Dict[str, str]:
    
    domain_info = get_random_domain_by_category(csv_path, category)
    subcategory = domain_info.get("subcategory")
    prompt = prompt_fn(domain_info["domain"], subcategory)

    try:
        generated_email = await call_qwen(prompt, model=model, retries=retries)
    except Exception as e:
        generated_email = f"Error generating email: {e}"
    
    result = {
        "domain": domain_info["domain"],
        "category": domain_info["category"],
        "subcategory": subcategory,
        "prompt": prompt,
        "generated_email": generated_email
    }

    if output_csv:
        try:
            df = pd.DataFrame([result])
            df.to_csv(output_csv, mode="a", index=False, header=not os.path.exists(output_csv))
        except Exception as e:
            print(f"Error saving to CSV: {e}")
    
    return result