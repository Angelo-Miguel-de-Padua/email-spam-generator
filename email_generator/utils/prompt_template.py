CATEGORY_LIST = [
    "jobs", "education", "travel", "finance", "ecommerce", "tech", "news",
    "media", "social", "forum", "health", "real_estate", "gaming", "sports",
    "adult", "cloud", "ai", "crypto", "security", "government", "general"
]

def build_prompt(text: str, domain: str) -> str:
    categories = ", ".join(CATEGORY_LIST)
    return f"""
You are an expert in domain classification.

Classify the following website based on its domain and content.

### DOMAIN:
{domain}

### WEBSITE TEXT:
{text}

### CATEGORIES:
Choose only one main category from the list:
{categories}

Also include the most appropriate subcategory (e.g., "apis", "banking", "streaming", "forums", "email", "hosting", etc.)

### RESPONSE FORMAT:
category: <main_category>
subcategory: <subcategory>
confidence: <1-10>
explanation: <brief explanation why this category and subcategory were chosen>
"""
