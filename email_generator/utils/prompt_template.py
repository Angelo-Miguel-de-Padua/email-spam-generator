CATEGORY_LIST = [
    "jobs", "education", "travel", "finance", "ecommerce", "tech", "news",
    "media", "social", "forum", "health", "real_estate", "gaming", "sports",
    "adult", "cloud", "ai", "crypto", "security", "government", "general"
]

def build_prompt(text: str) -> str:
    categories = ", ".join(CATEGORY_LIST)
    return f"""
You are an expert in web content classifier.

Your task is to read the following website content and categorize it into one of the following categories:
{categories}

### WEBSITE TEXT:
{text}

### INSTRUCTION:
Respond with only the most appropriate category name from the list above.
Do not explain.
Do not add extra punctuation or words.
Just reply with the category.
"""