CATEGORY_LIST = [
    "jobs", "education", "travel", "finance", "ecommerce", "tech", "news",
    "media", "social", "forum", "health", "real_estate", "gaming", "sports",
    "adult", "cloud", "ai", "crypto", "security", "government", "general"
]

def build_prompt(text: str, domain: str) -> str:
    categories = ", ".join(CATEGORY_LIST)
    return f"""
You are an expert in domain classification.

Your task is to classify a website based on the **provided scraped text**, and if the text is unclear or missing, fall back to clues from the domain name.

### PRIORITY RULES:
1. Use the scraped text as your **primary source**.
2. Only use the domain name if the scraped text is vague or missing — and only based on **visible words** in the domain. Do not guess or hallucinate.
3. Never assume meanings beyond the actual words provided.

### DOMAIN NAME USAGE:
- Break domain into visible parts (e.g., "adult", "machiko").
- Use parts only if they give **clear meaning** (e.g., “pornhub” → adult).
- Weak or ambiguous terms like “shop”, “hub”, or “top” alone are not enough.

### ONLY RESPOND IF CONFIDENT:
Only classify if confidence ≥ 8.  
If unsure, respond with:
  "category": "unknown",
  "subcategory": "unknown",
  "confidence": 0,
  "explanation": "Insufficient information in website text and domain to determine category."

### DOMAIN:
{domain}

### WEBSITE TEXT:
{text}

### ALLOWED CATEGORIES:
Choose one from:
{categories}

Also include a subcategory (e.g., "banking", "forums", "video streaming", etc.)

### RESPONSE FORMAT:
Respond with **ONLY a single valid JSON object**, like this:

{{
  "category": "<one of the allowed categories>",
  "subcategory": "<specific subcategory>",
  "confidence": <integer 0 to 10>,
  "explanation": "<short, single-line explanation>"
}}
"""

