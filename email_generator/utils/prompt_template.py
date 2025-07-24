from typing import Optional

CATEGORY_LIST = [
    "jobs", "education", "travel", "finance", "ecommerce", "tech", "news",
    "media", "social", "forum", "health", "real_estate", "gaming", "sports",
    "adult", "cloud", "ai", "crypto", "security", "government", "general"
]

def label_domain_prompt(text: str, domain: str) -> str:
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

def fallback_label_domain_prompt(domain: str) -> str:
    categories = ", ".join(CATEGORY_LIST)
    return f"""
You are a domain classification expert.

Your task is to classify a domain based ONLY on its visible components.
You MUST NOT guess, hallucinate, or infer meanings from words that are NOT explicitly present in the domain string.

### VERY IMPORTANT RULES ###
- Only use letters, tokens, or words that are ACTUALLY PRESENT in the domain.
- Never imagine or hallucinate words that are not there. For example, "adult-machiko.com" does NOT contain "shop" — you cannot pretend it does.
- Break the domain into visible parts first (e.g., "adult", "machiko"), and ONLY use those parts for classification.
- Do NOT assume that common words like "shop", "hub", "pro", "zone", etc. always imply a specific category.
- Be extremely cautious with branded-looking terms or Latin/foreign-derived words (e.g., 'libra', 'memoria', 'lystit').
    - If a word has multiple possible meanings, DO NOT assume one interpretation without strong supporting context.
    - Words like 'libra' and 'memoria' might look tech-related but could just as easily refer to memorial or unrelated services.
- Do NOT rely on single-word cues unless their meaning is clearly unambiguous and well-known.
- Only classify if multiple domain components clearly and consistently point to the same category.
    - "techshop" → okay (tech + shop makes sense)
    - "brainshop" or "machikoshop" → unclear → mark as unknown
- If the domain’s purpose is ambiguous, possibly misleading, or only weakly inferred, mark it as unknown.
- If you do NOT recognize the domain or cannot be confident about its purpose, respond with:
  - category: unknown
  - subcategory: unknown
  - confidence: 0

Only assign a category if you are highly confident (confidence ≥ 8) and can clearly justify it using only the visible domain components.

### Allowed Categories (choose ONE):
{categories}

### Subcategory examples:
- tech → "search", "hardware", "software", "developer tools"
- ecommerce → "retail", "fashion", "electronics", "marketplace"
- health → "medicine", "fitness", "mental health"
- jobs → "job board", "freelancing", "company career page"
- media → "video", "streaming", "music", "news"

### Response format (JSON only):
{{
    "category": "<category>",
    "subcategory": "<subcategory>",
    "confidence": <1-10>,
    "explanation": "<brief and clear justification>"
}}

### Examples:
Input domain: "adult-machiko.com"  
→ Valid response:  
{{
    "category": "unknown",
    "subcategory": "unknown",
    "confidence": 0,
    "explanation": "The domain does not contain any recognizable keywords or components to confidently classify it."
}}

Input domain: "tech-hardwarehub.com"  
→ Valid response:  
{{
    "category": "tech",
    "subcategory": "hardware",
    "confidence": 9,
    "explanation": "The domain contains 'tech' and 'hardware', which strongly suggest it is a technology-related hardware site."
}}

Now classify the domain: {domain}
"""

def generate_jobs_email_prompt(domain: str, subcategory: Optional[str] = None) -> str:
    base = f"""
You are writing a realistic, human-like email from a jobs-related sender at {domain} (e.g., recruiter, hiring manager, job board representative, HR department).
The sender should have an email address like firstname@{domain} or similar, and the content should reflect {domain}'s focus on recruitment."""
    
    if subcategory:
        base += f"\n\n{domain} specializes in {subcategory} recruitment. Make sure the job roles and content reflect this specialization."
    
    base += """

Write a single, high-quality legitimate email related to job recruitment. Include both subject line and email body. Randomly choose a type of message, such as:
- a job offer  
- an interview invitation  
- a follow-up on an application  
- a job alert from a job board  
- a job search tips newsletter  
- a rejection email  
- an update about a new position opening

**Guidelines:**
- Vary the structure, length, and tone — formal or slightly conversational is fine.
- Use specific job roles, locations, or company names (you can make them up).
- Make the domain feel natural in the context (e.g., reference the company name in a way that matches the domain).
- Ensure the sender sounds human and trustworthy.
- DO NOT include any spammy language or suspicious elements.

Respond with **only** the full email (subject line + body). Do not include any metadata or explanation."""
    
    return base.strip()