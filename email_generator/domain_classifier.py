from email_generator.utils.category_keywords import CATEGORY_KEYWORDS

min_keyword_matches = 2

def classify_text(text: str) -> tuple[str, dict]:
    text = text.lower
    scores = {}

    for category, keywords in CATEGORY_KEYWORDS.items():
        match_count = sum(1 for word in keywords if word in text)
        if match_count >= min_keyword_matches:
            scores[category] = match_count
        
        if not scores:
            return "general", {"scores": {}, "is_tied": False, "confidence": "low"}