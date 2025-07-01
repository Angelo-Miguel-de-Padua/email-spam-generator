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

        sorted_cats = sorted(scores.items(), key=lambda x: -x[1])

        top_score = sorted_cats[0][1]
        top_categories = {cat for cat, score in sorted_cats if score == top_score}

        is_tied = len(top_categories) > 1
        confidence = "high" if len(sorted_cats) == 1 or (top_score - sorted_cats[1][1] >= 2) else "low"

        return sorted_cats[0][0], {
            "scores": scores,
            "is_tied": is_tied,
            "confidence": confidence
        }