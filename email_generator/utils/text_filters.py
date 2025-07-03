import re

def useless_text(text: str) -> bool:
    if not text or len(text.strip()) < 30:
        return True
    
    lowered = text.lower()

    noisy_signals = [
        "error 404", "not found", "403 forbidden", "cloudflare", "captcha",
        "this site can’t be reached", "access denied", "nginx", "server error",
        "that’s all we know", "please enable javascript", "502 bad gateway"
    ]

    num_tokens = len(lowered.split())
    matched_signals = sum(1 for signal in noisy_signals if signal in lowered)

    return matched_signals >= 2 or num_tokens < 10