import time
import random
from collections import defaultdict

_domain_last_request = defaultdict(float)

MIN_DOMAIN_DELAY = 5.0
JITTER_RANGE = (1.5, 3.5)

def apply_rate_limit(domain: str):
    now = time.time()
    elapsed = now - _domain_last_request(domain)

    if elapsed < MIN_DOMAIN_DELAY:
        time.sleep(MIN_DOMAIN_DELAY - elapsed)

    time.sleep(random.uniform(*JITTER_RANGE))

    _domain_last_request[domain] = time.time()
