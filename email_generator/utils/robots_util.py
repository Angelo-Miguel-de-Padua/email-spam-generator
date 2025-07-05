import os
import json
import time

ROBOTS_CACHE_FILE = "resources/robots_cache.json"
CACHE_TTL_SECONDS = 86400 # 1day

_robots_cache = {}
_cache_loaded = False

def _load_robots_cache():
    global _cache_loaded
    if os.path.exists(ROBOTS_CACHE_FILE):
        try:
            with open(ROBOTS_CACHE_FILE, "r", encoding="utf-8") as f:
                _robots_cache.update(json.load(f))
        except json.JSONDecodeError:
            pass
    _cache_loaded = True

def _cleanup_expired_entries():
    now = int(time.time())
    expired = [d for d, e in _robots_cache.items()
                if now - e.get("fetched_at", 0) > CACHE_TTL_SECONDS]
    for d in expired:
        del _robots_cache[d]