import os
import json
import time

ROBOTS_CACHE_FILE = "resources/robots_cache.json"
CACHE_TTL_SECONDS = 86400 # 1day
CACHE_WRITE_THRESHOLD = 5

_robots_cache = {}
_cache_loaded = False
_cache_dirty = False
_cache_write_count = 0

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
    
def _save_robots_cache(force: bool = False):
    global _cache_dirty, _cache_write_count
    if not _cache_dirty:
        return
    if not force and _cache_write_count < CACHE_WRITE_THRESHOLD:
        return
    _cleanup_expired_entries()
    os.makedirs(os.path.dirname(ROBOTS_CACHE_FILE), exist_ok=True)
    with open(ROBOTS_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(_robots_cache, f, indent=2)
    _cache_dirty = False
    _cache_write_count = 0
