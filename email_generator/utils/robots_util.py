import os
import json

ROBOTS_CACHE_FILE = "resources/robots_cache.json"

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