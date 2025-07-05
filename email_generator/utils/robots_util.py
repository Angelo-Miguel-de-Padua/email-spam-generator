import os
import json
import time
import urllib.robotparser
from threading import Lock

ROBOTS_CACHE_FILE = "resources/robots_cache.json"
CACHE_TTL_SECONDS = 86400 # 1day
CACHE_WRITE_THRESHOLD = 5
FETCH_STUCK_TIMEOUT = 30 # seconds

_robots_cache = {}
_cache_loaded = False
_cache_dirty = False
_cache_write_count = 0

_cache_lock = Lock()
_fetching_domains: dict[str, float] = {}

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

def is_scraping_allowed(domain: str, user_agent: str = "*") -> bool:
    global _cache_dirty, _cache_write_count

    domain = domain.lower().strip()
    now = time.time()

    with _cache_lock:
        if not _cache_loaded:
            _load_robots_cache()
        
        cached = _robots_cache.get(domain)
        if cached and now - cached.get("fetched_at", 0) <= CACHE_TTL_SECONDS:
            return cached.get("allowed", True)
        
        fetch_started = _fetching_domains.get(domain)
        if fetch_started is not None:
            if now - fetch_started < FETCH_STUCK_TIMEOUT:
                return True
            else:
                _fetching_domains[domain] = now
        else:
            _fetching_domains[domain] = now
    
    try:
        robots_url = f"https://{domain}/robots.txt"
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        allowed = rp.can_fetch(user_agent, f"https://{domain}/")
    except Exception:
        allowed = True
    
    with _cache_lock:
        _robots_cache[domain] = {
            "allowed": allowed,
            "fetched_at": int(now)
        }
        _cache_dirty = True
        _cache_write_count += 1
        _fetching_domains.pop(domain, None)
        _save_robots_cache()
        return allowed