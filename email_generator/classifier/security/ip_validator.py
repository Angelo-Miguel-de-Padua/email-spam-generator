import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

cloud_metadata_sources = {
    "payloadsallthethings": "https://raw.githubusercontent.com/swisskyrepo/PayloadsAllTheThings/master/Server%20Side%20Request%20Forgery/README.md",
    "securitytrails": "https://raw.githubusercontent.com/SecurityTrails/ssrf-targets/main/cloud-metadata.txt",
    "assetnote_surf": "https://raw.githubusercontent.com/assetnote/surf/main/lists/cloud-metadata.txt",
    "hacktricks": "https://raw.githubusercontent.com/carlospolop/hacktricks/master/pentesting-web/ssrf-server-side-request-forgery/README.md",
    "bugbounty_cheatsheet": "https://raw.githubusercontent.com/EdOverflow/bugbounty-cheatsheet/master/cheatsheets/ssrf.md",
}

fallback_cloud_metadata_ips = {
    '169.254.169.254',
    '169.254.170.2',
    '100.100.100.200',
    '169.254.169.249',
    '169.254.169.250',
    '169.254.0.1',
}

class CloudMetadataUpdater:
    def __init__(self, cache_dir: str = "cache", cache_ttl: int = 86400):
        """
        Initializes the cloud metadata updater.

        Args:
            cache_dir (str): Directory to store cached metadata IPs.
            cache_ttl (int): Time-to-live for cached data (in seconds). Default is 24 hours.
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_ttl = cache_ttl
        self.cache_file = self.cache_dir / "cloud_metadata_ips.json"

    def _get_cache_path(self, source_name: str) -> Path:
        """

        Return the cache file path for a specific metadata source.
        
        Args:
            source_name (str): Identifier for the metadata source

        Returns:
            Path: Path to the corresponding cache file.
        """
        return self.cache_dir / f"{source_name}_metadata.json"
    
    def _is_cache_valid(self, cache_path: Path) -> bool:
        """
        Determine if a cache file is still valid based on TTL.

        Args:
            cache_path (Path): Path to the cache file.
        
        Returns:
            bool: True if the cache file is still within TTL, else False.
        """
        if not cache_path.exists():
            return False
        
        cache_age = time.time() - cache_path.stat().st_mtime
        return cache_age < self.cache_ttl