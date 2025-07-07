import logging
import time
import json
from pathlib import Path

fallback_cloud_metadata_ips = {
    '169.254.169.254',
    '169.254.170.2',
    '100.100.100.200',
    '169.254.169.249',
    '169.254.169.250',
    '169.254.0.1',
}

logger = logging.getLogger(__name__)

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

    def get_cloud_metadata_ips(self, force_refresh: bool = False) -> set[str]:
        """
        Returns the fallback set of cloud metadata IPs.

        Args: 
            force_refresh (bool): Ignored since there are no dynamic sources now.

        Returns:
            set[str]: A set of known cloud metadata IPs (fallback only).
        """
        combined_cache = {
            'timestamp': time.time(),
            'ips': list(fallback_cloud_metadata_ips),
            'sources': ["fallback_only"]
        }

        try:
            with open(self.cache_file, 'w') as f:
                json.dump(combined_cache, f, indent=2)
            logger.info(f"Using fallback cloud metadata IPs: {len(fallback_cloud_metadata_ips)}")
        except Exception as e:
            logger.warning(f"Failed to write cache file: {e}")

        return fallback_cloud_metadata_ips
