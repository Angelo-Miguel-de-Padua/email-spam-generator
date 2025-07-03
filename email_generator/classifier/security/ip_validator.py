import logging
import time
import re
import ipaddress
import json
import requests
from pathlib import Path
from typing import Optional

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
    
    def _extract_ips_from_text(self, text: str) -> set[str]:
        """
        Extracts valid cloud metadata-related IP addresses from a raw text blob.
        
        Args:
            text (str): Raw content from the fetched source (e.g., GitHub README or list)
        
        Returns:
            set[str]: A set of valid and relevant IP addresses.
        """

        ip_pattern = r'\b(?:[0-9]{1-3}\.){3}[0-9]{1-3}\b'
        potential_ips = re.findall(ip_pattern, text)

        valid_ips = set()
        for ip in potential_ips:
            try:
                ip_obj = ipaddress.ip_address(ip)

                if (
                    ip_obj.is_link_local or         # 169.254.x.x (AWS, Azure, GCP)
                    ip.startswith("100.100.") or    # Alibaba metadata
                    ip.startswith("169.254.")       # General cloud metadata
                ):
                    valid_ips.add(ip)
            
            except ValueError:
                continue

        return valid_ips
    
    def _fetch_from_source(self, source_name: str, url: str) -> Optional[set[str]]:
        """
        Fetches cloud metadata IP addresses from a remote source URL.

        Args:
            source_name (str): Identifier for the metadata source (used for cache naming).
            url (str): The URL to fetch the IP data from.

        Returns:
            Optional[set[str]]: A set of extracted IPs if successful, otherwise None.
        """
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # Extract IPs from the content
            ips = self._extract_ips_from_text(response.text)
            
            # Cache the results
            cache_path = self._get_cache_path(source_name)
            cache_data = {
                'timestamp': time.time(),
                'ips': list(ips),
                'source_url': url
            }
            
            with open(cache_path, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            logger.info(f"Fetched {len(ips)} IPs from {source_name}")
            return ips
            
        except Exception as e:
            logger.warning(f"Failed to fetch from {source_name}: {e}")
            return None
    
    def _load_from_cache(self, source_name: str) -> Optional[set[str]]:
        """
        Loads cached IP addresses from a specific source, if the cache is still valid.

        Args:
            source_name (str): Identifier for the metadata source.

        Returns:
            Optional[set[str]]: Set of IPs if the cache is valid and readable, otherwise None.
        """
        cache_path = self._get_cache_path(source_name)

        if not self._is_cache_valid(cache_path):
            return None
        
        try:
            with open(cache_path, 'r') as f:
                cache_data = json.load(f)

            return set(cache_data['ips'])
        
        except Exception as e:
            logger.warning(f"Failed to load cache for {source_name}: {e}")
            return None
    
    def get_cloud_metadata_ips(self, force_refresh: bool = False) -> set[str]:
        """
        Collects and returns a combined set of cloud metadata IPs from cache, fallback, or live sources.

        Args: 
            force_refresh (bool): if True, fetches fresh data from the sources instead of using cached results.
        
        Returns:
            set[str]: A set of all known cloud metadata IPs, including fallback and dynamically fetched ones.
        """
        all_ips = set(fallback_cloud_metadata_ips)

        for source_name, url in cloud_metadata_sources.items():
            if not force_refresh:
                cached_ips = self._load_from_cache(source_name)
                if cached_ips:
                    all_ips.update(cached_ips)
                    continue
            
            fetched_ips = self._fetch_from_source(source_name, url)
            if fetched_ips:
                all_ips.update(fetched_ips)
        
        combined_cache = {
            'timestamp': time.time(),
            'ips': list(all_ips),
            'sources': list(cloud_metadata_sources.keys())
        }

        with open(self.cache_file, 'w') as f:
            json.dump(combined_cache, f, indent=2)

        logger.info(f"Total cloud metadata IPs: {len(all_ips)}")
        return all_ips
    
_metadata_updater = CloudMetadataUpdater()

def get_dangerous_cloud_ips() -> set[str]:
    """Get current set of dangerous cloud metadata IPs"""
    return _metadata_updater.get_cloud_metadata_ips()

def is_dangerous_ip(ip_str: str) -> bool:
    """Determines if a given IP address is potentially dangerous"""
    try:
        ip = ipaddress.ip_address(ip_str)

        cloud_ips = get_dangerous_cloud_ips()
        if ip_str in cloud_ips or str(ip) in cloud_ips:
            return True
        
        if isinstance(ip, ipaddress.IPv4Address):
            return {
                ip.is_private or
                ip.is_loopback or
                ip.is_link_local or
                ip.is_multicast or
                ip.is_reserved or
                ip.is_unspecified
            }
        elif isinstance(ip, ipaddress.IPv6Address):
            return {
                ip.is_private or
                ip.is_loopback or
                ip.is_link_local or
                ip.is_multicast or
                ip.is_reserved or
                ip.is_unspecified or
                ip.is_site_local
            }

        return False
    except ValueError:
        return True

def refresh_cloud_metadata_ips():
    """Manually refresh cloud metadata IPs"""
    return _metadata_updater.get_cloud_metadata_ips(force_refresh=True)
