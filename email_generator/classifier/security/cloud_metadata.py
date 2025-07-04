import ipaddress
import socket
import logging
from typing import Set
from .cloud_metadata import CloudMetadataUpdater

logger = logging.getLogger(__name__)

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
            return (
                ip.is_private or
                ip.is_loopback or
                ip.is_link_local or
                ip.is_multicast or
                ip.is_reserved or
                ip.is_unspecified
            )
        elif isinstance(ip, ipaddress.IPv6Address):
            return (
                ip.is_private or
                ip.is_loopback or
                ip.is_link_local or
                ip.is_multicast or
                ip.is_reserved or
                ip.is_unspecified or
                ip.is_site_local
            )

        return False
    except ValueError:
        return True

def refresh_cloud_metadata_ips():
    """Manually refresh cloud metadata IPs"""
    return _metadata_updater.get_cloud_metadata_ips(force_refresh=True)

def check_domain_safety(domain: str) -> bool:
    """
    Validates a domain by resolving its IPs and ensuring none match dangerous or reserved address ranges.
    """
    try:
        all_ips = []

        try:
            ipv4_info = socket.getaddrinfo(domain, None, socket.AF_INET)
            for info in ipv4_info:
                all_ips.append(info[4][0])
        except socket.gaierror:
            pass

        try:
            ipv6_info = socket.getaddrinfo(domain, None, socket.AF_INET6)
            for info in ipv6_info:
                ip = info[4][0]
                if '%' in ip:
                    ip = ip.split('%')[0]
                all_ips.append(ip)
        except socket.gaierror:
            pass

        if not all_ips:
            return False
        
        for ip in all_ips:
            if is_dangerous_ip(ip):
                return False
            
        return True
    
    except Exception:
        return False
    
def scheduled_cloud_metadata_update() -> bool:
    try:
        new_ips = refresh_cloud_metadata_ips()
        logger.info(f"Scheduled update completed: {len(new_ips)} cloud metadata IPs")
        return True
    except Exception as e:
        logger.error(f"Scheduled update failed: {e}")
        return False