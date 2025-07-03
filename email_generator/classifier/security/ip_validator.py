import logging

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