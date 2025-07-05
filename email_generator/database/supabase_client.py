import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Any
from supabase import create_client, Client

load_dotenv()

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_ANON_KEY")

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment variables")
        
        self.client: Client = create_client(self.supabase_url, self.supabase_key)
    
    def _safe_execute(self, query, error_msg: str, return_data: bool = True):
        try:
            result = query.execute()
            return result.data if return_data else result
        except Exception as e:
            logger.error(f"{error_msg}: {e}")
            return None if return_data else False
        
    def _get_current_timestamp(self) -> str:
        return datetime.utcnow().isoformat()
    
    def _get_domain_field(self, domain: str, field: str) -> Optional[Any]:
        result = self._safe_execute(
            self.client.table("domain_labels").select(field).eq("domain", domain),
            f"Error getting {field} for domain {domain}"
        )
        return result[0][field] if result else None
    
    def domain_exists(self, domain: str) -> bool:
        result = self._safe_execute(
            self.client.table("domain_labels").select("domain").eq("domain", domain),
            f"Error checking domain existence: {domain}"
        )
        return bool(result)

    def is_domain_scraped(self, domain: str) -> bool:
        scraped_text = self._get_domain_field(domain, "scraped_text")
        has_scraped = scraped_text is not None

        if has_scraped:
            logger.debug(f"Domain {domain} already scraped - skipping")

        return has_scraped
    
    def is_domain_classified(self, domain: str) -> bool:
        category = self._get_domain_field(domain, "category")
        has_category = category is not None

        if has_category:
            logger.debug(f"Domain {domain} already classified - skipping")
        
        return has_category
