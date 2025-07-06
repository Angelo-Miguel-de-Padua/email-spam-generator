import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Any, Dict, List
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
    
    def get_domain_data(self, domain: str) -> Optional[Dict[str, Any]]:
        result = self._safe_execute(
                self.client.table("domain_labels").select("*").eq("domain", domain),
                f"Error getting domain data: {domain}"
            )
        return result[0] if result else None
    
    def store_scrape_results(self, domain: str, text: str, error: Optional[str] = None) -> bool:
        data = {
            "domain": domain,
            "scraped_text": text,
            "last_scraped": self._get_current_timestamp()
        }

        if error:
            data["scrape_error"] = error
            logger.warning(f"Storing scrape results for {domain} with error: {error}")
        
        result = self._safe_execute(
            self.client.table("domain_labels").upsert(data),
            f"Error storing scrape results for {domain}",
            return_data=False
        )

        if result:
            logger.info(f"Stored scrape results for {domain}")

        return bool(result)

    def store_classification_results(
            self, 
            domain: str, 
            category: str, 
            subcategory: str = None, 
            confidence: int = 0, 
            explanation: str = None,
            source: str = None, 
            text: str = None, 
            error: str = None
        ) -> bool:
        
        flagged = bool(error)

        data = {
            "domain": domain,
            "category": category,
            "confidence": confidence,
            "last_classified": self._get_current_timestamp(),
            "flagged_for_review": flagged
        }

        data.update({k: v for k, v in {
            "subcategory": subcategory,
            "explanation": explanation,
            "source": source,
            "text": text,
            "error": error
        }.items() if v is not None})

        if error:
            logger.warning(f"Storing classification for {domain} with error: {error}")
        
        result = self._safe_execute(
            self.client.table("domain_labels").upsert(data),
            f"Error storing classification for {domain}",
            return_data=False
        )

        if result:
            logger.info(f"Stored classification for {domain}: {category}")
        
        return bool(result)
    
    def get_unclassified_domains(self, limit: int = 100) -> List[Dict[str, Any]]:
        result = self._safe_execute(
            self.client.table("domain_labels")
            .select("*")
            .not_.is_("scraped_text", None)
            .is_("category", None)
            .order("last_scraped", desc=True)
            .limit(limit),
            "Error getting unclassified domains"
        )
        return result or []