import os
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Any, Dict, List, Set
from supabase import create_client, Client
from email_generator.utils.load_tranco import load_tranco_domains

load_dotenv()

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_ANON_KEY")

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment variables")
        
        self.client: Client = create_client(self.supabase_url, self.supabase_key)
    
    def __repr__(self) -> str:
        return f"<SupabaseClient connected={bool(self.client)} url={self.supabase_url[:50]}...>"
    
    def _safe_execute(self, query, error_msg: str, return_data: bool = True, retries: int = 3, delay: float = 1.0):
        for attempt in range(1, retries + 1):
            try:
                result = query.execute()
                return result.data if return_data else result
            except Exception as e:
                logger.warning(f"{error_msg} (attempt {attempt}/{retries}): {e}")
                if attempt > retries:
                    time.sleep(delay)
                else:
                    logger.error(f"{error_msg} - Failed after {retries} attempts")
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
    
    def delete_domain(self, domain: str) -> bool:
        result = self._safe_execute(
            self.client.table("domain_labels").delete().eq("domain", domain),
            f"Error deleting domain: {domain}",
            return_data=False
        )

        if result:
            logger.info(f"Deleted domain: {domain}")
        else:
            logger.warning(f"Failed to delete domain: {domain}")
        
        return bool(result)
    
    def get_classification_stats(self) -> Dict[str, int]:
        try:
            total_result = self.client.table("domain_labels").select("domain", count="exact").execute()
            total_domains = total_result.count or 0

            classified_result = self.client.table("domain_labels").select("domain", count="exact").not_.is_("category", None).execute()
            classified_domains = classified_result.count or 0

            scraped_result = self.client.table("domain_labels").select("domain", count="exact").not_.is_("scraped_text", None).execute()
            scraped_domains = scraped_result.count or 0

            logger.debug(f"Classification stats: {total_domains} total, {scraped_domains} scraped, {classified_domains} classified")

            return {
                "total_domains": total_domains,
                "scraped_domains": scraped_domains,
                "classified_domains": classified_domains,
                "pending_classification": max(0, scraped_domains - classified_domains)
            }
        except Exception as e:
            logger.error(f"Error getting classification stats: {e}")
            return {
                "total_domains": 0,
                "scraped_domains": 0,
                "classified_domains": 0,
                "pending_classification": 0
            }
    
    def preload_domains(
        self,
        domains: List[str],
        batch_size: int = 1000,
        created_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        
        if not domains: 
            logger.warning("No domains provided for preloading")
            return {
                "success": False,
                "inserted": 0,
                "skipped": 0,
                "total": 0,
                "error": "No domains provided"
            }
        
        logger.info(f"Starting preload of {len(domains)} domains")

        existing_domains: Set[str] = set()
        try:
            existing_result = (
                self.client
                    .table("domain_labels")
                    .select("domain")
                    .in_("domain", domains)
                    .execute()
            )
            existing_domains = {row["domain"] for row in existing_result.data}
            logger.info(f"Found {len(existing_domains)} existing domains out of {len(domains)}")
        except Exception as e:
            logger.warning(f"Could not check existing domains: {e}")

        new_domains = [domain for domain in domains if domain not in existing_domains]

        if not new_domains:
            logger.info("All domains already exist in database")
            return {
                "success": True,
                "inserted": 0,
                "skipped": len(existing_domains),
                "total": len(domains),
                "message": "All domains already exist"
            }
        
        logger.info(f"Inserting {len(new_domains)} new domains")

        timestamp = created_at or self._get_current_timestamp()
        total_inserted = 0

        for i in range(0, len(new_domains), batch_size):
            batch = new_domains[i:i + batch_size]

            batch_data = [
                {
                    "domain": domain,
                    "created_at": timestamp
                }
                for domain in batch
            ]

            result = self._safe_execute(
                self.client.table("domain_labels").insert(batch_data),
                f"Error inserting batch {i//batch_size + 1}",
                return_data=False
            )

            if result:
                total_inserted += len(batch)
                logger.info(f"Inserted batch {i//batch_size + 1}/{(len(new_domains) + batch_size - 1)//batch_size}: {len(batch)} domains")
            else:
                logger.error(f"Failed to insert batch {i//batch_size + 1}")
                return {
                    "success": False,
                    "inserted": total_inserted,
                    "skipped": len(existing_domains),
                    "total": len(domains),
                    "error": f"Failed to insert batch {i//batch_size + 1}"
                }                
    
        logger.info(f"Successfully preloaded {total_inserted} domains")
        return {
            "success": True,
            "inserted": total_inserted,
            "skipped": len(existing_domains),
            "total": len(domains),
            "message": f"Successfully inserted {total_inserted} domains"
        }
    
    def preload_tranco_domains(
        self,
        csv_path: str,
        limit: int = 500,
        batch_size: int = 1000,
        created_at: Optional[str] = None
    ) -> Dict[str, Any]:
        
        domains = load_tranco_domains(csv_path, limit)

        if not domains:
            return {
                "success": False,
                "inserted": 0,
                "skipped": 0,
                "total": 0,
                "error": f"Failed to load domains from {csv_path}"
            }
        
        return self.preload_domains(domains, batch_size, created_at)
        
db = SupabaseClient()