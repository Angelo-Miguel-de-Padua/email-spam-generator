import os
import logging
import time
from datetime import datetime, timezone
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
                if attempt < retries:
                    time.sleep(delay)
                else:
                    logger.error(f"{error_msg} - Failed after {retries} attempts")
                    return None if return_data else False
        
    def _get_current_timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat()
    
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
    
    def get_scraped_domains_from_list(self, domains: List[str], batch_size: int = 500) -> Set[str]:
        if not domains:
            return set()
        
        scraped_domains = set()
        
        for i in range(0, len(domains), batch_size):
            batch = domains[i:i + batch_size]

            result = self._safe_execute(
                self.client.table("domain_labels")
                .select("domain")
                .in_("domain", domains)
                .not_.is_("scraped_text", None),
                "Error getting scraped domains from list"
            )

            if result:
                batch_scraped = {row["domain"] for row in result}
                scraped_domains.update(batch_scraped)

            return {row["domain"] for row in result} if result else set()
    
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
            scraped_text: str = None, 
            scrape_error: str = None,
            classifier_error: str = None
        ) -> bool:
        
        flagged = bool(classifier_error or scrape_error)

        data = {
            "domain": domain,
            "category": category,
            "confidence": confidence,
            "last_classified": self._get_current_timestamp(),
            "flagged_for_review": flagged
        }

        # Optional fields to include if present
        data.update({k: v for k, v in {
            "subcategory": subcategory,
            "explanation": explanation,
            "source": source,
            "scraped_text": scraped_text,
            "scrape_error": scrape_error,
            "classifier_error": classifier_error
        }.items() if v is not None})

        if classifier_error or scrape_error:
            logger.warning(f"Storing classification for {domain} with error(s): "
                        f"{classifier_error or ''} {scrape_error or ''}".strip())
        
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
        check_batch_size: int = 1000
    ) -> Dict[str, Any]:
        
        if not domains: 
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
            for i in range(0, len(domains), check_batch_size):
                batch = domains[i:i + check_batch_size]
                existing_result = (
                    self.client
                        .table("domain_labels")
                        .select("domain")
                        .in_("domain", domains)
                        .execute()
                )
                if existing_result.data:
                    batch_existing = {row["domain"] for row in existing_result.data}
                    existing_domains.update(batch_existing)

                if i % (check_batch_size * 10) == 0:
                    logger.info(f"Checked {min(i + check_batch_size, len(domains))}/{len(domains)} for existence")

            logger.info(f"Found {len(existing_domains)} existing domains out of {len(domains)}")
        except Exception as e:
            logger.warning(f"Could not check existing domains: {e}")
            existing_domains = set()

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
            batch_data = [{"domain": domain, "created_at": timestamp} for domain in batch]

            try:
                result = (
                    self.client
                        .table("domain_labels")
                        .upsert(batch_data, count="exact")
                        .execute()
                )
                inserted = result.count or 0
                total_inserted += inserted
                logger.info(f"Batch {i//batch_size + 1}: Inserted {inserted}/{len(batch)} domains")
            except Exception as e:
                logger.error(f"Batch {i//batch_size + 1} failed: {e}")
                return {
                    "success": False,
                    "inserted": total_inserted,
                    "skipped": len(domains) - total_inserted,
                    "total": len(domains),
                    "error": str(e)
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
    
    def retry_failed_domains(self, limit: int = 1000) -> List[Dict[str, Any]]:
        result = self._safe_execute(
            self.client.table("domain_labels")
            .select("*")
            .or_(
                "explanation.ilike.%Fallback failed:%,"
                "explanation.ilike.%Failed to parse json%"
            )
            .order("last_classified", desc=True)
            .limit(limit),
            "Error getting failed domains"
        )
        return result or []

    
    def get_low_confidence_domains(self, limit: int = 500) -> List[Dict[str, Any]]:
        result = self._safe_execute(
            self.client.table("domain_labels")
            .select("*")
            .not_.is_("scraped_text", None)
            .lte("confidence", 7)
            .not_.in_("category", ["unknown", "error"])
            .order("last_classified", desc=True)
            .limit(limit),
            "Error getting low-confidence domains"
        )
        return result or []
    
    def export_classified_domains(self, output_file: str = "classified_domains.csv", batch_size: int = 1000) -> None:
        """
        Export all classified domains (domain, category, confidence, explanation) to a CSV file.
        """
        offset = 0
        total_exported = 0

        with open(output_file, "w", encoding="utf-8") as f:
            # Write CSV header
            f.write("domain,category,confidence,explanation\n")

            while True:
                result = self._safe_execute(
                    self.client.table("domain_labels")
                    .select("domain, category, confidence, explanation")
                    .not_.is_("category", None)
                    .range(offset, offset + batch_size - 1),
                    f"Error exporting classified domains (offset {offset})"
                )

                if not result:
                    break

                for row in result:
                    domain = row["domain"]
                    category = row.get("category", "")
                    confidence = row.get("confidence", 0)
                    explanation = row.get("explanation", "").replace("\n", " ").replace(",", ";")  # Clean CSV
                    f.write(f"{domain},{category},{confidence},{explanation}\n")
                    total_exported += 1

                if len(result) < batch_size:
                    break  # Reached the end
                offset += batch_size

        logger.info(f"Exported {total_exported} classified domains to {output_file}")

    
db = SupabaseClient()

