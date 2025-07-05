import os
import logging
from datetime import datetime
from dotenv import load_dotenv
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
