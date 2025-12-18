import os

from dotenv import load_dotenv
from supabase import create_client


class SupabaseStagingUpsertPipeline:
    def open_spider(self, spider):
        load_dotenv()
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        self.table = os.getenv("SUPABASE_STAGING_TABLE", "recipes_staging")
        if not self.url or not self.key:
            raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")
        self.client = create_client(self.url, self.key)

    def process_item(self, item, spider):
        payload = {
            "source_url": item["source_url"],
            "name": item.get("name"),
            "image_url": item.get("image_url"),
            "ingredients_raw": item.get("ingredients_raw") or [],
            "instructions_raw": item.get("instructions_raw") or [],
            "content_fingerprint": item["content_fingerprint"],
            "extract_status": item.get("extract_status", "ok"),
            "extract_error": item.get("extract_error"),
        }
        self.client.table(self.table).upsert(payload, on_conflict="source_url").execute()
        return item
