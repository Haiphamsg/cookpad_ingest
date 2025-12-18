BOT_NAME = "cookpad_ingest"
SPIDER_MODULES = ["cookpad_ingest.spiders"]
NEWSPIDER_MODULE = "cookpad_ingest.spiders"

ROBOTSTXT_OBEY = True

DOWNLOAD_DELAY = 1.0
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS = 4
CONCURRENT_REQUESTS_PER_DOMAIN = 2

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 20.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0

RETRY_ENABLED = True
RETRY_TIMES = 5
RETRY_HTTP_CODES = [429, 500, 502, 503, 504]

COOKIES_ENABLED = True

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi,en-US;q=0.7,en;q=0.3",
}

ITEM_PIPELINES = {
    "cookpad_ingest.pipelines.SupabaseStagingUpsertPipeline": 300,
}

LOG_LEVEL = "INFO"
