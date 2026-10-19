# Environment-based config with safety controls

import os

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = "sqlite:///job_apply_agent.db"

# Optional mirror of each scraped jobs row. Unset leaves SQLite as the only store.
MONGODB_URI = (os.getenv("MONGODB_URI") or "").strip()
MONGODB_DB = (os.getenv("MONGODB_DB") or "EngineerDB").strip() or "EngineerDB"
MONGODB_JOBS_COLLECTION = (os.getenv("MONGODB_JOBS_COLLECTION") or "scraping_pool").strip() or "scraping_pool"

# Safety settings
DRY_RUN = True  # Default to safe mode
SAFETY_LIMITS = {
    "max_applications_per_day": 2000,
    "max_auto_opens_per_session": 10,
    "require_confirmation_after": 3,
    "applied_to_same_company_within": 30
}

# Rate limiting
RATE_LIMIT_DELAY = 2  # seconds between API calls
MAX_RETRIES = 3
RETRY_BACKOFF = 10  # seconds

# Job sources (API keys stored in environment variables if needed later)
REMOTIVE_API_URL = "https://remotive.com/api/remote-jobs"
REMOTEOK_API_URL = "https://remoteok.com/api"

# Ollama / LLM analysis
# "local" → http://localhost:11434 (no key)
# "cloud" → https://ollama.com/api/chat (OLLAMA_API_KEY in .env)
OLLAMA_MODE = "cloud"
OLLAMA_API_KEY = (os.getenv("OLLAMA_API_KEY") or "").strip()
OLLAMA_LOCAL_URL = "http://localhost:11434/api/chat"
OLLAMA_CLOUD_URL = "https://ollama.com/api/chat"
OLLAMA_URL = OLLAMA_CLOUD_URL if OLLAMA_MODE == "cloud" else OLLAMA_LOCAL_URL
OLLAMA_LOCAL_MODEL = "qwen2.5:3b"
OLLAMA_CLOUD_MODEL = "gpt-oss:120b-cloud"
OLLAMA_MODEL = OLLAMA_CLOUD_MODEL if OLLAMA_MODE == "cloud" else OLLAMA_LOCAL_MODEL
LLM_TIMEOUT = 120
LLM_MAX_JOBS_PER_RUN = 2000
MAX_JOB_AGE_DAYS = 2  # Incremental fetch window after a source has been ingested
MAX_JOB_AGE_DAYS_INITIAL = 3  # First completed fetch for a source (jobs_fetched > 0)
MAX_DEDUPLICATION_PERIOD = 7  # Days to deduplicate jobs
REJECT_STALE_DAYS = 3  # Review and shortlisted jobs older than this can be bulk-archived
LLM_STATUS_DEFAULT = "review"
LLM_PROMOTION_CONFIDENCE = 75
