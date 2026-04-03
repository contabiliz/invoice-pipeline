"""Environment-based configuration for invoice-pipeline.

All settings are read from environment variables at import time.
No defaults for secrets — missing required variables fail fast at startup.
"""

from __future__ import annotations

import os


def _require(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"Required environment variable {name!r} is not set")
    return value


def _get(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


# --- Broker ---
REDIS_URL: str = _get("REDIS_URL", "redis://localhost:6379/0")
CELERY_BROKER_URL: str = _get("CELERY_BROKER_URL", REDIS_URL)

# --- Database (PgBouncer transaction pooling endpoint) ---
DATABASE_URL: str = _require("DATABASE_URL")

# --- Supabase Storage ---
SUPABASE_STORAGE_URL: str = _require("SUPABASE_STORAGE_URL")
SUPABASE_SERVICE_ROLE_KEY: str = _require("SUPABASE_SERVICE_ROLE_KEY")
STORAGE_BUCKET: str = _get("STORAGE_BUCKET", "documents")

# --- Gemini (worker-img only) ---
GEMINI_API_KEY: str = _get("GEMINI_API_KEY")
GEMINI_RATE_LIMIT_RPM: int = int(_get("GEMINI_RATE_LIMIT_RPM", "60"))

# --- Redis key for Gemini rate limiter ---
GEMINI_RATE_LIMITER_KEY: str = _get("GEMINI_RATE_LIMITER_KEY", "invoice:gemini:rate_limiter")
