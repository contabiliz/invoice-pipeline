"""Supabase Storage download client.

Uses a persistent httpx session (not recreated per task) to avoid TCP
connection overhead on every file download.
"""
from __future__ import annotations

import httpx

from invoice_pipeline import settings

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0))
    return _client


def download(s3_key: str) -> bytes:
    """Download a file from Supabase Storage by object key.

    Raises httpx.HTTPStatusError on non-2xx responses (caller handles retry).
    Raises httpx.TimeoutException on network timeout (caller handles retry).
    """
    url = (
        f"{settings.SUPABASE_STORAGE_URL}"
        f"/storage/v1/object/{settings.STORAGE_BUCKET}/{s3_key}"
    )
    headers = {
        "Authorization": f"Bearer {settings.SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": settings.SUPABASE_SERVICE_ROLE_KEY,
    }
    response = _get_client().get(url, headers=headers)
    response.raise_for_status()
    return response.content
