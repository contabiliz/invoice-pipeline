"""Client for BrasilAPI public REST endpoints.

Used only by the promotion job to enrich entity records with official data.
Callers must create a client via make_client() and pass it in — this avoids
binding a module-level client to an event loop that is closed between Celery
beat ticks (asyncio.run() closes the loop on each invocation).
"""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)


def make_client() -> httpx.AsyncClient:
    """Return a new AsyncClient configured for BrasilAPI."""
    return httpx.AsyncClient(
        base_url="https://brasilapi.com.br/api",
        timeout=10.0,
        headers={"Accept": "application/json"},
    )


async def fetch_razao_social_async(documento: str, client: httpx.AsyncClient) -> str | None:
    """Return razao_social for a CNPJ from BrasilAPI, or None on any failure.

    Accepts the documento in any format (formatted or digits-only).
    Returns None for CPF documents (11 digits) — BrasilAPI does not expose CPF data.
    """
    digits = "".join(c for c in documento if c.isdigit())

    # CPF or invalid — no API lookup available
    if len(digits) != 14:
        return None

    try:
        response = await client.get(f"/cnpj/v1/{digits}")

        if response.status_code == 200:
            return response.json().get("razao_social")

        logger.warning(
            "brasilapi.cnpj_lookup_failed", extra={"cnpj": digits, "status": response.status_code}
        )
        return None
    except Exception:
        logger.warning("brasilapi.cnpj_request_error", extra={"cnpj": digits}, exc_info=True)
        return None
