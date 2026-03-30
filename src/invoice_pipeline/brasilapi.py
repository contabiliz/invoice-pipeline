"""Client for BrasilAPI public REST endpoints.

Used only by the promotion job to enrich entity records with official data.
A single module-level httpx.Client is reused across calls (connection pooling).
"""
from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)

_client: httpx.Client | None = None


def _get_client() -> httpx.Client:
    """Return the module-level client, creating it on first use."""
    global _client
    if _client is None:
        _client = httpx.Client(
            base_url="https://brasilapi.com.br/api",
            timeout=10.0,
            headers={"Accept": "application/json"},
        )
    return _client


def fetch_razao_social(documento: str) -> str | None:
    """Return razao_social for a CNPJ from BrasilAPI, or None on any failure.

    Accepts the documento in any format (formatted or digits-only).
    Returns None for CPF documents (11 digits) — BrasilAPI does not expose CPF data.
    """
    digits = "".join(c for c in documento if c.isdigit())
    if len(digits) != 14:
        return None  # CPF or invalid — no API lookup available

    try:
        response = _get_client().get(f"/cnpj/v1/{digits}")
        if response.status_code == 200:
            return response.json().get("razao_social")
        logger.warning(
            "brasilapi.cnpj_lookup_failed",
            cnpj=digits,
            status=response.status_code,
        )
        return None
    except Exception:
        logger.warning("brasilapi.cnpj_request_error", cnpj=digits, exc_info=True)
        return None
