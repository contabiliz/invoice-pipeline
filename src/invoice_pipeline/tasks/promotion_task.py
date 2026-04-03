"""Celery task: Analytics promotion job.

Reads completed, unpromoted documents and promotes them to the analytics tables
(entidades, invoices, invoice_items). Runs as a periodic Celery Beat task.

Entity names (nome in entidades) are fetched from BrasilAPI using the CNPJ.
For CPF destinatários (11 digits), the name from extracted_data is used as
a fallback since BrasilAPI does not expose CPF data.

Each document is promoted in a single transaction. If the transaction fails,
promoted_at is not set and the document is retried on the next beat tick.

Workers do not write to analytics tables directly (05.2 ADR-006). This task
is the sole writer to entidades, invoices, and invoice_items.

Pool: prefork, concurrency 2 (low volume — shares queue.promotion worker)
Beat schedule: every 5 minutes (see celery_app.py)
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

import structlog

from invoice_pipeline import brasilapi, db_async
from invoice_pipeline.celery_app import app

logger = structlog.get_logger(__name__)

_BATCH_SIZE = 500
_CONCURRENT_REQUESTS = 30


@app.task(
    name="invoice.tasks.promote_documents",
    max_retries=0,
    ignore_result=True,
)
def promote_documents() -> None:
    """Promote a batch of completed documents to the analytics tables."""
    asyncio.run(_promote_documents_async())


async def _promote_documents_async() -> None:
    """Promote a batch of completed documents to the analytics tables."""
    rows = await db_async.fetch_promotable_documents(limit=_BATCH_SIZE)
    if not rows:
        return

    logger.info("promotion.batch_start", count=len(rows))

    # Phase 1: parse all documents and build the BrasilAPI identifier set in one pass.
    # destinatario_cnpj is computed here once and carried forward — avoids re-classifying
    # the same documento_destinatario string in the promotion loop.
    parsed: list[tuple[dict[str, Any], dict[str, Any], str | None, str | None]] = []
    identifiers: set[str] = set()
    skipped = 0

    for row in rows:
        fields, missing = _parse_extracted_data(row["doc_type"], row["extracted_data"])

        if fields is None:
            skipped += 1
            logger.warning(
                "promotion.document_skipped",
                document_id=str(row["id"]),
                doc_type=row["doc_type"],
                missing_fields=missing,
            )
            continue

        destinatario_cnpj, destinatario_cpf = _classify_destinatario(
            fields["documento_destinatario"]
        )
        identifiers.add(fields["cnpj_emitente"])
        if destinatario_cnpj is not None:
            identifiers.add(destinatario_cnpj)
        parsed.append((row, fields, destinatario_cnpj, destinatario_cpf))

    # Phase 2: batch-resolve entity names from BrasilAPI.
    resolved = await _resolve_entity_names(identifiers)

    # Phase 3: promote each document using the pre-resolved name dict.
    promoted = 0
    failed = 0
    for row, fields, destinatario_cnpj, destinatario_cpf in parsed:
        # For CPF destinatários (11 digits) BrasilAPI returns None — fall back to
        # whatever name the processor extracted, or a generic placeholder.
        nome_emitente = resolved.get(fields["cnpj_emitente"]) or fields["nome_emitente_fallback"]
        nome_destinatario = (
            (resolved.get(destinatario_cnpj) if destinatario_cnpj is not None else None)
            or fields["nome_destinatario_fallback"]
            or "Não identificado"
        )

        if nome_emitente is None:
            skipped += 1
            logger.warning(
                "promotion.document_skipped",
                document_id=str(row["id"]),
                doc_type=row["doc_type"],
                missing_fields=["nome_emitente (api_failed, no_fallback)"],
            )
            continue

        try:
            await db_async.promote_document(
                document_id=row["id"],
                user_id=row["user_id"],
                doc_type=row["doc_type"],
                chave_acesso=fields["chave_acesso"],
                cnpj_emitente=fields["cnpj_emitente"],
                nome_emitente=nome_emitente,
                destinatario_cnpj=destinatario_cnpj,
                destinatario_cpf=destinatario_cpf,
                nome_destinatario=nome_destinatario,
                data_emissao=fields["data_emissao"],
                numero_nota=fields["numero_nota"],
                serie=fields["serie"],
                valor_total=fields["valor_total"],
                confidence=fields["confidence"],
                items=fields["items"],
            )
            promoted += 1
        except Exception:
            failed += 1
            logger.exception(
                "promotion.document_failed",
                document_id=str(row["id"]),
                doc_type=row["doc_type"],
            )

    logger.info("promotion.batch_done", promoted=promoted, skipped=skipped, failed=failed)


async def _resolve_entity_names(
    identifiers: set[str],
) -> dict[str, str | None]:
    """Fetch razao_social for a deduplicated set of CNPJ/CPF identifiers.

    Each identifier is fetched exactly once. BrasilAPI failures are caught
    per-identifier so a single network error does not abort the entire batch.
    """

    sem = asyncio.Semaphore(_CONCURRENT_REQUESTS)

    async def fetch_one(identifier: str) -> tuple[str, str | None]:
        async with sem:
            try:
                name = await brasilapi.fetch_razao_social_async(identifier)
                return identifier, name
            except Exception:
                logger.warning("brasilapi.fecth_failed", identifier=identifier)
                return identifier, None

    results = await asyncio.gather(*[fetch_one(i) for i in identifiers])
    return dict(results)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _classify_destinatario(documento: str) -> tuple[str | None, str | None]:
    """Returns (destinatario_cnpj, destinatario_cpf) — exactly one is set, or (None, None) if malformed."""
    digits = re.sub(r"\D", "", documento)
    if len(digits) == 14:
        return digits, None  # company
    if len(digits) == 11:
        return None, digits  # individual
    return None, None  # malformed — neither CNPJ nor CPF


def _parse_extracted_data(
    doc_type: str,
    data: dict[str, Any],
) -> tuple[dict[str, Any] | None, list[str]]:
    """Extract promotion fields from extracted_data based on doc_type.

    Returns (fields, missing_fields). fields is None if the document cannot be
    promoted; missing_fields lists which required fields were absent or null.

    The returned dict includes nome_emitente_fallback and nome_destinatario_fallback
    keys when the processor extracted names — used if BrasilAPI is unavailable.
    """
    if doc_type == "xml":
        return _parse_xml(data)
    if doc_type == "pdf":
        return _parse_pdf(data)
    # Image documents (ImageExtractionResult) have no chave_acesso — not promotable.
    return None, ["doc_type_not_promotable"]


def _parse_xml(data: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Parse NfeXmlResult fields. All header fields are guaranteed present by the model."""
    destinatario_cnpj, destinatario_cpf = _classify_destinatario(data["documento_destinatario"])
    items = [
        {
            "numero_item": item["numero_item"],
            "codigo_produto": item["codigo_produto"],
            "descricao": item["descricao"],
            "ncm": item["ncm"],
            "cfop": item["cfop"],
            "cst_csosn": item["cst_csosn"],
            "unidade": item["unidade"],
            "quantidade": item.get("quantidade"),
            "valor_unitario": item.get("valor_unitario"),
            "valor_total_item": item.get("valor_total_item"),
            "destinatario_cnpj": destinatario_cnpj,
            "destinatario_cpf": destinatario_cpf,
        }
        for item in data.get("items", [])
    ]
    return {
        "chave_acesso": data["chave_acesso"],
        "cnpj_emitente": data["cnpj_emitente"],
        "nome_emitente_fallback": data.get("nome_emitente"),
        "documento_destinatario": data["documento_destinatario"],
        "nome_destinatario_fallback": data.get("nome_destinatario"),
        "data_emissao": data["data_emissao"],
        "numero_nota": data["numero_nota"],
        "serie": data["serie"],
        "valor_total": data["valor_total_nota"],
        "confidence": None,  # XML extraction is deterministic (05.2 ADR-004)
        "items": items,
    }, []


def _parse_pdf(data: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    """Parse PdfExtractionResult fields.

    PDF fields are FieldOutput dicts ({value, confidence, method, ...}).
    PdfExtractionResult has no nome_emitente — entity name comes from BrasilAPI.
    nome_destinatario is kept as a fallback for CPF destinatários.

    Line items missing required DB columns (ncm, cfop, unidade) are skipped.
    codigo_produto is not extractable from DANFE PDFs; it defaults to "".
    numero_item is synthesized from list position (1-based).
    """

    def _val(key: str) -> str | None:
        fo = data.get(key)
        return fo.get("value") if fo else None

    required = {
        "chave_acesso": _val("chave_acesso"),
        "cnpj_emitente": _val("cnpj_emitente"),
        "documento_destinatario": _val("documento_destinatario"),
        "data_emissao": _val("data_emissao"),
        "numero_nota": _val("numero_nota"),
        "serie": _val("serie"),
        "valor_total_nota": _val("valor_total_nota"),
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        return None, missing

    raw_confidence = data.get("table_confidence")
    confidence = round(raw_confidence * 100) if raw_confidence is not None else None

    items = []
    for i, item in enumerate(data.get("items", []), start=1):
        ncm = item.get("ncm")
        cfop = item.get("cfop")
        unidade = item.get("unidade")
        cst_csosn = item.get("cst_csosn")
        if not all((ncm, cfop, unidade, cst_csosn)):
            continue
        items.append(
            {
                "numero_item": i,
                "codigo_produto": "",
                "descricao": item["descricao"],
                "ncm": ncm,
                "cfop": cfop,
                "unidade": unidade,
                "cst_csosn": cst_csosn,
                "quantidade": item.get("quantidade"),
                "valor_unitario": item.get("valor_unitario"),
                "valor_total_item": item.get("valor_total_item"),
            }
        )

    return {
        "chave_acesso": required["chave_acesso"],
        "cnpj_emitente": required["cnpj_emitente"],
        "nome_emitente_fallback": None,  # PdfExtractionResult has no nome_emitente
        "documento_destinatario": required["documento_destinatario"],
        "nome_destinatario_fallback": _val("nome_destinatario"),
        "data_emissao": required["data_emissao"],
        "numero_nota": required["numero_nota"],
        "serie": required["serie"],
        "valor_total": required["valor_total_nota"],
        "confidence": confidence,
        "items": items,
    }, []
