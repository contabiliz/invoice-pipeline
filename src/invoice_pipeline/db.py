"""Synchronous database access using psycopg3.

One long-lived connection per worker process, reused across all task executions.
Reconnects automatically if the connection is dropped.

PgBouncer transaction pooling compatibility:
  - prepare_threshold=0 disables prepared statements
  - No session-level features (advisory locks, SET LOCAL, temp tables)
  - Each operation acquires and releases within a single transaction
"""
from __future__ import annotations

import json
import logging
from uuid import UUID

import psycopg
import psycopg.rows

from invoice_pipeline import settings

logger = logging.getLogger(__name__)

_conn: psycopg.Connection | None = None  # type: ignore[type-arg]


def _get_conn() -> psycopg.Connection:  # type: ignore[type-arg]
    """Return the process-level connection, reconnecting if closed."""
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg.connect(
            settings.DATABASE_URL,
            autocommit=False,
            prepare_threshold=0,  # Required for PgBouncer transaction pooling
            row_factory=psycopg.rows.dict_row,
        )
    return _conn


# ---------------------------------------------------------------------------
# Consumer operations
# ---------------------------------------------------------------------------


def get_document_for_routing(document_id: UUID) -> dict | None:  # type: ignore[type-arg]
    """Fetch fields needed by the Consumer to route a document.

    Returns dict with: status, doc_type, s3_key, user_id
    Returns None if the document does not exist.
    """
    conn = _get_conn()
    with conn.transaction():
        row = conn.execute(
            """
            SELECT status, doc_type, s3_key, user_id
            FROM documents
            WHERE id = %s
            """,
            (document_id,),
        ).fetchone()
    return dict(row) if row else None


def transition_to_queued(document_id: UUID, batch_id: UUID) -> None:
    """upload_confirmed → queued.  Increment batch.queued_count.

    Called by the Consumer AFTER the Celery task has been successfully published.
    If the document is not in upload_confirmed, this is a no-op (idempotency).
    """
    conn = _get_conn()
    with conn.transaction():
        updated = conn.execute(
            """
            UPDATE documents
            SET status = 'queued', updated_at = NOW()
            WHERE id = %s AND status = 'upload_confirmed'
            """,
            (document_id,),
        ).rowcount
        if updated:
            conn.execute(
                """
                UPDATE batches
                SET queued_count = queued_count + 1, updated_at = NOW()
                WHERE id = %s
                """,
                (batch_id,),
            )


# ---------------------------------------------------------------------------
# Task operations
# ---------------------------------------------------------------------------


def transition_to_processing(document_id: UUID) -> None:
    """queued → processing.  First line of every task.

    Idempotent: if already processing (redelivery), the UPDATE affects 0 rows
    and proceeds without error.
    """
    conn = _get_conn()
    with conn.transaction():
        conn.execute(
            """
            UPDATE documents
            SET status = 'processing',
                processing_started_at = COALESCE(processing_started_at, NOW()),
                updated_at = NOW()
            WHERE id = %s AND status IN ('queued', 'processing')
            """,
            (document_id,),
        )


def complete_document(
    document_id: UUID,
    batch_id: UUID,
    extracted_data: dict,  # type: ignore[type-arg]
) -> None:
    """processing → completed.  Write extracted data.  Check batch completion.

    Upsert semantics: safe to call twice (redelivery idempotency).
    """
    conn = _get_conn()
    with conn.transaction():
        conn.execute(
            """
            UPDATE documents
            SET status = 'completed',
                extracted_data = %s::jsonb,
                completed_at = COALESCE(completed_at, NOW()),
                updated_at = NOW()
            WHERE id = %s
            """,
            (json.dumps(extracted_data), document_id),
        )
        row = conn.execute(
            """
            UPDATE batches
            SET completed_count = completed_count + 1, updated_at = NOW()
            WHERE id = %s
            RETURNING completed_count, failed_count, total_documents
            """,
            (batch_id,),
        ).fetchone()
        if row:
            _maybe_close_batch(conn, batch_id, row)


def fail_document(document_id: UUID, batch_id: UUID, reason: str) -> None:
    """processing → failed.  Write error reason.  Check batch completion.

    Idempotent: safe to call twice.
    """
    conn = _get_conn()
    with conn.transaction():
        conn.execute(
            """
            UPDATE documents
            SET status = 'failed',
                error_reason = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (reason, document_id),
        )
        row = conn.execute(
            """
            UPDATE batches
            SET failed_count = failed_count + 1, updated_at = NOW()
            WHERE id = %s
            RETURNING completed_count, failed_count, total_documents
            """,
            (batch_id,),
        ).fetchone()
        if row:
            _maybe_close_batch(conn, batch_id, row)


# ---------------------------------------------------------------------------
# Promotion operations (analytics layer)
# ---------------------------------------------------------------------------


def fetch_promotable_documents(limit: int = 500) -> list[dict]:  # type: ignore[type-arg]
    """Fetch completed, unpromoted documents that have a chave_acesso.

    Used by the promotion job to populate entidades, invoices, invoice_items.
    Returns list of dicts with: id, user_id, doc_type, extracted_data.

    For PDF documents the chave_acesso lives inside a FieldOutput dict
    (extracted_data['chave_acesso']['value']), so the caller must check the
    actual value is non-null after parsing — the DB filter only guarantees
    the top-level key is present.
    """
    conn = _get_conn()
    with conn.transaction():
        rows = conn.execute(
            """
            SELECT id, user_id, doc_type, extracted_data
            FROM documents
            WHERE status = 'completed'
              AND promoted_at IS NULL
              AND extracted_data->>'chave_acesso' IS NOT NULL
            ORDER BY completed_at
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def promote_document(
    document_id: UUID,
    user_id: UUID,
    doc_type: str,
    chave_acesso: str,
    cnpj_emitente: str,
    nome_emitente: str,
    documento_destinatario: str,
    nome_destinatario: str,
    data_emissao: str,
    numero_nota: str,
    serie: str,
    valor_total: str,
    confidence: int | None,
    items: list[dict],  # type: ignore[type-arg]
) -> None:
    """Promote one document to the analytics tables in a single transaction.

    All five writes (2x entidades upsert, invoices insert, invoice_items inserts,
    promoted_at update) commit atomically. If any step fails the transaction rolls
    back and promoted_at is not set, so the document will be retried on the next
    promotion run.

    All inserts use ON CONFLICT DO NOTHING — safe to call twice (idempotent).
    """
    conn = _get_conn()
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO entidades (documento, nome)
            VALUES (%s, %s)
            ON CONFLICT (documento) DO NOTHING
            """,
            (cnpj_emitente, nome_emitente),
        )
        conn.execute(
            """
            INSERT INTO entidades (documento, nome)
            VALUES (%s, %s)
            ON CONFLICT (documento) DO NOTHING
            """,
            (documento_destinatario, nome_destinatario),
        )
        conn.execute(
            """
            INSERT INTO invoices (
                chave_acesso, document_id, user_id,
                cnpj_emitente, documento_destinatario,
                data_emissao, numero_nota, serie,
                valor_total, doc_type, confidence
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (chave_acesso) DO NOTHING
            """,
            (
                chave_acesso, document_id, user_id,
                cnpj_emitente, documento_destinatario,
                data_emissao, numero_nota, serie,
                valor_total, doc_type, confidence,
            ),
        )
        for item in items:
            conn.execute(
                """
                INSERT INTO invoice_items (
                    chave_acesso, user_id,
                    numero_item, codigo_produto, descricao,
                    ncm, cfop, unidade,
                    quantidade, valor_unitario, valor_total_item
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chave_acesso, numero_item) DO NOTHING
                """,
                (
                    chave_acesso, user_id,
                    item["numero_item"], item["codigo_produto"], item["descricao"],
                    item["ncm"], item["cfop"], item["unidade"],
                    item.get("quantidade"), item.get("valor_unitario"), item.get("valor_total_item"),
                ),
            )
        conn.execute(
            """
            UPDATE documents
            SET promoted_at = NOW()
            WHERE id = %s
            """,
            (document_id,),
        )
    logger.info("promotion.document_promoted: document_id=%s", document_id)


def _maybe_close_batch(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    batch_id: UUID,
    row: dict,  # type: ignore[type-arg]
) -> None:
    """Update batch status if all documents have a terminal state.

    Called within an existing transaction — no new transaction opened here.
    """
    completed = row["completed_count"]
    failed = row["failed_count"]
    total = row["total_documents"]

    if completed + failed < total:
        return

    if failed == 0:
        new_status = "completed"
    elif completed == 0:
        new_status = "failed"
    else:
        new_status = "partial_failure"

    conn.execute(
        """
        UPDATE batches
        SET status = %s, updated_at = NOW()
        WHERE id = %s AND status NOT IN ('completed', 'partial_failure', 'failed')
        """,
        (new_status, batch_id),
    )
    logger.info("batch closed: batch_id=%s status=%s", batch_id, new_status)
