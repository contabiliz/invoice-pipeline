import logging
from uuid import UUID

import psycopg
import psycopg.rows
from psycopg_pool import AsyncConnectionPool

from invoice_pipeline import settings

logger = logging.getLogger(__name__)
_pool: AsyncConnectionPool | None = None


async def _get_pool() -> AsyncConnectionPool:  # type: ignore[type-arg]
    """Return the process-level connection, reconnecting if closed."""
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            settings.DATABASE_URL,
            min_size=2,
            max_size=10,
            open=False,
            kwargs={
                "autocommit": False,
                "prepare_threshold": 0,
                "row_factory": psycopg.rows.dict_row,
            },
        )
        await _pool.open()
    return _pool


async def fetch_promotable_documents(limit: int = 500) -> list[dict]:  # type: ignore[type-arg]
    """Fetch completed, unpromoted documents that have a chave_acesso.

    Used by the promotion job to populate entidades, invoices, invoice_items.
    Returns list of dicts with: id, user_id, doc_type, extracted_data.

    For PDF documents the chave_acesso lives inside a FieldOutput dict
    (extracted_data['chave_acesso']['value']), so the caller must check the
    actual value is non-null after parsing — the DB filter only guarantees
    the top-level key is present.
    """
    pool = await _get_pool()
    async with pool.connection() as conn:
        async with conn.transaction():
            rows = await (
                await conn.execute(
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
                )
            ).fetchall()
    return [dict(row) for row in rows]


async def promote_document(
    document_id: UUID,
    user_id: UUID,
    doc_type: str,
    chave_acesso: str,
    cnpj_emitente: str,
    nome_emitente: str,
    destinatario_cpf: str | None,
    destinatario_cnpj: str | None,
    nome_destinatario: str,
    data_emissao: str,
    numero_nota: str,
    serie: str,
    valor_total: str,
    confidence: int | None,
    items: list[dict],  # type: ignore[type-arg]
) -> None:
    """Promote one document to the analytics tables in a single transaction.

    All five writes (entidades upserts, invoices insert, invoice_items inserts,
    promoted_at update) commit atomically. If any step fails the transaction rolls
    back and promoted_at is not set, so the document will be retried on the next
    promotion run.

    All inserts use ON CONFLICT DO NOTHING — safe to call twice (idempotent).

    entidades receives the emitente (always a CNPJ) and the destinatário only when
    it is also a CNPJ. CPF destinatários are stored directly on invoices.destinatario_cpf
    and are never written to entidades.
    """
    pool = await _get_pool()
    async with pool.connection() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO entidades (documento, nome)
                VALUES (%s, %s)
                ON CONFLICT (documento) DO NOTHING
                """,
                (cnpj_emitente, nome_emitente),
            )
            if destinatario_cnpj is not None:
                await conn.execute(
                    """
                    INSERT INTO entidades (documento, nome)
                    VALUES (%s, %s)
                    ON CONFLICT (documento) DO NOTHING
                    """,
                    (destinatario_cnpj, nome_destinatario),
                )
            await conn.execute(
                """
                INSERT INTO invoices (
                    chave_acesso, document_id, user_id,
                    cnpj_emitente, destinatario_cnpj, destinatario_cpf, destinatario_nome,
                    data_emissao, numero_nota, serie,
                    valor_total, doc_type, confidence
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chave_acesso) DO NOTHING
                """,
                (
                    chave_acesso,
                    document_id,
                    user_id,
                    cnpj_emitente,
                    destinatario_cnpj,
                    destinatario_cpf,
                    nome_destinatario,
                    data_emissao,
                    numero_nota,
                    serie,
                    valor_total,
                    doc_type,
                    confidence,
                ),
            )
            for item in items:
                await conn.execute(
                    """
                    INSERT INTO invoice_items (
                        chave_acesso, user_id,
                        numero_item, codigo_produto, descricao,
                        ncm, cfop, cst_csosn, unidade,
                        quantidade, valor_unitario, valor_total_item
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chave_acesso, numero_item) DO NOTHING
                    """,
                    (
                        chave_acesso,
                        user_id,
                        item["numero_item"],
                        item["codigo_produto"],
                        item["descricao"],
                        item["ncm"],
                        item["cfop"],
                        item["cst_csosn"],
                        item["unidade"],
                        item.get("quantidade"),
                        item.get("valor_unitario"),
                        item.get("valor_total_item"),
                    ),
                )
            await conn.execute(
                """
                UPDATE documents
                SET promoted_at = NOW()
                WHERE id = %s
                """,
                (document_id,),
            )
    logger.info("promotion.document_promoted: document_id=%s", document_id)


