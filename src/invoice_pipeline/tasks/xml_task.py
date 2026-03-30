"""Celery task: XML branch.

Step 1: queued → processing
Step 2: download from Supabase Storage
Step 3: processors.xml.process(content) → ProcessorResult
Step 4: write result or terminal failure to DB

Pool: gevent, concurrency 50 (set via WORKER_CONCURRENCY env var at launch)
Max retries: 4 (network / transient errors only — XML structural failures are always terminal)
"""

from __future__ import annotations

import structlog
import structlog.contextvars
from invoice_models.tasks import XmlTaskPayload

from invoice_pipeline import db, storage
from invoice_pipeline.celery_app import app

logger = structlog.get_logger(__name__)


@app.task(
    bind=True,
    name="invoice.tasks.process_xml",
    max_retries=4,
    default_retry_delay=30,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
def process_xml(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
    payload = XmlTaskPayload.model_validate(kwargs)

    structlog.contextvars.bind_contextvars(
        document_id=str(payload.document_id),
        batch_id=str(payload.batch_id),
        doc_type="xml",
        retry_count=self.request.retries,
    )

    try:
        _run(self, payload)
    finally:
        structlog.contextvars.clear_contextvars()


def _run(task, payload: XmlTaskPayload) -> None:
    db.transition_to_processing(payload.document_id)
    logger.info("task.processing")

    # --- Download ---
    try:
        content = storage.download(payload.s3_key)
    except Exception as exc:
        logger.warning("task.download_failed", error=str(exc))
        raise task.retry(exc=exc) from exc

    # --- Process ---
    from processors.xml import process as xml_process  # type: ignore[import]

    result = xml_process(content)

    # --- Persist ---
    if result.is_terminal_failure:
        logger.warning("task.terminal_failure", reason=result.terminal_reason)
        db.fail_document(payload.document_id, payload.batch_id, result.terminal_reason or "unknown")
        return

    assert result.result is not None
    db.complete_document(
        payload.document_id,
        payload.batch_id,
        result.result.model_dump(mode="json"),
    )
    logger.info("task.completed")
