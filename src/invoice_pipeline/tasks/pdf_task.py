"""Celery task: PDF branch.

Step 1: queued → processing
Step 2: download from Supabase Storage
Step 3: processors.pdf.process(content) → ProcessorResult
Step 4: write result to DB, or reroute to image queue if routing_override=IMAGE

Pool: prefork, concurrency nproc (set via WORKER_CONCURRENCY env var at launch)
Max retries: 4 (network / transient errors — PDF structural failures are terminal)
"""

from __future__ import annotations

import structlog
import structlog.contextvars
from invoice_models.extraction import RoutingOverride
from invoice_models.tasks import ImageTaskPayload, PdfTaskPayload

from invoice_pipeline import db, storage
from invoice_pipeline.celery_app import app

logger = structlog.get_logger(__name__)


@app.task(
    bind=True,
    name="invoice.tasks.process_pdf",
    max_retries=4,
    default_retry_delay=30,
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
)
def process_pdf(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
    payload = PdfTaskPayload.model_validate(kwargs)

    structlog.contextvars.bind_contextvars(
        document_id=str(payload.document_id),
        batch_id=str(payload.batch_id),
        doc_type="pdf",
        retry_count=self.request.retries,
    )

    try:
        _run(self, payload)
    finally:
        structlog.contextvars.clear_contextvars()


def _run(task, payload: PdfTaskPayload) -> None:
    db.transition_to_processing(payload.document_id)
    logger.info("task.processing")

    # --- Download ---
    try:
        content = storage.download(payload.s3_key)
    except Exception as exc:
        logger.warning("task.download_failed", error=str(exc))
        raise task.retry(exc=exc) from exc

    # --- Process ---
    from processors.pdf import process as pdf_process  # type: ignore[import]

    try:
        result = pdf_process(content)
    except Exception as exc:
        logger.warning("task.process_failed", error=str(exc))
        raise task.retry(exc=exc) from exc

    # --- Handle result ---
    if result.is_terminal_failure:
        logger.warning("task.terminal_failure", reason=result.terminal_reason)
        db.fail_document(payload.document_id, payload.batch_id, result.terminal_reason or "unknown")
        return

    if result.routing_override == RoutingOverride.IMAGE:
        # Insufficient text — reroute to image branch.
        # source="pdf_fallback" prevents the image task from trying PDF processing again.
        image_payload = ImageTaskPayload(
            document_id=payload.document_id,
            batch_id=payload.batch_id,
            user_id=payload.user_id,
            s3_key=payload.s3_key,
            source="pdf_fallback",
        )
        app.send_task(
            "invoice.tasks.process_image",
            kwargs=image_payload.model_dump(mode="json"),
            queue="queue.img",
        )
        logger.info("task.rerouted_to_image")
        return

    assert result.result is not None
    db.complete_document(
        payload.document_id,
        payload.batch_id,
        result.result.model_dump(mode="json"),
    )
    logger.info("task.completed")
