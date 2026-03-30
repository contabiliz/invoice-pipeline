"""Celery task: Dead Letter Queue worker.

Receives tasks that have exhausted all retries.
Writes terminal failure state to DB and logs for manual review.

Pool: prefork, concurrency 4 (set via WORKER_CONCURRENCY env var at launch)
No retries — DLQ entries are held for manual inspection.
"""

from __future__ import annotations

import structlog
import structlog.contextvars
from invoice_models.tasks import BaseTaskPayload

from invoice_pipeline import db
from invoice_pipeline.celery_app import app

logger = structlog.get_logger(__name__)


@app.task(
    bind=True,
    name="invoice.tasks.handle_dlq",
    max_retries=0,
)
def handle_dlq(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
    payload = BaseTaskPayload.model_validate(kwargs)

    structlog.contextvars.bind_contextvars(
        document_id=str(payload.document_id),
        batch_id=str(payload.batch_id),
        retry_count=self.request.retries,
    )

    try:
        _run(payload)
    finally:
        structlog.contextvars.clear_contextvars()


def _run(payload: BaseTaskPayload) -> None:
    logger.error(
        "dlq.task_received",
        s3_key=payload.s3_key,
    )

    error_reason = "retries_exhausted: task moved to DLQ after all retry attempts failed"

    db.fail_document(payload.document_id, payload.batch_id, error_reason)

    logger.info("dlq.document_failed")
