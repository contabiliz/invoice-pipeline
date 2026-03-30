"""Consumer — bridges upload events to Celery processing queues.

Runs as a standalone long-lived process (not a Celery worker).
Uses the synchronous redis-py pub/sub interface.

Lifecycle:
  1. Subscribe to Redis channel 'uploads.confirmed'
  2. For each message: fetch doc_type from DB, publish Celery task, update status
  3. Idempotency: discard message if document is not in upload_confirmed status

See spec: 03.1-specification.md
"""

from __future__ import annotations

import json
import logging
import sys
from uuid import UUID

import redis
import structlog
import structlog.contextvars
from invoice_models.enums import DocType
from invoice_models.tasks import (
    ImageTaskPayload,
    PdfTaskPayload,
    UploadConfirmationNotification,
    XmlTaskPayload,
)

from invoice_pipeline import db, settings
from invoice_pipeline.celery_app import app as celery_app

logger = structlog.get_logger(__name__)

CHANNEL = "uploads.confirmed"

_TASK_NAME: dict[str, str] = {
    DocType.xml.value: "invoice.tasks.process_xml",
    DocType.pdf.value: "invoice.tasks.process_pdf",
    DocType.img.value: "invoice.tasks.process_image",
}
_QUEUE_NAME: dict[str, str] = {
    DocType.xml.value: "queue.xml",
    DocType.pdf.value: "queue.pdf",
    DocType.img.value: "queue.img",
}
_PAYLOAD_CLASS = {
    DocType.xml.value: XmlTaskPayload,
    DocType.pdf.value: PdfTaskPayload,
    DocType.img.value: ImageTaskPayload,
}


def run() -> None:
    """Entry point — blocks forever, processing upload notifications."""
    _configure_logging()

    r: redis.Redis = redis.from_url(settings.REDIS_URL, decode_responses=True)  # type: ignore[type-arg]
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(CHANNEL)

    logger.info("consumer.started", channel=CHANNEL)

    try:
        for message in pubsub.listen():
            if message is None or message.get("type") != "message":
                continue
            _handle_message(message["data"])
    except KeyboardInterrupt:
        logger.info("consumer.shutdown")
    finally:
        pubsub.unsubscribe()
        r.close()


def _handle_message(data: str) -> None:
    """Parse and dispatch one upload notification.  Never raises."""
    try:
        raw = json.loads(data)
        notif = UploadConfirmationNotification.model_validate(raw)
    except Exception:
        logger.exception("consumer.invalid_message")
        return

    structlog.contextvars.bind_contextvars(
        document_id=str(notif.document_id),
        batch_id=str(notif.batch_id),
    )
    try:
        _dispatch(notif)
    except Exception:
        logger.exception("consumer.unhandled_error")
    finally:
        structlog.contextvars.clear_contextvars()


def _dispatch(notif: UploadConfirmationNotification) -> None:
    doc = db.get_document_for_routing(notif.document_id)

    if doc is None:
        logger.warning("consumer.document_not_found")
        return

    status = doc["status"]
    if status != "upload_confirmed":
        # Already queued or further — idempotent discard.
        logger.info("consumer.already_processed", status=status)
        return

    doc_type: str = doc["doc_type"]
    task_name = _TASK_NAME.get(doc_type)
    queue_name = _QUEUE_NAME.get(doc_type)
    PayloadClass = _PAYLOAD_CLASS.get(doc_type)

    if task_name is None or queue_name is None or PayloadClass is None:
        logger.error("consumer.unknown_doc_type", doc_type=doc_type)
        return

    payload = PayloadClass(
        document_id=notif.document_id,
        batch_id=notif.batch_id,
        user_id=UUID(str(doc["user_id"])),
        s3_key=doc["s3_key"],
    )

    # Publish the Celery task first.  Only update DB if publish succeeds.
    # This prevents the document from being stuck in 'queued' with no task.
    celery_app.send_task(
        task_name,
        kwargs=payload.model_dump(mode="json"),
        queue=queue_name,
    )

    db.transition_to_queued(notif.document_id, notif.batch_id)

    logger.info("consumer.task_published", doc_type=doc_type, queue=queue_name)


def _configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


if __name__ == "__main__":
    run()
