"""Celery application configuration.

All task names are explicit (never auto-generated from module paths).
If the package is renamed or a module is moved, auto-generated names change
silently and all queued tasks become unroutable.
"""

from __future__ import annotations

from celery import Celery

from invoice_pipeline import settings

app = Celery("invoice", broker=settings.CELERY_BROKER_URL)

app.conf.update(
    # Serialization — never use pickle (ADR-004 / SEC)
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # At-least-once delivery (ADR-001)
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # All state lives in PostgreSQL — no result backend (ADR-003)
    result_backend=None,
    # Queue routing — one queue per branch (ADR-002)
    task_routes={
        "invoice.tasks.process_xml": {"queue": "queue.xml"},
        "invoice.tasks.process_pdf": {"queue": "queue.pdf"},
        "invoice.tasks.process_image": {"queue": "queue.img"},
        "invoice.tasks.handle_dlq": {"queue": "queue.dlq"},
        "invoice.tasks.promote_documents": {"queue": "queue.promotion"},
    },
    # Periodic promotion job — runs every 5 minutes (05.2 §05.2.4)
    beat_schedule={
        "promote-documents": {
            "task": "invoice.tasks.promote_documents",
            "schedule": 1 * 60,  # seconds
        },
    },
    timezone="UTC",
    enable_utc=True,
)

# Register task modules so @app.task decorators are discovered on worker start.
app.conf.include = [
    "invoice_pipeline.tasks.xml_task",
    "invoice_pipeline.tasks.pdf_task",
    "invoice_pipeline.tasks.image_task",
    "invoice_pipeline.tasks.dlq_task",
    "invoice_pipeline.tasks.promotion_task",
]
