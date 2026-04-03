"""Celery task: Image branch.

Step 1: queued → processing
Step 2: download from Supabase Storage
Step 3: acquire Gemini rate limiter token
Step 4: processors.image.process(content, extractor) → ProcessorResult
Step 5: write result or terminal failure to DB

Pool: gevent, concurrency 100 (set via WORKER_CONCURRENCY env var at launch)

Retry policy:
  Gemini 429 (rate limit):   5 retries, 60s base, 10m cap
  Gemini 5xx (server error): 4 retries, 30s base, 5m cap
  Network / timeout:         3 retries, 30s linear

Terminal failures (no retry):
  Gemini 400/401, magic byte mismatch, decompression bomb, corrupt image

NOTE: This task delegates image processing to invoice-processors.
      The image processor module (processors.image) is not yet implemented.
      When it is, the import below will resolve automatically.
      Expected interface: process(content: bytes, extractor: Extractor) -> ProcessorResult
      where Extractor is defined in invoice-processors.
"""
from __future__ import annotations

import redis
import structlog
import structlog.contextvars
from invoice_models.tasks import ImageTaskPayload

from invoice_pipeline import db, settings, storage
from invoice_pipeline.celery_app import app
from invoice_pipeline.rate_limiter import get_limiter

logger = structlog.get_logger(__name__)

_GEMINI_429_MAX_RETRIES = 5
_GEMINI_5XX_MAX_RETRIES = 4
_NETWORK_MAX_RETRIES = 3
_MAX_RETRIES = max(_GEMINI_429_MAX_RETRIES, _GEMINI_5XX_MAX_RETRIES, _NETWORK_MAX_RETRIES)

_redis_client: redis.Redis | None = None  # type: ignore[type-arg]


def _get_redis() -> redis.Redis:  # type: ignore[type-arg]
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(settings.REDIS_URL)
    return _redis_client


@app.task(
    bind=True,
    name="invoice.tasks.process_image",
    max_retries=_MAX_RETRIES,
    default_retry_delay=30,
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
)
def process_image(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
    payload = ImageTaskPayload.model_validate(kwargs)

    structlog.contextvars.bind_contextvars(
        document_id=str(payload.document_id),
        batch_id=str(payload.batch_id),
        doc_type="img",
        source=payload.source,
        retry_count=self.request.retries,
    )

    try:
        _run(self, payload)
    finally:
        structlog.contextvars.clear_contextvars()


def _run(task, payload: ImageTaskPayload) -> None:
    db.transition_to_processing(payload.document_id)
    logger.info("task.processing")

    # --- Download ---
    try:
        content = storage.download(payload.s3_key)
    except Exception as exc:
        logger.warning("task.download_failed", error=str(exc))
        raise task.retry(exc=exc, max_retries=_NETWORK_MAX_RETRIES, countdown=30) from exc

    # --- Acquire Gemini rate limiter token before calling the processor ---
    limiter = get_limiter(_get_redis())
    try:
        limiter.acquire()
    except TimeoutError:
        logger.warning("task.rate_limiter_timeout")
        raise task.retry(countdown=60, max_retries=_GEMINI_429_MAX_RETRIES) from None

    # --- Process ---
    try:
        from processors.image import GeminiExtractor  # type: ignore[import]
        from processors.image import process as image_process  # type: ignore[import]
    except ImportError as exc:
        # invoice-processors does not yet have an image processor.
        # See: invoice-processors/src/processors/image/ for the required interface.
        # Expected signatures:
        #   class GeminiExtractor:
        #       def __init__(self, api_key: str) -> None: ...
        #       def extract(self, image_bytes: bytes) -> dict: ...
        #   process(content: bytes, extractor: Extractor) -> ProcessorResult
        raise NotImplementedError(
            "invoice-processors image processor not implemented. "
            "Create processors/image/__init__.py with process(content, extractor) -> ProcessorResult "
            "and GeminiExtractor class."
        ) from exc

    extractor = GeminiExtractor(api_key=settings.GEMINI_API_KEY)

    try:
        result = image_process(content, extractor)
    except Exception as exc:
        # Prefer a structured status_code attribute on the exception (set by the
        # processor). Fall back to substring matching for processors that raise
        # plain exceptions with the status code embedded in the message.
        status_code = getattr(exc, "status_code", None)
        error_str = str(exc)
        # Terminal Gemini errors — no retry
        if status_code in (400, 401) or ("400" in error_str or "401" in error_str):
            logger.warning("task.gemini_terminal", error=error_str)
            db.fail_document(payload.document_id, payload.batch_id, f"gemini_terminal: {error_str}")
            return
        # Gemini rate limit (429)
        if status_code == 429 or "429" in error_str:
            logger.warning("task.gemini_rate_limit")
            raise task.retry(exc=exc, countdown=60, max_retries=_GEMINI_429_MAX_RETRIES) from exc
        # Gemini server error (5xx)
        if status_code in (500, 502, 503, 504) or any(code in error_str for code in ("500", "502", "503", "504")):
            logger.warning("task.gemini_server_error")
            raise task.retry(exc=exc, countdown=30, max_retries=_GEMINI_5XX_MAX_RETRIES) from exc
        raise

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
