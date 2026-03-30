# Build context: contabiliz/ (parent directory)
# Set in docker-compose.yml: build.context: ..
#
# The same image runs all worker types and the Consumer.
# Queue, pool, and concurrency are controlled by env vars at launch.
#
# Usage in docker-compose:
#   worker-pdf:
#     command: celery -A invoice_pipeline.celery_app worker --queues=queue.pdf --pool=prefork

# ── Stage 1: install dependencies ──────────────────────────────────────────
FROM python:3.12-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libxml2-dev \
    libxslt-dev \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Local dependency: invoice-processors (must be copied before the app)
COPY invoice-processors/ /deps/invoice-processors/

# Application source
COPY invoice-pipeline/ /app/

# Rewrite the local path so it resolves inside the container.
# In development, pyproject.toml points to ../invoice-processors (host path).
# Inside the image, it lives at /deps/invoice-processors.
RUN sed -i \
    's|path = "\.\./invoice-processors"|path = "/deps/invoice-processors"|' \
    pyproject.toml

# invoice-models is a private GitHub dependency — requires a token at build time.
# The secret is mounted read-only and never written into any image layer.
RUN --mount=type=secret,id=github_token \
    export GITHUB_TOKEN=$(cat /run/secrets/github_token) \
    && git config --global url."https://x-access-token:${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/" \
    && uv sync --no-dev \
    && git config --global --remove-section "url.https://x-access-token:${GITHUB_TOKEN}@github.com/"


# ── Stage 2: runtime ───────────────────────────────────────────────────────
FROM python:3.12-slim-bookworm AS runtime

WORKDIR /app

# System packages required at runtime:
#   poppler-utils  → pdf2image (PDF → image rasterization)
#   libmagic1      → python-magic (file type detection)
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    libmagic1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1001 appgroup \
    && useradd -u 1001 -g appgroup -s /bin/sh -m appuser

# Carry over the venv and source from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src   /app/src

# invoice-processors editable install: the .pth file inside the venv points here
COPY --from=builder /deps /deps

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src"

# tmpfs is mounted at /tmp in docker-compose for pdf2image output.
# The container writes rasterized PDF pages there, not to memory.
VOLUME ["/tmp"]

USER 1001

# Default: consumer. Override CMD in docker-compose for workers.
CMD ["python", "-m", "invoice_pipeline.consumer"]
