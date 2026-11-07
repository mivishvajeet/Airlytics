# Production-grade Dockerfile for Airlytics OpenSky Ingestion Service on Cloud Run
FROM python:3.11-slim as builder

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY ingest/requirements.txt .
RUN pip install --user --no-warn-script-location -r requirements.txt

# Final lightweight runtime image
FROM python:3.11-slim as runner

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/home/appuser/.local/bin:$PATH"

# Run as non-root user for security best practices
RUN groupadd -r appuser && useradd -r -g appuser appuser && \
    mkdir -p /app && chown -R appuser:appuser /app

COPY --from=builder --chown=appuser:appuser /root/.local /home/appuser/.local
COPY --chown=appuser:appuser ingest/ /app/ingest/

USER appuser

ENV PYTHONPATH="/app"

ENTRYPOINT ["python", "-m", "ingest.main"]
