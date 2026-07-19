FROM python:3.11-slim AS base

# Prevent Python from writing .pyc and enable unbuffered stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (psycopg2 build, etc.)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        postgresql-client \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir psycopg[binary]

# App code
COPY . .

# Create directories for persistent data
RUN mkdir -p /app/models /app/cache /app/logs /tmp/backups

# ✅ SECURITY: Create non-root user
RUN groupadd -r embytrakt && useradd -r -g embytrakt embytrakt
RUN chown -R embytrakt:embytrakt /app

# Switch to non-root user
USER embytrakt

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["sh", "-c", "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000 --log-level info"]

