# =============================================================================
# MASX AI ETL CPU Pipeline - Production Docker Image
# =============================================================================
FROM python:3.12-slim AS builder
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1

# -----------------------------------------------------------------------------
# Build dependencies (compile wheels once)
# -----------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ pkg-config \
    libicu-dev libxml2-dev libxslt1-dev libffi-dev libssl-dev \
    libjpeg-dev libpng-dev libtiff-dev libwebp-dev \
    libpq-dev tesseract-ocr tesseract-ocr-eng curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements-prod.txt .
RUN python -m pip install --upgrade pip==24.2 && \
    pip install --no-cache-dir -r requirements-prod.txt && \
    python -m spacy download xx_ent_wiki_sm --direct --no-cache && \
    python -m spacy validate

# -----------------------------------------------------------------------------
# Stage 2: Runtime (small, secure)
# -----------------------------------------------------------------------------
FROM python:3.12-slim
RUN groupadd -r appuser && useradd -r -g appuser appuser
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libicu-dev libxml2 libxslt1.1 libffi8 libssl3 \
    libjpeg-turbo-progs libpng16-16 libtiff6 libwebp7 libpq5 \
    tesseract-ocr tesseract-ocr-eng curl \
    && rm -rf /var/lib/apt/lists/*

# Copy built packages from builder
COPY --from=builder /usr/local /usr/local

# -----------------------------------------------------------------------------
# Path + Environment Setup
# -----------------------------------------------------------------------------
# Both /app and /app/src visible for imports (src.*)
ENV PYTHONPATH="/app"
ENV PATH="/usr/local/bin:${PATH}"

# Copy application source
COPY . .

# Clean extra files
RUN rm -rf tests .git .env.example debug.py *.md LICENSE && \
    find . -name "__pycache__" -type d -exec rm -rf {} +

USER appuser
EXPOSE 8000

# -----------------------------------------------------------------------------
# Healthcheck
# -----------------------------------------------------------------------------
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# -----------------------------------------------------------------------------
# Final CMD: Run FastAPI directly via Uvicorn (no run.py)
# -----------------------------------------------------------------------------
CMD ["uvicorn", "api.server:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
