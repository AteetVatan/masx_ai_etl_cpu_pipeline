# =============================================================================
# MASX AI ETL CPU Pipeline - Production Docker Image
# =============================================================================
FROM python:3.12-slim AS builder
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1

# -----------------------------------------------------------------------------
# Build dependencies (compile wheels)
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
# Runtime (lightweight image)
# -----------------------------------------------------------------------------
FROM python:3.12-slim

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install only runtime libs (⚠️ fixed ICU package)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libicu-dev libxml2 libxslt1.1 libffi-dev libssl-dev \
    libjpeg62-turbo libpng16-16 libtiff6 libwebp7 libpq5 \
    tesseract-ocr tesseract-ocr-eng curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy Python site-packages from builder
COPY --from=builder /usr/local /usr/local

# Ensure imports like `from src.models import ...` work
ENV PYTHONPATH="/app"

# Copy the entire app
COPY . .

# Prune unnecessary files
RUN rm -rf tests .git .env.example debug.py *.md || true \
    && find . -name "__pycache__" -type d -exec rm -rf {} +

USER appuser
EXPOSE 8000

CMD ["python", "run.py"]
