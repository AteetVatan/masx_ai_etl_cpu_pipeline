# =============================================================================
# MASX AI ETL CPU Pipeline - Production Docker Image
# =============================================================================
# Optimized, secure, and minimal production build
# =============================================================================

FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ pkg-config \
    libicu-dev libxml2-dev libxslt1-dev libffi-dev libssl-dev \
    libjpeg-dev libpng-dev libtiff-dev libwebp-dev \
    libpq-dev tesseract-ocr tesseract-ocr-eng curl \
    && rm -rf /var/lib/apt/lists/*

# Set workdir and copy requirements
WORKDIR /app
COPY requirements.txt .

# Install dependencies in /root/.local for smaller runtime copy
RUN pip install --no-cache-dir --user -r requirements.txt && \
    python -m spacy download xx_ent_wiki_sm

# -----------------------------------------------------------------------------
# Stage 2: Runtime image (smaller, no build tools)
# -----------------------------------------------------------------------------
FROM python:3.12-slim

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser
WORKDIR /app

# Minimal runtime deps only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libicu-dev libxml2 libxslt1.1 libffi8 libssl3 \
    libjpeg-turbo-progs libpng16-16 libtiff6 libwebp7 libpq5 \
    tesseract-ocr tesseract-ocr-eng curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Copy application source
COPY . .

# Cleanup development files
RUN rm -rf tests .git .env.example debug.py *.md LICENSE \
    && find . -name "__pycache__" -exec rm -rf {} +

USER appuser
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "run.py"]
