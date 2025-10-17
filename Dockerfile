# ================================================================
# MASX AI ETL CPU Pipeline - Production Dockerfile
# ================================================================
FROM python:3.12-slim

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# ----------------------------------------------------------------
# Install required OS packages (for spaCy, psycopg2, etc.)
# ----------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ pkg-config \
    libpq-dev libffi-dev libssl-dev libxml2-dev libxslt1-dev libicu-dev \
    && rm -rf /var/lib/apt/lists/*

# ----------------------------------------------------------------
# Set working directory
# ----------------------------------------------------------------
WORKDIR /app

# ----------------------------------------------------------------
# Install dependencies (CPU-only Playwright)
# ----------------------------------------------------------------
COPY requirements-prod.txt .

# Persistent browser path for all users
ENV PLAYWRIGHT_BROWSERS_PATH=/usr/lib/playwright \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=0

RUN python -m pip install --upgrade pip==24.2 && \
    pip install --no-cache-dir -r requirements-prod.txt && \
    python -m spacy download xx_ent_wiki_sm --direct --no-cache && \
    python -m spacy validate && \
    python -m playwright install --with-deps chromium && \
    chmod -R 777 /usr/lib/playwright && \
    rm -rf /root/.cache/pip /root/.cache/ms-playwright

# ----------------------------------------------------------------
# Copy source code and install as editable package
# ----------------------------------------------------------------
COPY . .

# If pyproject.toml is present, install src as a proper package
RUN if [ -f "pyproject.toml" ]; then pip install -e .; fi

# ----------------------------------------------------------------
# Environment configuration for Railway & FastAPI
# ----------------------------------------------------------------
# Railway dynamically sets $PORT, so let FastAPI bind to it
ENV PORT=8080
EXPOSE 8080

# ----------------------------------------------------------------
# Default startup command
# ----------------------------------------------------------------
CMD ["python", "run.py"]
