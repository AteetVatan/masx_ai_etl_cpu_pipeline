# ================================================================
# MASX AI ETL CPU Pipeline - Production Dockerfile (robust for Playwright)
# ================================================================
FROM python:3.12-slim

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# ----------------------------------------------------------------
# Install required OS packages (for spaCy, Playwright dependencies, etc.)
# ----------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ pkg-config \
    libpq-dev libffi-dev libssl-dev libxml2-dev libxslt1-dev libicu-dev \
    wget ca-certificates gnupg \
    # Dependencies needed for Playwright / Chromium to run
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libxss1 libasound2 libxcomposite1 libxdamage1 libxrandr2 libgbm-dev \
    libgtk-3-0 libpango-1.0-0 libpangocairo-1.0-0 \
    && rm -rf /var/lib/apt/lists/*

# ----------------------------------------------------------------
# Set working directory
# ----------------------------------------------------------------
WORKDIR /app

# ----------------------------------------------------------------
# Copy requirements
# ----------------------------------------------------------------
COPY requirements-prod.txt .

# Persistent browser path across runtime users
ENV PLAYWRIGHT_BROWSERS_PATH=/usr/lib/playwright \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=0 \
    # Sometimes helps prevent “fork” issues if HOME is used by Playwright
    HOME=/usr/lib/playwright

# ----------------------------------------------------------------
# Install dependencies and browsers
# ----------------------------------------------------------------
RUN python -m pip install --upgrade pip==24.2 && \
    pip install --no-cache-dir -r requirements-prod.txt && \
    python -m spacy download xx_ent_wiki_sm --direct --no-cache && \
    python -m spacy validate && \
    python -m playwright install --with-deps chromium && \
    chmod -R a+rx /usr/lib/playwright && \
    rm -rf /root/.cache/pip /root/.cache/ms-playwright

# ----------------------------------------------------------------
# Copy source code
# ----------------------------------------------------------------
COPY . .

# If pyproject.toml is present, install as package to make imports (src.*) work
RUN if [ -f "pyproject.toml" ]; then pip install -e .; fi

# ----------------------------------------------------------------
# Environment configuration for Railway / FastAPI
# ----------------------------------------------------------------
ENV PORT=8080
EXPOSE 8080

# ----------------------------------------------------------------
# Default startup command
# ----------------------------------------------------------------
CMD ["python", "run_flask.py"]
