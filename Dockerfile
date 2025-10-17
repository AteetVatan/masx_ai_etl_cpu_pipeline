# ================================================================
# MASX AI ETL CPU Pipeline - Minimal Production Dockerfile
# ================================================================

FROM python:3.12-slim

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# Install required OS packages for dependencies (spacy, psycopg2, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ pkg-config \
    libpq-dev libffi-dev libssl-dev libxml2-dev libxslt1-dev libicu-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency file and install dependencies
COPY requirements-prod.txt .
RUN python -m pip install --upgrade pip==24.2 && \
    pip install --no-cache-dir -r requirements-prod.txt && \
    python -m spacy download xx_ent_wiki_sm --direct --no-cache && \
    python -m spacy validate

# Copy all project files
COPY . .
RUN pip install -e .

# Expose FastAPI port
EXPOSE 8000

# Default command to run the FastAPI app through run.py
CMD ["python", "run.py"]
