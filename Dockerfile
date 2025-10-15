# =============================================================================
# MASX AI ETL CPU Pipeline - Production Docker Image
# =============================================================================
# Optimized production-ready Docker image
# =============================================================================

FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    # Essential build tools
    build-essential \
    gcc \
    g++ \
    # System libraries for dependencies
    libicu-dev \
    libxml2-dev \
    libxslt1-dev \
    libffi-dev \
    libssl-dev \
    # Image processing libraries
    libjpeg-dev \
    libpng-dev \
    libtiff-dev \
    libwebp-dev \
    # OCR dependencies
    tesseract-ocr \
    tesseract-ocr-eng \
    # PostgreSQL client libraries
    libpq-dev \
    # curl for health checks
    curl \
    # Cleanup
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install spaCy model
RUN python -m spacy download xx_ent_wiki_sm

# Copy source code
COPY . .

# Remove development files and unnecessary components
RUN rm -rf \
    tests/ \
    .git/ \
    .gitignore \
    README.md \
    LICENSE \
    debug.py \
    *.md \
    .env.example \
    third_party/ \
    && find . -name "*.pyc" -delete \
    && find . -name "__pycache__" -delete

# Change ownership to appuser
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Production command
CMD ["python", "run.py"]
