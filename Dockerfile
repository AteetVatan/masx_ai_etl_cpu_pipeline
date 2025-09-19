# Multi-stage Dockerfile for MASX AI ETL CPU Pipeline
# Optimized for production deployment with minimal image size

# Stage 1: Build stage
FROM python:3.12-slim as builder

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libjpeg-dev \
    libpng-dev \
    libwebp-dev \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Stage 2: Production stage
FROM python:3.12-slim as production

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app/src"

# Install runtime system dependencies
RUN apt-get update && apt-get install -y \
    libxml2 \
    libxslt1.1 \
    libjpeg62-turbo \
    libpng16-16 \
    libwebp7 \
    libffi8 \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create non-root user for security
RUN groupadd -r masx && useradd -r -g masx masx

# Create app directory
WORKDIR /app

# Copy application code
COPY src/ ./src/
COPY requirements.txt .
COPY env.example .env.example

# Create necessary directories
RUN mkdir -p /app/logs /app/data && \
    chown -R masx:masx /app

# Switch to non-root user
USER masx

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command
CMD ["python", "-m", "uvicorn", "src.api.server:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]

# Labels for metadata
LABEL maintainer="MASX AI Team" \
      version="1.0.0" \
      description="High-performance CPU-only news enrichment pipeline with FastAPI service layer" \
      org.opencontainers.image.title="MASX AI ETL CPU Pipeline" \
      org.opencontainers.image.description="A high-performance CPU-only news enrichment pipeline with FastAPI service layer" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.authors="MASX AI Team" \
      org.opencontainers.image.url="https://github.com/masx-ai/etl-cpu-pipeline" \
      org.opencontainers.image.documentation="https://github.com/masx-ai/etl-cpu-pipeline/blob/main/README.md" \
      org.opencontainers.image.source="https://github.com/masx-ai/etl-cpu-pipeline" \
      org.opencontainers.image.licenses="MIT"
