# Makefile for MASX AI ETL CPU Pipeline
# Provides convenient commands for development, testing, and deployment

.PHONY: help install dev test lint format clean build run docker-build docker-run docker-compose-up docker-compose-down

# Default target
help:
	@echo "MASX AI ETL CPU Pipeline - Available Commands:"
	@echo ""
	@echo "Development:"
	@echo "  install          Install dependencies"
	@echo "  dev              Run development server (using run.py)"
	@echo "  dev-direct       Run development server (direct uvicorn)"
	@echo "  run              Run application (same as dev)"
	@echo "  test             Run tests"
	@echo "  test-coverage    Run tests with coverage"
	@echo "  lint             Run linting"
	@echo "  format           Format code"
	@echo "  clean            Clean temporary files"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build     Build Docker image"
	@echo "  docker-run       Run Docker container"
	@echo "  docker-compose-up    Start all services with Docker Compose"
	@echo "  docker-compose-down  Stop all services"
	@echo ""
	@echo "Database:"
	@echo "  db-init          Initialize database"
	@echo "  db-migrate       Run database migrations"
	@echo ""
	@echo "Monitoring:"
	@echo "  logs             View application logs"
	@echo "  health           Check application health"
	@echo "  stats            View application statistics"

# Development commands
install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt
	python -m spacy download en_core_web_sm
	python -m spacy download es_core_news_sm
	python -m spacy download fr_core_news_sm
	python -m spacy download de_core_news_sm
	@echo "Installation complete!"

dev:
	@echo "Starting development server..."
	python run.py

dev-direct:
	@echo "Starting development server directly with uvicorn..."
	python -m uvicorn src.api.server:app --reload --host 0.0.0.0 --port 8000

run: dev

test:
	@echo "Running tests..."
	pytest tests/ -v

test-coverage:
	@echo "Running tests with coverage..."
	pytest tests/ --cov=src --cov-report=html --cov-report=term

lint:
	@echo "Running linting..."
	flake8 src/ tests/
	black --check src/ tests/
	isort --check-only src/ tests/

format:
	@echo "Formatting code..."
	black src/ tests/
	isort src/ tests/

clean:
	@echo "Cleaning temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf dist/
	rm -rf build/

# Docker commands
docker-build:
	@echo "Building Docker image..."
	docker build -t masx-etl-pipeline:latest .

docker-run:
	@echo "Running Docker container..."
	docker run -d \
		--name masx-etl-pipeline \
		-p 8000:8000 \
		--env-file .env \
		--restart unless-stopped \
		masx-etl-pipeline:latest

docker-compose-up:
	@echo "Starting all services with Docker Compose..."
	docker-compose up -d

docker-compose-down:
	@echo "Stopping all services..."
	docker-compose down

docker-compose-logs:
	@echo "Viewing Docker Compose logs..."
	docker-compose logs -f

# Database commands
db-init:
	@echo "Initializing database..."
	@echo "Please run the init.sql script in your Supabase SQL editor"

db-migrate:
	@echo "Running database migrations..."
	@echo "Database migrations are handled by Supabase"

# Monitoring commands
logs:
	@echo "Viewing application logs..."
	docker-compose logs -f masx-etl-pipeline

health:
	@echo "Checking application health..."
	curl -f http://localhost:8000/health || echo "Health check failed"

stats:
	@echo "Viewing application statistics..."
	curl -s http://localhost:8000/stats | jq .

# Production commands
prod-build:
	@echo "Building production image..."
	docker build -t masx-etl-pipeline:prod --target production .

prod-run:
	@echo "Running production container..."
	docker run -d \
		--name masx-etl-pipeline-prod \
		-p 8000:8000 \
		--env-file .env.production \
		--restart unless-stopped \
		masx-etl-pipeline:prod

# Utility commands
check-env:
	@echo "Checking environment variables..."
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Please copy env.example to .env and configure it."; \
		exit 1; \
	fi
	@echo "Environment file found."

setup-dev:
	@echo "Setting up development environment..."
	make check-env
	make install
	@echo "Development environment setup complete!"

# CI/CD commands
ci-test:
	@echo "Running CI tests..."
	pytest tests/ --cov=src --cov-report=xml --cov-report=term

ci-lint:
	@echo "Running CI linting..."
	flake8 src/ tests/
	black --check src/ tests/
	isort --check-only src/ tests/

ci-build:
	@echo "Running CI build..."
	docker build -t masx-etl-pipeline:ci .

# Security commands
security-scan:
	@echo "Running security scan..."
	safety check
	bandit -r src/

# Performance commands
benchmark:
	@echo "Running performance benchmarks..."
	python -m pytest tests/test_performance.py -v

load-test:
	@echo "Running load tests..."
	@echo "Please install locust first: pip install locust"
	locust -f tests/load_test.py --host=http://localhost:8000

# Documentation commands
docs:
	@echo "Generating documentation..."
	@echo "Documentation is available in README.md"

# Backup commands
backup-db:
	@echo "Backing up database..."
	@echo "Please use Supabase dashboard for database backups"

# Update commands
update-deps:
	@echo "Updating dependencies..."
	pip install --upgrade -r requirements.txt

update-models:
	@echo "Updating spaCy models..."
	python -m spacy download en_core_web_sm
	python -m spacy download es_core_news_sm
	python -m spacy download fr_core_news_sm
	python -m spacy download de_core_news_sm

# All-in-one commands
full-setup: check-env install
	@echo "Full setup complete!"

full-test: lint test
	@echo "Full test suite complete!"

full-clean: clean docker-compose-down
	@echo "Full cleanup complete!"

# Default target
.DEFAULT_GOAL := help
