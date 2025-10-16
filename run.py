#!/usr/bin/env python3
"""
Run script for MASX AI ETL CPU Pipeline.

Provides a convenient entry point to start the FastAPI server with
proper configuration and error handling.
"""

import os
import sys
import asyncio
from src.config import get_service_logger, get_settings
from pathlib import Path

# # Add src directory to Python path
# src_path = Path(__file__).parent / "src"
# sys.path.insert(0, str(src_path))

import uvicorn

settings = get_settings()


def check_environment():
    """Check if required environment variables are set."""
    required_vars = [
        "SUPABASE_URL",
        "SUPABASE_KEY",
        "SUPABASE_SERVICE_KEY",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
    ]

    missing_vars = []
    for var in required_vars:
        if not getattr(settings, var.lower(), None):
            missing_vars.append(var)

    if missing_vars:
        print("Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease check your .env file or environment variables.")
        print("You can copy env.example to .env and configure it.")
        return False

    return True


def print_startup_info():
    """Print startup information."""
    print("MASX AI ETL CPU Pipeline")
    print("=" * 50)
    print(f"Version: 1.0.0")
    print(f"Host: {settings.host}")
    print(f"Port: {settings.port}")
    print(f"Debug: {settings.debug}")
    print(f"Log Level: {settings.log_level}")
    print(f"Max Workers: {settings.max_workers}")
    print(f"Batch Size: {settings.batch_size}")
    print(f"Image Search: {settings.enable_image_search}")
    print(f"Geotagging: {settings.enable_geotagging}")
    print(f"Text Cleaning: {settings.clean_text}")
    print("=" * 50)
    print(f"API Documentation: http://{settings.host}:{settings.port}/docs")
    print(f"Health Check: http://{settings.host}:{settings.port}/health")
    print(f"Statistics: http://{settings.host}:{settings.port}/stats")
    print("=" * 50)


def main():
    """Main entry point for the application."""
    try:
        # Setup logging

        logger = get_service_logger(__name__)
        print_startup_info()

        app_path = "src.api.server:app" if settings.debug else "src.api.server:app"

        # Configure uvicorn
        uvicorn_config = {
            "app": app_path,
            "host": settings.host,
            "port": settings.port,
            "log_level": settings.log_level.lower(),
            "access_log": True,
            "reload": settings.debug,
            "reload_dirs": ["src"] if settings.debug else None,
            "workers": 1,  # Single worker for development, use multiple for production
        }

        # Start the server
        logger.info("Starting MASX AI ETL CPU Pipeline server...")
        uvicorn.run(**uvicorn_config)

    except KeyboardInterrupt:
        print("\n Server stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f" Error starting server: {e}")
        logger.error(f"Failed to start server: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
