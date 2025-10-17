#!/usr/bin/env python3
"""
Run script for MASX AI ETL CPU Pipeline.

Provides a convenient entry point to start the FastAPI server with
proper configuration, retries, and graceful failure handling.
"""

import os
import sys
import asyncio
from pathlib import Path
import uvicorn

from src.config import get_service_logger, get_settings

settings = get_settings()


def print_startup_info():
    """Print startup configuration for visibility."""
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
    """Main entry point for the MASX AI ETL CPU Pipeline."""
    logger = get_service_logger(__name__)
    print_startup_info()

    app_path = "src.api.server:app"
    uvicorn_config = {
        "app": app_path,
        "host": settings.host,
        "port": settings.port,
        "log_level": settings.log_level.lower(),
        "access_log": True,
        "reload": settings.debug,
        "reload_dirs": ["src"] if settings.debug else None,
        "workers": 1,
    }

    try:

        #sys.path.append(os.path.join(os.path.dirname(__file__), "src"))
        logger.info("Starting MASX AI ETL CPU Pipeline FastAPI server...")        
        uvicorn.run(**uvicorn_config)
    except KeyboardInterrupt:
        print("\n🧹 Server stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to start server: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
