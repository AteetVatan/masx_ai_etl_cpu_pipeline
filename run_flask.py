#!/usr/bin/env python3
"""
Run script for MASX AI ETL CPU Pipeline Flask Server.

Provides a convenient entry point to start the Flask server with
proper configuration, retries, and graceful failure handling.
"""

import os
import sys
import asyncio
from pathlib import Path
from flask import Flask
import threading

from src.config import get_service_logger, get_settings

settings = get_settings()


def print_startup_info():
    """Print startup configuration for visibility."""
    print("MASX AI ETL CPU Pipeline - Flask Server")
    print("=" * 50)
    print(f"Version: 1.0.0")
    print(f"Host: {settings.host}")
    print(f"Port: {settings.port}")
    print(f"Debug: {settings.debug}")
    print(f"Log Level: {settings.log_level}")
    print(f"Max Workers: {settings.max_workers}")
    print(f"DB Batch Size: {settings.db_batch_size}")
    print(f"Image Search: {settings.enable_image_search}")
    print(f"Geotagging: {settings.enable_geotagging}")
    print(f"Text Cleaning: {settings.clean_text}")
    print("=" * 50)
    print(f"API Endpoints:")
    print(f"  POST /feed/process")
    print(f"  POST /feed/process/flashpoint")
    print("=" * 50)


async def initialize_services():
    """Initialize database and pipeline services."""
    from src.db import db_connection
    from src.pipeline import pipeline_manager
    
    try:
        # Initialize database connection
        await db_connection.connect()
        print("Database connection established")
        
        # Initialize pipeline manager
        await pipeline_manager.health_check()
        print("Pipeline manager initialized")
        
    except Exception as e:
        print(f"Failed to initialize services: {e}")
        raise


def main():
    """Main entry point for the MASX AI ETL CPU Pipeline Flask server."""
    logger = get_service_logger(__name__)
    print_startup_info()

    try:
        # Initialize services in async context
        asyncio.run(initialize_services())
        
        # Import and run Flask app
        from src.api_flask.server_flask import app
        
        logger.info("Starting MASX AI ETL CPU Pipeline Flask server...")
        
        # Run Flask app
        app.run(
            host=settings.host,
            port=settings.port,
            debug=settings.debug,
            threaded=True
        )
        
    except KeyboardInterrupt:
        print("\nServer stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to start server: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
