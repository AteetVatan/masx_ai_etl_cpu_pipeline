#!/usr/bin/env python3
"""
Run script for MASX AI ETL CPU Pipeline.

Provides a convenient entry point to start the FastAPI server with
proper configuration and error handling.
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from src.config.settings import settings
from src.api.server import app
import uvicorn


def setup_logging():
    """Configure logging for the application."""
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    
    if settings.log_format.lower() == "json":
        # JSON logging for production
        import json
        import time
        
        class JSONFormatter(logging.Formatter):
            def format(self, record):
                log_entry = {
                    "timestamp": time.time(),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                    "module": record.module,
                    "function": record.funcName,
                    "line": record.lineno
                }
                
                if record.exc_info:
                    log_entry["exception"] = self.formatException(record.exc_info)
                
                return json.dumps(log_entry)
        
        formatter = JSONFormatter()
    else:
        # Text logging for development
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=formatter,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Set specific loggers
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    logging.getLogger("fastapi").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def check_environment():
    """Check if required environment variables are set."""
    required_vars = [
        "SUPABASE_URL",
        "SUPABASE_KEY",
        "SUPABASE_SERVICE_KEY",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not getattr(settings, var.lower(), None):
            missing_vars.append(var)
    
    if missing_vars:
        print("❌ Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease check your .env file or environment variables.")
        print("You can copy env.example to .env and configure it.")
        return False
    
    return True


def print_startup_info():
    """Print startup information."""
    print("🚀 MASX AI ETL CPU Pipeline")
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
    print(f"📖 API Documentation: http://{settings.host}:{settings.port}/docs")
    print(f"🔍 Health Check: http://{settings.host}:{settings.port}/health")
    print(f"📊 Statistics: http://{settings.host}:{settings.port}/stats")
    print("=" * 50)


def main():
    """Main entry point for the application."""
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)
        
        # Check environment
        if not check_environment():
            sys.exit(1)
        
        # Print startup information
        print_startup_info()
        
        # Configure uvicorn
        uvicorn_config = {
            "app": "src.api.server:app",
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
        print("\n🛑 Server stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        logging.error(f"Failed to start server: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
