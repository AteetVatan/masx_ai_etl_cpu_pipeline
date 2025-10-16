#!/usr/bin/env python3
"""
Debug script for MASX AI ETL CPU Pipeline Feed Processing.

This script provides a simple interface to test the feed processing functionality
without needing to make HTTP requests to the FastAPI server.

Usage:
    python debug.py --help
    python debug.py --warmup --date 20250702
    python debug.py --process --date 20250702
    python debug.py --process-flashpoint --date 20250702 --flashpoint-id 123e4567-e89b-12d3-a456-426614174000
    python debug.py --stats
    python debug.py --entries --date 20250702
"""

import asyncio
import sys
from datetime import datetime
from typing import Optional

# Add src to path so we can import our modules
# sys.path.insert(0, 'src')


from src.config import settings
from src.db import db_connection
from src.processing import feed_processor
from src.pipeline import pipeline_manager
from src.utils import validate_date_format, get_today_date
from src.config import get_service_logger

# Configure logging

logger = get_service_logger(__name__)


# Date validation is now imported from utils.date_validation


async def warmup_server(date: str) -> None:
    """Warm up the server by loading feed entries for a specific date."""
    logger.info(f"Warming up server with feed entries for date: {date}")

    try:
        feed_processor.set_date(date)
        result = await feed_processor.warm_up_server()
        logger.info(f"Warm-up successful: {result}")
    except Exception as e:
        logger.error(f"Warm-up failed: {e}")
        raise


async def process_feed_entries_by_date(date: str, batch_mode: bool = False) -> None:
    """Process all feed entries for a specific date."""
    logger.info(f"Processing feed entries for date: {date}")

    try:
        feed_processor.set_date(date)
        result = await feed_processor.process_all_feed_entries(batch_mode)
        logger.info(f"Processing completed: {result}")
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise


async def process_feed_entries_by_flashpoint_id(date: str, flashpoint_id: str) -> None:
    """Process feed entries for a specific date and flashpoint ID."""
    logger.info(
        f"Processing feed entries for date: {date}, flashpoint_id: {flashpoint_id}"
    )

    try:
        feed_processor.set_date(date)
        result = await feed_processor.process_feed_entries_by_flashpoint_id(
            flashpoint_id
        )
        logger.info(f"Processing completed: {result}")
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise


async def get_feed_stats() -> None:
    """Get feed processing statistics."""
    logger.info("Getting feed processing statistics...")

    try:
        stats = feed_processor.get_processing_stats()
        logger.info(f"Feed processing stats: {stats}")
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise


async def get_feed_entries(date: str) -> None:
    """Get loaded feed entries for a specific date."""
    logger.info(f"Getting feed entries for date: {date}")

    try:
        feed_processor.set_date(date)
        entries = feed_processor.get_feed_entries()
        logger.info(f"Found {len(entries)} entries for date {date}")
        if entries:
            logger.info(f"Sample entry: {entries[0]}")
    except Exception as e:
        logger.error(f"Failed to get entries: {e}")
        raise


async def clear_feed_entries(date: Optional[str] = None) -> None:
    """Clear feed entries from memory."""
    if date:
        logger.info(f"Clearing feed entries for date: {date}")
        feed_processor.clear_feed_entries(date)
        logger.info(f"Cleared feed entries for date {date}")
    else:
        logger.info("Clearing all feed entries from memory")
        feed_processor.clear_feed_entries()
        logger.info("Cleared all feed entries from memory")


async def main(date: Optional[str] = None, flashpoint_id: Optional[str] = None):
    """Main function to handle command line arguments and execute operations."""

    # Validate date format if provided
    if date and not validate_date_format(date):
        logger.error(
            f"Invalid date format: {date}. Must be in YYYY-MM-DD format (e.g., 2025-07-02)"
        )
        sys.exit(1)

    # Use today's date if not provided and required
    if not date:
        date = get_today_date()
        logger.info(f"Using today's date: {date}")

    try:
        # Initialize database connection
        logger.info("Initializing database connection...")
        db_connection.date = date
        await db_connection.connect()

        # Initialize pipeline manager
        logger.info("Initializing pipeline manager...")
        await pipeline_manager.health_check()

        # test warmup
        await warmup_server(date)

        # test process
        await process_feed_entries_by_date(date, batch_mode=False)

        # test process-flashpoint
        await process_feed_entries_by_flashpoint_id(date, flashpoint_id)

        # test stats
        await get_feed_stats()

        # test entries
        await get_feed_entries(date)

        # test clear
        await clear_feed_entries(date)

        # test clear-all

        await clear_feed_entries(date)

        await clear_feed_entries()

        logger.info("Operation completed successfully!")

    except Exception as e:
        logger.error(f"Operation failed: {e}")
        sys.exit(1)

    finally:
        # Cleanup
        logger.info("Cleaning up...")
        await pipeline_manager.shutdown()
        await db_connection.disconnect()
        logger.info("Cleanup completed")


if __name__ == "__main__":
    # en-core-web-sm
    # fr-core-news-sm

    # date = "2025-07-01"
    # flashpoints_ids = ["70ef3f5a-3dbd-4b9a-8eb5-1b971a37fbc0"]

    # date = "2025-07-02"
    # flashpoints_ids = ["d8547bcf-4cc9-4168-bb36-fccd517fbef9"]

    date = "2025-07-01"
    flashpoint_id = "70ef3f5a-3dbd-4b9a-8eb5-1b971a37fbc0"

    asyncio.run(main(date=date, flashpoint_id=flashpoint_id))
