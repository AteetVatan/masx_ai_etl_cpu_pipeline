"""
Flask server for MASX AI ETL CPU Pipeline.

Provides production-ready REST API endpoints for feed processing
and system monitoring with comprehensive error handling.
"""

import asyncio
import threading
from typing import Dict, Any
from datetime import datetime
from functools import wraps

from flask import Flask, request, jsonify, g
from flask_cors import CORS

from src.config import get_settings, get_api_logger
from src.utils import validate_and_raise, get_today_date
from src.db import DatabaseError

logger = get_api_logger(__name__)
settings = get_settings()

# Create Flask application
app = Flask(__name__)
CORS(app)

# Global variables for services
_pipeline_manager = None
_feed_processor = None
_services_initialized = False


def get_async_loop():
    """Get or create async event loop for the current thread."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Event loop is closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


def async_route(f):
    """Decorator to handle async functions in Flask routes."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        loop = get_async_loop()
        return loop.run_until_complete(f(*args, **kwargs))

    return wrapper


def verify_api_key():
    """
    Verify API key from request headers.

    Returns:
        bool: True if API key is valid

    Raises:
        Exception: If API key is missing or invalid
    """
    # Skip verification if not required
    if not settings.require_api_key:
        return True

    # Get API key from headers
    api_key = request.headers.get("X-API-Key") or request.headers.get("Authorization")

    if not api_key:
        raise Exception(
            "API key required. Please provide X-API-Key or Authorization header"
        )

    # Remove 'Bearer ' prefix if present
    if api_key.startswith("Bearer "):
        api_key = api_key[7:]

    # Verify against configured API key
    if api_key != settings.api_key:
        raise Exception("Invalid API key")

    return True


def get_pipeline_manager():
    """Get pipeline manager instance."""
    global _pipeline_manager
    if _pipeline_manager is None:
        from src.pipeline import pipeline_manager

        _pipeline_manager = pipeline_manager
    return _pipeline_manager


def get_feed_processor():
    """Get feed processor instance."""
    global _feed_processor
    if _feed_processor is None:
        from src.processing import feed_processor

        _feed_processor = feed_processor
    return _feed_processor


def run_background_task(func, *args, **kwargs):
    """Run a function in a background thread."""

    def background_worker():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(func(*args, **kwargs))
        finally:
            loop.close()

    thread = threading.Thread(target=background_worker, daemon=False)
    thread.start()
    return thread


# Error handlers
@app.errorhandler(400)
def bad_request(error):
    """Handle 400 Bad Request errors."""
    return jsonify({"detail": str(error.description)}), 400


@app.errorhandler(401)
def unauthorized(error):
    """Handle 401 Unauthorized errors."""
    return jsonify({"detail": str(error.description)}), 401


@app.errorhandler(404)
def not_found(error):
    """Handle 404 Not Found errors."""
    return jsonify({"detail": str(error.description)}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 Internal Server errors."""
    logger.error(f"Internal server error: {error}")
    return jsonify({"detail": "Internal server error", "type": "internal_error"}), 500


@app.errorhandler(Exception)
def global_exception_handler(error):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception: {error}", exc_info=True)
    return jsonify({"detail": "Internal server error", "type": "internal_error"}), 500


# Middleware for API key verification
@app.before_request
def before_request():
    """Verify API key before processing requests."""
    try:
        verify_api_key()
    except Exception as e:
        return jsonify({"detail": str(e)}), 401


# API Endpoints


@app.route("/", methods=["GET"])
def root():
    """Root endpoint with API information."""
    return jsonify(
        {
            "message": "MASX AI ETL CPU Pipeline API - Flask",
            "version": "1.0.0",
            "endpoints": {
                "feed_process": "/feed/process",
                "feed_process_flashpoint": "/feed/process/flashpoint",
            },
            "status": "operational",
        }
    )


@app.route("/health", methods=["GET"])
@async_route
async def health_check():
    """
    Comprehensive health check endpoint.
    Also generates outbound traffic to keep Railway awake.
    """
    import aiohttp, time
    try:
        pipeline_manager = get_pipeline_manager()
        health_data = await pipeline_manager.health_check()

        # --- outbound ping to ensure Railway activity ---
        ping_status = "skipped"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://1.1.1.1/", timeout=3) as resp:
                    ping_status = f"ok ({resp.status})"
        except Exception as e:
            ping_status = f"failed ({type(e).__name__}: {e})"

        health_data["outbound_ping"] = {
            "status": ping_status,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        return jsonify(health_data), 200

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({"detail": f"Health check failed: {str(e)}"}), 500


@app.route("/stats", methods=["GET"])
@async_route
async def get_stats():
    """
    Get comprehensive pipeline statistics.

    Returns detailed statistics about processing performance,
    thread pool usage, and database operations.
    """
    try:
        pipeline_manager = get_pipeline_manager()
        stats_data = await pipeline_manager.get_pipeline_stats()
        return jsonify(stats_data)
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        return jsonify({"detail": f"Failed to get stats: {str(e)}"}), 500


@app.route("/feed/process", methods=["POST"])
@async_route
async def process_feed_entries():
    """
    Process all feed entries for a specific date.

    This endpoint processes all feed entries from the feed_entries_{date} table
    through the complete pipeline: scrape → clean → geotag → find image → save to DB.
    If no date is provided, uses today's date.

    Request body:
    {
        "date": "2024-01-01",  # Optional, defaults to today
        "flashpoints": ["fp1", "fp2"],  # Optional
        "trigger": "masxai"  # Optional, for background processing
    }
    """
    try:
        # Parse request data
        data = request.get_json() or {}
        date = data.get("date")
        flashpoints = data.get("flashpoints")
        trigger = data.get("trigger")

        feed_processor = get_feed_processor()

        # Use today's date if not provided
        date = date or get_today_date()

        # Validate date format
        try:
            validated_date = validate_and_raise(date, "date")
        except ValueError as e:
            return jsonify({"detail": str(e)}), 400

        # Process feed entries
        feed_processor.set_date(validated_date)

        # Fire-and-forget mode for MASX AI trigger
        if trigger == "masxai":
            run_background_task(
                feed_processor.process_all_feed_entries, batch_mode=True
            )
            logger.info(f"MASX AI background job started for {validated_date}")
            return jsonify(
                {
                    "status": "started",
                    "message": f"MASX AI background processing initiated for {validated_date}",
                    "date": validated_date,
                    "total_entries": 0,
                    "successful": 0,
                    "failed": 0,
                    "processing_time": 0,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

        result = await feed_processor.process_all_feed_entries(batch_mode=True)
        return jsonify(result)

    except DatabaseError as e:
        logger.error(f"Database error during processing: {e}")
        return jsonify({"detail": f"Table feed_entries_{date} not available"}), 404
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}")
        return jsonify({"detail": f"Processing failed: {str(e)}"}), 500


@app.route("/feed/process/flashpoint", methods=["POST"])
@async_route
async def process_feed_entries_by_flashpoint():
    """
    Process feed entries for a specific date and flashpoint ID.

    This endpoint processes feed entries from the feed_entries_{date} table
    filtered by flashpoint_id through the complete pipeline.
    If no date is provided, uses today's date.

    Request body:
    {
        "date": "2024-01-01",  # Optional, defaults to today
        "flashpoint_id": "fp123",  # Required
        "trigger": "masxai"  # Optional, for background processing
    }
    """
    try:
        # Parse request data
        data = request.get_json() or {}
        date = data.get("date")
        flashpoint_id = data.get("flashpoint_id")
        trigger = data.get("trigger")

        feed_processor = get_feed_processor()

        # Use today's date if not provided
        date = date or get_today_date()

        # Validate date format
        try:
            validated_date = validate_and_raise(date, "date")
        except ValueError as e:
            return jsonify({"detail": str(e)}), 400

        # Validate flashpoint_id
        if not flashpoint_id:
            return jsonify({"detail": "flashpoint_id is required"}), 400

        # Process feed entries by flashpoint ID
        feed_processor.set_date(validated_date)

        # Fire-and-forget mode for MASX AI trigger
        if trigger == "masxai":
            run_background_task(
                feed_processor.process_feed_entries_by_flashpoint_id, flashpoint_id
            )
            logger.info(
                f"MASX AI background job started for {validated_date} and flashpoint_id {flashpoint_id}"
            )
            return jsonify(
                {
                    "status": "started",
                    "message": f"MASX AI background processing initiated for {validated_date} and flashpoint_id {flashpoint_id}",
                    "date": validated_date,
                    "flashpoint_id": flashpoint_id,
                    "total_entries": 0,
                    "successful": 0,
                    "failed": 0,
                    "processing_time": 0,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

        result = await feed_processor.process_feed_entries_by_flashpoint_id(
            flashpoint_id
        )
        return jsonify(result)

    except DatabaseError as e:
        logger.error(f"Database error during processing: {e}")
        return jsonify({"detail": f"Table feed_entries_{date} not available"}), 404
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}")
        return jsonify({"detail": f"Processing failed: {str(e)}"}), 500


@app.route("/feed/process/batch_articles", methods=["POST"])
@async_route
async def process_batch_articles():
    """
    Process feed entries for a specific date and flashpoint ID.

    This endpoint processes feed entries from the feed_entries_{date} table
    filtered by flashpoint_id through the complete pipeline.
    If no date is provided, uses today's date.

    Request body:
    {
        "date": "2025-07-02",  # Optional, defaults to today
        "articles_ids": ["0015b088-3fdf-4f0f-bfa5-27dff8e1d3e7","00170f44-067b-47be-b198-7d92ece35727"],  # Required
        "trigger": "masxai"  # Optional, for background processing
    }
    """
    try:
        # Parse request data , get date and articles_ids
        data = request.get_json() or {}
        date = data.get("date", "")

        articles_ids = data.get("articles_ids", [])

        trigger = data.get("trigger")
        logger.info(
            f"Processing feed entries for date: {date}, length of articles: {len(articles_ids)}"
        )

        feed_processor = get_feed_processor()

        # Validate date
        if not date:
            return jsonify({"detail": "date is required"}), 400

        if len(articles_ids) == 0:
            return jsonify({"detail": "articles are required"}), 400

        # Validate date format
        try:
            validated_date = validate_and_raise(date, "date")
        except ValueError as e:
            return jsonify({"detail": str(e)}), 400

        # Process feed entries by articles IDs
        feed_processor.set_date(validated_date)

        # Fire-and-forget mode for MASX AI trigger
        if trigger == "masxai":
            run_background_task(feed_processor.process_articles_batch, articles_ids)
            logger.info(
                f"MASX AI background job started for {validated_date} and articles {articles_ids}"
            )
            return jsonify(
                {
                    "status": "started",
                    "message": f"MASX AI background processing initiated for {validated_date}",
                    "date": validated_date,
                    "articles_ids": articles_ids,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

        result = await feed_processor.process_articles_batch(articles_ids)
        return jsonify(result)

    except DatabaseError as e:
        logger.error(f"Database error during processing: {e}")
        return jsonify({"detail": f"Table feed_entries_{date} not available"}), 404
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}")
        return jsonify({"detail": f"Processing failed: {str(e)}"}), 500


@app.route("/feed/process/article", methods=["POST"])
@async_route
async def process_feed_entries_by_article_id():
    """
    Process feed entries for a specific date and flashpoint ID.

    This endpoint processes feed entries from the feed_entries_{date} table
    filtered by flashpoint_id through the complete pipeline.
    If no date is provided, uses today's date.

    Request body:
    {
        "date": "2025-07-01",  # Optional, defaults to today
        "flashpoint_id": "70ef3f5a-3dbd-4b9a-8eb5-1b971a37fbc0",  # Required
        "article_id": "3e0d011b-7ec4-4d16-9c18-b4169057faf8",  # Required
        "trigger": "masxai"  # Optional, for background processing
    }
    """
    try:
        # Parse request data
        data = request.get_json() or {}
        date = data.get("date")
        flashpoint_id = data.get("flashpoint_id")
        article_id = data.get("article_id")
        trigger = data.get("trigger")
        logger.info(
            f"Processing feed entries for date: {date}, flashpoint_id: {flashpoint_id}, article_id: {article_id}"
        )

        feed_processor = get_feed_processor()

        # Validate date
        if not date:
            return jsonify({"detail": "date is required"}), 400

        # Validate date format
        try:
            validated_date = validate_and_raise(date, "date")
        except ValueError as e:
            return jsonify({"detail": str(e)}), 400

        # Validate flashpoint_id
        if not flashpoint_id:
            return jsonify({"detail": "flashpoint_id is required"}), 400

        # Validate article_id
        if not article_id:
            return jsonify({"detail": "article_id is required"}), 400

        # Process feed entries by flashpoint ID
        feed_processor.set_date(validated_date)

        result = await feed_processor.process_by_article_id(flashpoint_id, article_id)
        return jsonify(result)

    except DatabaseError as e:
        logger.error(f"Database error during processing: {e}")
        return jsonify({"detail": f"Table feed_entries_{date} not available"}), 404
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}")
        return jsonify({"detail": f"Processing failed: {str(e)}"}), 500


@app.route("/ready", methods=["GET"])
def readiness_check():
    """Instant readiness endpoint for health probes."""
    return jsonify({"status": "ready", "uptime": "OK"})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
