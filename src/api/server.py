"""
FastAPI server for MASX AI ETL CPU Pipeline.

Provides production-ready REST API endpoints for feed processing
and system monitoring with comprehensive error handling.
"""


from typing import Dict, Any
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel


from src.config import get_settings, get_api_logger
from src.utils import validate_and_raise, get_today_date

from src.db import DatabaseClientAndPool, DatabaseError


logger = get_api_logger(__name__)
settings = get_settings()


# Pydantic models for API requests/responses


class HealthResponse(BaseModel):
    """Response model for health check."""

    overall: str
    components: Dict[str, Any]
    timestamp: str


class StatsResponse(BaseModel):
    """Response model for pipeline statistics."""

    pipeline_stats: Dict[str, Any]
    thread_pool_stats: Dict[str, Any]
    database_stats: Dict[str, Any]
    uptime: float


class FeedWarmupRequest(BaseModel):
    date: str | None = None


class FeedWarmupResponse(BaseModel):
    """Response model for feed warm-up."""

    status: str
    date: str
    total_entries: int
    message: str
    timestamp: str


class FeedProcessRequest(BaseModel):
    date: str | None = None
    flashpoints: list[str] | None = None
    trigger: str | None = None


class FeedProcessResponse(BaseModel):
    """Response model for feed processing."""

    status: str
    message: str
    date: str
    total_entries: int
    successful: int
    failed: int
    processing_time: float
    timestamp: str


class FeedProcessFlashpointRequest(BaseModel):
    date: str | None = None
    flashpoint_id: str | None = None
    trigger: str | None = None


class FeedProcessFlashpointResponse(BaseModel):
    """Response model for feed processing by flashpoint ID."""

    status: str
    date: str
    flashpoint_id: str
    total_entries: int
    successful: int
    failed: int
    processing_time: float
    message: str
    timestamp: str


# Application lifespan management
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown."""
    logger.info("Starting MASX AI ETL CPU Pipeline server")
    from src.db import db_connection, DatabaseClientAndPool, DatabaseError
    from src.pipeline import pipeline_manager
    from src.processing import feed_processor

    try:
        # Initialize database connection
        await db_connection.connect()
        logger.info("Database connection established")

        # Initialize pipeline manager
        await pipeline_manager.health_check()
        logger.info("Pipeline manager initialized")

        yield

    finally:
        # Shutdown
        logger.info("Shutting down MASX AI ETL CPU Pipeline server")

        # Shutdown pipeline manager
        await pipeline_manager.shutdown()

        # Disconnect from database
        await db_connection.disconnect()

        logger.info("Server shutdown completed")


async def verify_api_key(request: Request):
    """
    Verify API key from request headers.

    Args:
        request: FastAPI request object

    Returns:
        bool: True if API key is valid

    Raises:
        HTTPException: If API key is missing or invalid
    """
    settings = get_settings()

    # Skip verification if not required
    if not settings.require_api_key:
        return True

    # Get API key from headers
    api_key = request.headers.get("X-API-Key") or request.headers.get("Authorization")

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="API key required. Please provide X-API-Key or Authorization header",
        )

    # Remove 'Bearer ' prefix if present
    if api_key.startswith("Bearer "):
        api_key = api_key[7:]

    # Verify against configured API key
    if api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return True


# Create FastAPI application
app = FastAPI(
    title="MASX AI ETL CPU Pipeline",
    description="High-performance CPU-only news enrichment pipeline with FastAPI service layer",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    dependencies=[Depends(verify_api_key)],
    lifespan=lifespan,
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)


# Dependency for database connection
async def get_db_client():
    from src.db import db_connection

    """Dependency to ensure database connection."""
    if not db_connection.client:
        await db_connection.connect()
    return db_connection


#  # ✅ Lazy import after app + loop exist
#     from src.db import db_connection, DatabaseClientAndPool, DatabaseError
#     from src.pipeline import pipeline_manager
#     from src.processing import feed_processor


async def get_pipeline_manager():
    from src.pipeline import pipeline_manager

    return pipeline_manager


async def get_feed_processor():
    from src.processing import feed_processor

    return feed_processor


# API Endpoints


@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint with API information."""
    return {
        "message": "MASX AI ETL CPU Pipeline API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "status": "operational",
    }


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Comprehensive health check endpoint.

    Returns the health status of all system components including
    database, thread pool, and processing modules.
    """
    try:
        pipeline_manager = await get_pipeline_manager()
        health_data = await pipeline_manager.health_check()
        return HealthResponse(**health_data)
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")


@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """
    Get comprehensive pipeline statistics.

    Returns detailed statistics about processing performance,
    thread pool usage, and database operations.
    """
    try:
        pipeline_manager = await get_pipeline_manager()
        stats_data = await pipeline_manager.get_pipeline_stats()
        return StatsResponse(**stats_data)
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


# @app.get("/text-cleaner/test")
# async def test_text_cleaner(text: str, language: str = "en"):
#     """
#     Test the text cleaner with sample text.

#     Useful for testing and debugging text cleaning functionality.
#     """
#     try:
#         result = text_cleaner.clean_text(text, language)
#         return result

#     except Exception as e:
#         logger.error(f"Text cleaner test failed: {e}")
#         raise HTTPException(status_code=500, detail=f"Text cleaner test failed: {str(e)}")


# @app.get("/geotagger/test")
# async def test_geotagger(text: str, language: str = "en"):
#     """
#     Test the geotagger with sample text.

#     Useful for testing and debugging geotagging functionality.
#     """
#     try:
#         result = geotagger.extract_geographic_entities(text, language)
#         return result

#     except Exception as e:
#         logger.error(f"Geotagger test failed: {e}")
#         raise HTTPException(status_code=500, detail=f"Geotagger test failed: {str(e)}")


# @app.get("/image-finder/test")
# async def test_image_finder(query: str, max_images: int = 3, language: str = "en"):
#     """
#     Test the image finder with a search query.

#     Useful for testing and debugging image search functionality.
#     """
#     try:
#         result = await image_finder.find_images(query, max_images, language)
#         return result

#     except Exception as e:
#         logger.error(f"Image finder test failed: {e}")
#         raise HTTPException(status_code=500, detail=f"Image finder test failed: {str(e)}")


# Feed Processing Endpoints


@app.post("/feed/warmup", response_model=FeedWarmupResponse)
async def warmup_feed_entries(
    request: FeedWarmupRequest, db: DatabaseClientAndPool = Depends(get_db_client)
):
    """
    Warm up the server by loading feed entries for a specific date.

    This endpoint loads all feed entries from the feed_entries_{date} table
    into memory for processing. If no date is provided, uses today's date.
    """
    try:
        feed_processor = await get_feed_processor()
        # Use today's date if not provided
        date = request.date or get_today_date()
        # Validate date format
        try:
            validated_date = validate_and_raise(date, "date")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Warm up the server
        feed_processor.set_date(validated_date)
        result = await feed_processor.warm_up_server()

        return FeedWarmupResponse(**result)

    except DatabaseError as e:
        logger.error(f"Database error during warm-up: {e}")
        raise HTTPException(
            status_code=404, detail=f"Table feed_entries_{date} not available"
        )
    except Exception as e:
        logger.error(f"Unexpected error during warm-up: {e}")
        raise HTTPException(status_code=500, detail=f"Warm-up failed: {str(e)}")


@app.post("/feed/process", response_model=FeedProcessResponse)
async def process_feed_entries(
    request: FeedProcessRequest,
    background_tasks: BackgroundTasks,
    db: DatabaseClientAndPool = Depends(get_db_client),
):
    """
    Process all feed entries for a specific date.

    This endpoint processes all feed entries from the feed_entries_{date} table
    through the complete pipeline: scrape → clean → geotag → find image → save to DB.
    If no date is provided, uses today's date.
    """
    try:
        feed_processor = await get_feed_processor()
        # Use today's date if not provided
        date = request.date or get_today_date()

        # Validate date format
        try:
            validated_date = validate_and_raise(date, "date")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Process feed entries
        feed_processor.set_date(validated_date)

        # Fire-and-forget mode for MASX AI trigger
        if getattr(request, "trigger", None) == "masxai":
            background_tasks.add_task(
                feed_processor.process_all_feed_entries, batch_mode=True
            )
            logger.info(f"MASX AI background job started for {validated_date}")
            return FeedProcessResponse(
                status="started",
                message=f"MASX AI background processing initiated for {validated_date}",
                date=validated_date,
                total_entries=0,
                successful=0,
                failed=0,
                processing_time=0,
                timestamp=datetime.utcnow().isoformat(),
            )

        result = await feed_processor.process_all_feed_entries(batch_mode=True)
        return FeedProcessResponse(**result)

    except DatabaseError as e:
        logger.error(f"Database error during processing: {e}")
        raise HTTPException(
            status_code=404, detail=f"Table feed_entries_{date} not available"
        )
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/feed/process/flashpoint", response_model=FeedProcessFlashpointResponse)
async def process_feed_entries_by_flashpoint(
    request: FeedProcessFlashpointRequest,
    background_tasks: BackgroundTasks,
    db: DatabaseClientAndPool = Depends(get_db_client),
):
    """
    Process feed entries for a specific date and flashpoint ID.

    This endpoint processes feed entries from the feed_entries_{date} table
    filtered by flashpoint_id through the complete pipeline.
    If no date is provided, uses today's date.
    """
    try:
        feed_processor = await get_feed_processor()
        # Use today's date if not provided
        date = request.date or get_today_date()
        flashpoint_id = request.flashpoint_id

        # Validate date format
        try:
            validated_date = validate_and_raise(date, "date")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Validate flashpoint_id
        if not flashpoint_id:
            raise HTTPException(status_code=400, detail="flashpoint_id is required")

        # Process feed entries by flashpoint ID
        feed_processor.set_date(validated_date)

        # Fire-and-forget mode for MASX AI trigger
        if getattr(request, "trigger", None) == "masxai":
            background_tasks.add_task(
                feed_processor.process_feed_entries_by_flashpoint_id, flashpoint_id
            )
            logger.info(
                f"MASX AI background job started for {validated_date} and flashpoint_id {flashpoint_id}"
            )
            return FeedProcessFlashpointResponse(
                status="started",
                message=f"MASX AI background processing initiated for {validated_date} and flashpoint_id {flashpoint_id}",
                date=validated_date,
                flashpoint_id=flashpoint_id,
                total_entries=0,
                successful=0,
                failed=0,
                processing_time=0,
                timestamp=datetime.utcnow().isoformat(),
            )

        result = await feed_processor.process_feed_entries_by_flashpoint_id(
            flashpoint_id
        )

        return FeedProcessFlashpointResponse(**result)

    except DatabaseError as e:
        logger.error(f"Database error during processing: {e}")
        raise HTTPException(
            status_code=404, detail=f"Table feed_entries_{date} not available"
        )
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.get("/feed/entries/{date}")
async def get_feed_entries(
    date: str, db: DatabaseClientAndPool = Depends(get_db_client)
):
    """
    Get loaded feed entries for a specific date.

    Returns the feed entries that are currently loaded in memory for the specified date.
    """
    try:
        feed_processor = await get_feed_processor()
        # Validate date format
        try:
            validated_date = validate_and_raise(date, "date")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Get feed entries from memory
        feed_processor.set_date(validated_date)
        feed_entries = feed_processor.get_feed_entries()

        return {
            "date": validated_date,
            "total_entries": len(feed_entries),
            "entries": feed_entries,
        }

    except Exception as e:
        logger.error(f"Error getting feed entries: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get feed entries: {str(e)}"
        )


@app.get("/feed/stats")
async def get_feed_stats():
    """
    Get feed processing statistics.

    Returns comprehensive statistics about feed processing including
    loaded entries, processing counts, and performance metrics.
    """
    try:
        feed_processor = await get_feed_processor()
        stats = feed_processor.get_processing_stats()
        return stats

    except Exception as e:
        logger.error(f"Error getting feed stats: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get feed stats: {str(e)}"
        )


@app.delete("/feed/clear/{date}")
async def clear_feed_entries(date: str):
    """
    Clear feed entries from memory for a specific date.

    This endpoint removes the loaded feed entries from memory to free up resources.
    """
    try:
        feed_processor = await get_feed_processor()
        # Validate date format
        try:
            validated_date = validate_and_raise(date, "date")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        # Clear feed entries
        feed_processor.clear_feed_entries(validated_date)

        return {
            "message": f"Cleared feed entries for date {validated_date}",
            "date": validated_date,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error clearing feed entries: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to clear feed entries: {str(e)}"
        )


@app.delete("/feed/clear")
async def clear_all_feed_entries():
    """
    Clear all feed entries from memory.

    This endpoint removes all loaded feed entries from memory to free up resources.
    """
    try:
        feed_processor = await get_feed_processor()
        # Clear all feed entries
        feed_processor.clear_feed_entries()

        return {
            "message": "Cleared all feed entries from memory",
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error clearing all feed entries: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to clear feed entries: {str(e)}"
        )


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "type": "internal_error"},
    )
