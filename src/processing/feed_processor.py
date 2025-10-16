"""
Feed entry processor for MASX AI ETL CPU Pipeline.

Handles processing of feed entries from date-based tables with
comprehensive pipeline orchestration and batch operations.
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta


from ..db import db_connection, DatabaseError
from ..models import FeedModel
from ..config import get_service_logger
from ..utils import validate_and_raise
from ..services import ProxyService

logger = get_service_logger(__name__)


def _get_pipeline_manager():
    """Lazy import to avoid circular dependency."""
    from ..pipeline.pipeline_manager import pipeline_manager

    return pipeline_manager


class FeedProcessor:
    """
    Feed entry processor for date-based feed processing.

    Handles loading feed entries from date-based tables and processing
    them through the complete enrichment pipeline.
    """

    def __init__(self):
        """Initialize the feed processor."""
        self.all_feed_entries: Dict[str, List[Dict[str, Any]]] = {}
        self.processing_stats = {
            "total_loaded": 0,
            "total_processed": 0,
            "successful": 0,
            "failed": 0,
            "last_processed_date": None,
        }
        self.date = datetime.now().strftime("%Y-%m-%d")
        db_connection.date = self.date

        logger.info("Feed processor initialized")

    def set_date(self, date: str):
        validated_date = validate_and_raise(date, "date")
        db_connection.date = validated_date
        self.date = validated_date

    async def warm_up_server(self) -> Dict[str, Any]:
        """
        Warm up the server by loading feed entries for a specific date.

        Args:
            date: Date in YYYY-MM-DD format (e.g., "2025-07-02")

        Returns:
            Dictionary containing warm-up results

        Raises:
            ValueError: If date format is invalid
        """

        try:
            logger.info(f"Warming up server for date: {self.date}")

            # Load feed entries for the date
            feed_entries = await self._load_feed_entries()

            # Store in global variable
            self.all_feed_entries[self.date] = feed_entries
            self.processing_stats["total_loaded"] = len(feed_entries)
            self.processing_stats["last_processed_date"] = self.date

            return {
                "status": "warmed_up",
                "date": self.date,
                "total_entries": len(feed_entries),
                "message": f"Server warmed up with {len(feed_entries)} feed entries for date {self.date}",
                "timestamp": datetime.utcnow().isoformat(),
            }

        except DatabaseError as e:
            logger.error(f"Database error during warm-up: {e}")
            return {
                "status": "error",
                "date": self.date,
                "error": str(e),
                "message": f"Table feed_entries_{self.date} not available",
                "timestamp": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.error(f"Unexpected error during warm-up: {e}")
            return {
                "status": "error",
                "date": self.date,
                "error": str(e),
                "message": "Unexpected error during warm-up",
                "timestamp": datetime.utcnow().isoformat(),
            }

    async def process_all_feed_entries(
        self, batch_mode: bool = False
    ) -> Dict[str, Any]:
        """
        Process all feed entries for a specific date.

        Args:
            date: Date in YYYY-MM-DD format (e.g., "2025-07-02")

        Returns:
            Dictionary containing processing results

        Raises:
            ValueError: If date format is invalid
        """

        try:
            logger.info(f"Processing feed entries for date: {self.date}")

            # Load feed entries if not already loaded
            if self.date not in self.all_feed_entries:
                feed_entries = await self._load_feed_entries()
                self.all_feed_entries[self.date] = feed_entries
            else:
                feed_entries = self.all_feed_entries[self.date]

            if not feed_entries:
                return {
                    "status": "no_entries",
                    "date": self.date,
                    "message": f"No feed entries found for date {self.date}",
                    "timestamp": datetime.utcnow().isoformat(),
                }

            # Process all entries
            results = {"successful": 0, "failed": 0, "processing_time": 0}

            proxy_service = ProxyService.get_instance()
            proxy_service.ping_start_proxy()
            await proxy_service.start_proxy_refresher()
            proxies = await proxy_service.get_proxy_cache()

            if batch_mode:
                results = await self._process_feed_entries_batch(feed_entries)
            else:
                results = await self._process_feed_entries(feed_entries)

            await proxy_service.stop_proxy_refresher()

            # Update statistics
            self.processing_stats["total_processed"] += len(feed_entries)
            self.processing_stats["successful"] += results["successful"]
            self.processing_stats["failed"] += results["failed"]
            self.processing_stats["last_processed_date"] = self.date

            return {
                "status": "completed",
                "date": self.date,
                "total_entries": len(feed_entries),
                "successful": results["successful"],
                "failed": results["failed"],
                "processing_time": results["processing_time"],
                "message": f"Processed {len(feed_entries)} feed entries for date {self.date}",
                "timestamp": datetime.utcnow().isoformat(),
            }

        except DatabaseError as e:
            logger.error(f"Database error during processing: {e}")
            return {
                "status": "error",
                "date": self.date,
                "error": str(e),
                "message": f"Table feed_entries_{self.date} not available",
                "timestamp": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.error(f"Unexpected error during processing: {e}")
            return {
                "status": "error",
                "date": self.date,
                "error": str(e),
                "message": "Unexpected error during processing",
                "timestamp": datetime.utcnow().isoformat(),
            }

    async def process_feed_entries_by_flashpoint_id(
        self, flashpoint_id: str
    ) -> Dict[str, Any]:
        """
        Process feed entries for a specific date and flashpoint_id.

        Args:
            date: Date in YYYY-MM-DD format (e.g., "2025-07-02")
            flashpoint_id: Flashpoint ID to filter by

        Returns:
            Dictionary containing processing results

        Raises:
            ValueError: If date format is invalid
        """

        try:
            logger.info(
                f"Processing feed entries for date: {self.date}, flashpoint_id: {flashpoint_id}"
            )

            # Load feed entries filtered by flashpoint_id
            feed_entries = await self._load_feed_entries_by_flashpoint_id(flashpoint_id)

            if not feed_entries:
                return {
                    "status": "no_entries",
                    "date": self.date,
                    "flashpoint_id": flashpoint_id,
                    "message": f"No feed entries found for date {self.date} and flashpoint_id {flashpoint_id}",
                    "timestamp": datetime.utcnow().isoformat(),
                }

            proxy_service = ProxyService.get_instance()
            proxy_service.ping_start_proxy()
            await proxy_service.start_proxy_refresher()
            proxies = await proxy_service.get_proxy_cache()

            # Process filtered entries
            results = await self._process_feed_entries(feed_entries)

            await proxy_service.stop_proxy_refresher()

            # Update statistics
            self.processing_stats["total_processed"] += len(feed_entries)
            self.processing_stats["successful"] += results["successful"]
            self.processing_stats["failed"] += results["failed"]
            self.processing_stats["last_processed_date"] = self.date

            return {
                "status": "completed",
                "date": self.date,
                "flashpoint_id": flashpoint_id,
                "total_entries": len(feed_entries),
                "successful": results["successful"],
                "failed": results["failed"],
                "processing_time": results["processing_time"],
                "message": f"Processed {len(feed_entries)} feed entries for date {self.date} and flashpoint_id {flashpoint_id}",
                "timestamp": datetime.utcnow().isoformat(),
            }

        except DatabaseError as e:
            logger.error(f"Database error during processing: {e}")
            return {
                "status": "error",
                "date": self.date,
                "flashpoint_id": flashpoint_id,
                "error": str(e),
                "message": f"Table feed_entries_{self.date} not available",
                "timestamp": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.error(f"Unexpected error during processing: {e}")
            return {
                "status": "error",
                "date": self.date,
                "flashpoint_id": flashpoint_id,
                "error": str(e),
                "message": "Unexpected error during processing",
                "timestamp": datetime.utcnow().isoformat(),
            }

    async def _load_feed_entries(self) -> List[Dict[str, Any]]:
        """Load feed entries for a specific date."""
        return await db_connection.fetch_feed_entries(self.date)

    async def _load_feed_entries_by_flashpoint_id(
        self, flashpoint_id: str
    ) -> List[Dict[str, Any]]:
        """Load feed entries for a specific date and flashpoint_id."""
        return await db_connection.fetch_feed_entries_by_flashpoint_id(
            self.date, flashpoint_id
        )

    async def _process_feed_entries(
        self, feed_entries: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Process a list of feed entries through the pipeline."""
        start_time = datetime.utcnow()
        successful = 0
        failed = 0

        for feed_entry in feed_entries:
            try:
                # Convert feed entry to article format for processing
                article_data: FeedModel = FeedModel.from_feed_entry(feed_entry)

                # Process through pipeline
                pipeline_manager = _get_pipeline_manager()
                result = await pipeline_manager.process_article(article_data, self.date)

                if result["status"] == "completed":
                    # Save processed article to database
                    enriched_data: FeedModel = result["enriched_data"]
                    save_success = await db_connection.update_processed_article(
                        enriched_data, self.date
                    )

                    if save_success:
                        successful += 1
                        logger.debug(
                            f"Successfully processed and saved article: {feed_entry.get('url')}"
                        )
                    else:
                        failed += 1
                        logger.warning(
                            f"Failed to save processed article: {feed_entry.get('url')}"
                        )
                else:
                    failed += 1
                    logger.warning(
                        f"Failed to process article: {feed_entry.get('url')} - {result.get('errors', [])}"
                    )

            except Exception as e:
                failed += 1
                logger.error(
                    f"Error processing feed entry {feed_entry.get('url')}: {e}"
                )

        processing_time = (datetime.utcnow() - start_time).total_seconds()

        return {
            "successful": successful,
            "failed": failed,
            "processing_time": processing_time,
        }

    async def _process_feed_entries_batch(
        self, feed_entries: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Process a list of feed entries through the pipeline in batch mode."""

        logger.info(f"Starting batch processing for {len(feed_entries)} articles")

        start_time = datetime.utcnow()
        successful = 0
        failed = 0

        try:
            # Convert all feed entries to article format for batch processing
            article_data_list = []
            for feed_entry in feed_entries:
                try:
                    article_data: FeedModel = FeedModel.from_feed_entry(feed_entry)
                    article_data_list.append(article_data)
                except Exception as e:
                    failed += 1
                    logger.error(
                        f"Error converting feed entry {feed_entry.get('url', 'unknown')}: {e}"
                    )

            if not article_data_list:
                logger.warning("No valid article data to process")
                return {
                    "successful": 0,
                    "failed": len(feed_entries),
                    "processing_time": (datetime.utcnow() - start_time).total_seconds(),
                }

            # article_data_list = article_data_list[:3]

            # Process articles in batch
            pipeline_manager = _get_pipeline_manager()
            data_results = await pipeline_manager.process_batch(
                article_data_list, self.date
            )

            if data_results["status"] == "completed":
                results = data_results["results"]

                # Process results and save to database
                for i, result in enumerate(results):
                    try:
                        if result["status"] == "completed":
                            # Save processed article to database
                            enriched_data: FeedModel = result["enriched_data"]
                            save_success = await db_connection.update_processed_article(
                                enriched_data, self.date
                            )

                            if save_success:
                                successful += 1
                                logger.debug(
                                    f"Successfully processed and saved article: {enriched_data.url}"
                                )
                            else:
                                failed += 1
                                logger.warning(
                                    f"Failed to save processed article: {enriched_data.url}"
                                )
                        else:
                            failed += 1
                            logger.warning(
                                f"Failed to process article: {article_data_list[i].url} - {result.get('errors', [])}"
                            )

                    except Exception as e:
                        failed += 1
                        logger.error(
                            f"Error saving processed article {article_data_list[i].url}: {e}"
                        )
            else:
                # If batch processing failed, mark all as failed
                failed = len(article_data_list)
                logger.error(
                    f"Batch processing failed: {data_results.get('error', 'Unknown error')}"
                )

        except Exception as e:
            # If there's a critical error, mark all remaining as failed
            remaining_failed = len(feed_entries) - successful - failed
            failed += remaining_failed
            logger.error(f"Critical error during batch processing: {e}")

        processing_time = (datetime.utcnow() - start_time).total_seconds()

        logger.info(
            f"Batch processing completed: {successful} successful, {failed} failed in {processing_time:.2f}s"
        )

        return {
            "successful": successful,
            "failed": failed,
            "processing_time": processing_time,
        }

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return {
            "feed_processor_stats": self.processing_stats,
            "loaded_dates": list(self.all_feed_entries.keys()),
            "total_loaded_entries": sum(
                len(entries) for entries in self.all_feed_entries.values()
            ),
        }

    def get_feed_entries(self) -> List[Dict[str, Any]]:
        """
        Get loaded feed entries.

        Returns:
            List of feed entries for the specified date

        Raises:
            ValueError: If date format is invalid
        """
        # Validate date format

        return self.all_feed_entries.get(self.date, [])

    def clear_feed_entries(self, date: str = None):
        """
        Clear feed entries from memory.

        Args:
            date: Date in YYYY-MM-DD format (e.g., "2025-07-02"). If None, clears all entries.

        Raises:
            ValueError: If date format is invalid
        """
        if date:
            # Validate date format
            validated_date = validate_and_raise(date, "date")
            if validated_date in self.all_feed_entries:
                del self.all_feed_entries[validated_date]
                logger.info(f"Cleared feed entries for date: {validated_date}")
        else:
            self.all_feed_entries.clear()
            logger.info("Cleared all feed entries")


# Global feed processor instance
feed_processor = FeedProcessor()


def get_feed_processor() -> FeedProcessor:
    return feed_processor
