"""
Feed entry processor for MASX AI ETL CPU Pipeline.

Handles processing of feed entries from date-based tables with
comprehensive pipeline orchestration and batch operations.
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging

from ..db.db_client import db_client, DatabaseError
from ..pipeline.pipeline_manager import pipeline_manager


logger = logging.getLogger(__name__)


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
            "last_processed_date": None
        }
        
        logger.info("Feed processor initialized")
    
    async def warm_up_server(self, date: str) -> Dict[str, Any]:
        """
        Warm up the server by loading feed entries for a specific date.
        
        Args:
            date: Date in YYYYMMDD format (e.g., "20250702")
            
        Returns:
            Dictionary containing warm-up results
        """
        try:
            logger.info(f"Warming up server for date: {date}")
            
            # Load feed entries for the date
            feed_entries = await self._load_feed_entries(date)
            
            # Store in global variable
            self.all_feed_entries[date] = feed_entries
            self.processing_stats["total_loaded"] = len(feed_entries)
            self.processing_stats["last_processed_date"] = date
            
            return {
                "status": "warmed_up",
                "date": date,
                "total_entries": len(feed_entries),
                "message": f"Server warmed up with {len(feed_entries)} feed entries for date {date}",
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except DatabaseError as e:
            logger.error(f"Database error during warm-up: {e}")
            return {
                "status": "error",
                "date": date,
                "error": str(e),
                "message": f"Table feed_entries_{date} not available",
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Unexpected error during warm-up: {e}")
            return {
                "status": "error",
                "date": date,
                "error": str(e),
                "message": "Unexpected error during warm-up",
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def process_feed_entries_by_date(self, date: str) -> Dict[str, Any]:
        """
        Process all feed entries for a specific date.
        
        Args:
            date: Date in YYYYMMDD format (e.g., "20250702")
            
        Returns:
            Dictionary containing processing results
        """
        try:
            logger.info(f"Processing feed entries for date: {date}")
            
            # Load feed entries if not already loaded
            if date not in self.all_feed_entries:
                feed_entries = await self._load_feed_entries(date)
                self.all_feed_entries[date] = feed_entries
            else:
                feed_entries = self.all_feed_entries[date]
            
            if not feed_entries:
                return {
                    "status": "no_entries",
                    "date": date,
                    "message": f"No feed entries found for date {date}",
                    "timestamp": datetime.utcnow().isoformat()
                }
            
            # Process all entries
            results = await self._process_feed_entries(feed_entries, date)
            
            # Update statistics
            self.processing_stats["total_processed"] += len(feed_entries)
            self.processing_stats["successful"] += results["successful"]
            self.processing_stats["failed"] += results["failed"]
            self.processing_stats["last_processed_date"] = date
            
            return {
                "status": "completed",
                "date": date,
                "total_entries": len(feed_entries),
                "successful": results["successful"],
                "failed": results["failed"],
                "processing_time": results["processing_time"],
                "message": f"Processed {len(feed_entries)} feed entries for date {date}",
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except DatabaseError as e:
            logger.error(f"Database error during processing: {e}")
            return {
                "status": "error",
                "date": date,
                "error": str(e),
                "message": f"Table feed_entries_{date} not available",
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Unexpected error during processing: {e}")
            return {
                "status": "error",
                "date": date,
                "error": str(e),
                "message": "Unexpected error during processing",
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def process_feed_entries_by_flashpoint_id(self, date: str, flashpoint_id: str) -> Dict[str, Any]:
        """
        Process feed entries for a specific date and flashpoint_id.
        
        Args:
            date: Date in YYYYMMDD format (e.g., "20250702")
            flashpoint_id: Flashpoint ID to filter by
            
        Returns:
            Dictionary containing processing results
        """
        try:
            logger.info(f"Processing feed entries for date: {date}, flashpoint_id: {flashpoint_id}")
            
            # Load feed entries filtered by flashpoint_id
            feed_entries = await self._load_feed_entries_by_flashpoint_id(date, flashpoint_id)
            
            if not feed_entries:
                return {
                    "status": "no_entries",
                    "date": date,
                    "flashpoint_id": flashpoint_id,
                    "message": f"No feed entries found for date {date} and flashpoint_id {flashpoint_id}",
                    "timestamp": datetime.utcnow().isoformat()
                }
            
            # Process filtered entries
            results = await self._process_feed_entries(feed_entries, date)
            
            # Update statistics
            self.processing_stats["total_processed"] += len(feed_entries)
            self.processing_stats["successful"] += results["successful"]
            self.processing_stats["failed"] += results["failed"]
            self.processing_stats["last_processed_date"] = date
            
            return {
                "status": "completed",
                "date": date,
                "flashpoint_id": flashpoint_id,
                "total_entries": len(feed_entries),
                "successful": results["successful"],
                "failed": results["failed"],
                "processing_time": results["processing_time"],
                "message": f"Processed {len(feed_entries)} feed entries for date {date} and flashpoint_id {flashpoint_id}",
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except DatabaseError as e:
            logger.error(f"Database error during processing: {e}")
            return {
                "status": "error",
                "date": date,
                "flashpoint_id": flashpoint_id,
                "error": str(e),
                "message": f"Table feed_entries_{date} not available",
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Unexpected error during processing: {e}")
            return {
                "status": "error",
                "date": date,
                "flashpoint_id": flashpoint_id,
                "error": str(e),
                "message": "Unexpected error during processing",
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def _load_feed_entries(self, date: str) -> List[Dict[str, Any]]:
        """Load feed entries for a specific date."""
        return await db_client.fetch_feed_entries_by_date(date)
    
    async def _load_feed_entries_by_flashpoint_id(self, date: str, flashpoint_id: str) -> List[Dict[str, Any]]:
        """Load feed entries for a specific date and flashpoint_id."""
        return await db_client.fetch_feed_entries_by_flashpoint_id(date, flashpoint_id)
    
    async def _process_feed_entries(self, feed_entries: List[Dict[str, Any]], date: str) -> Dict[str, Any]:
        """Process a list of feed entries through the pipeline."""
        start_time = datetime.utcnow()
        successful = 0
        failed = 0
        
        for feed_entry in feed_entries:
            try:
                # Convert feed entry to article format for processing
                article_data = self._convert_feed_entry_to_article(feed_entry)
                
                # Process through pipeline
                result = await pipeline_manager.process_article(article_data)
                
                if result["status"] == "completed":
                    # Save processed article to database
                    save_success = await db_client.save_processed_article(feed_entry, result["enriched_data"])
                    
                    if save_success:
                        successful += 1
                        logger.debug(f"Successfully processed and saved article: {feed_entry.get('url')}")
                    else:
                        failed += 1
                        logger.warning(f"Failed to save processed article: {feed_entry.get('url')}")
                else:
                    failed += 1
                    logger.warning(f"Failed to process article: {feed_entry.get('url')} - {result.get('errors', [])}")
                    
            except Exception as e:
                failed += 1
                logger.error(f"Error processing feed entry {feed_entry.get('url')}: {e}")
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        return {
            "successful": successful,
            "failed": failed,
            "processing_time": processing_time
        }
    
    def _convert_feed_entry_to_article(self, feed_entry: Dict[str, Any]) -> Dict[str, Any]:
        """Convert feed entry to article format for processing."""
        return {
            "id": feed_entry.get("id"),
            "url": feed_entry.get("url"),
            "title": feed_entry.get("title"),
            "content": feed_entry.get("description", ""),
            "author": "",  # Not available in feed entry
            "published_date": feed_entry.get("seendate"),
            "metadata": {
                "flashpoint_id": feed_entry.get("flashpoint_id"),
                "domain": feed_entry.get("domain"),
                "language": feed_entry.get("language"),
                "source_country": feed_entry.get("sourcecountry"),
                "original_image": feed_entry.get("image"),
                "feed_entry_id": feed_entry.get("id")
            }
        }
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return {
            "feed_processor_stats": self.processing_stats,
            "loaded_dates": list(self.all_feed_entries.keys()),
            "total_loaded_entries": sum(len(entries) for entries in self.all_feed_entries.values())
        }
    
    def get_feed_entries_for_date(self, date: str) -> List[Dict[str, Any]]:
        """Get loaded feed entries for a specific date."""
        return self.all_feed_entries.get(date, [])
    
    def clear_feed_entries(self, date: str = None):
        """Clear feed entries from memory."""
        if date:
            if date in self.all_feed_entries:
                del self.all_feed_entries[date]
                logger.info(f"Cleared feed entries for date: {date}")
        else:
            self.all_feed_entries.clear()
            logger.info("Cleared all feed entries")


# Global feed processor instance
feed_processor = FeedProcessor()
