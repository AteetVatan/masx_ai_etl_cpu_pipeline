"""
Supabase database client for MASX AI ETL CPU Pipeline.

Provides high-performance batch operations for fetching and updating articles
with proper error handling and connection management.
"""

import asyncio
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import logging

from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions

from ..config.settings import settings


logger = logging.getLogger(__name__)


class DatabaseClient:
    """
    High-performance Supabase client with batch operations.
    
    Handles connection management, batch fetching, and updating of articles
    with proper error handling and retry logic.
    """
    
    def __init__(self):
        """Initialize the Supabase client with connection pooling."""
        self.client: Optional[Client] = None
        self._connection_pool_size = 10
        self._max_retries = settings.retry_attempts
        self._retry_delay = settings.retry_delay
        
    async def connect(self) -> None:
        """
        Establish connection to Supabase with retry logic.
        
        Raises:
            ConnectionError: If unable to connect after retries
        """
        for attempt in range(self._max_retries):
            try:
                # Configure client options for better performance
                options = ClientOptions(
                    auto_refresh_token=True,
                    persist_session=True,
                    detect_session_in_url=True
                )
                
                self.client = create_client(
                    settings.supabase_url,
                    settings.supabase_key,
                    options=options
                )
                
                # Test connection
                await self._test_connection()
                logger.info("Successfully connected to Supabase")
                return
                
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(self._retry_delay * (2 ** attempt))
                else:
                    raise ConnectionError(f"Failed to connect to Supabase after {self._max_retries} attempts: {e}")
    
    async def _test_connection(self) -> None:
        """Test the database connection with a simple query."""
        if not self.client:
            raise ConnectionError("Client not initialized")
        
        # Simple query to test connection
        result = self.client.table("articles").select("id").limit(1).execute()
        if not result.data:
            logger.warning("Connection test returned no data - this may be normal for empty tables")
    
    async def disconnect(self) -> None:
        """Close the database connection."""
        if self.client:
            # Supabase client doesn't have explicit close method
            # but we can clear the reference
            self.client = None
            logger.info("Disconnected from Supabase")
    
    async def fetch_feed_entries_by_date(self, date: str) -> List[Dict[str, Any]]:
        """
        Fetch all feed entries for a specific date.
        
        Args:
            date: Date in YYYYMMDD format (e.g., "20250702")
            
        Returns:
            List of feed entry dictionaries
            
        Raises:
            DatabaseError: If fetch operation fails
        """
        if not self.client:
            raise ConnectionError("Database client not connected")
        
        try:
            table_name = f"feed_entries_{date}"
            
            # Try to fetch all entries from the table
            # If table doesn't exist, Supabase will return an error
            result = self.client.table(table_name).select("*").execute()
            
            if result.data is None:
                logger.warning(f"No feed entries found in table {table_name}")
                return []
            
            logger.info(f"Fetched {len(result.data)} feed entries from {table_name}")
            return result.data
            
        except Exception as e:
            error_msg = str(e)
            if "relation" in error_msg.lower() and "does not exist" in error_msg.lower():
                raise DatabaseError(f"Table feed_entries_{date} not available")
            else:
                logger.error(f"Failed to fetch feed entries for date {date}: {e}")
                raise DatabaseError(f"Failed to fetch feed entries for date {date}: {e}")
    
    async def fetch_feed_entries_by_flashpoint_id(self, date: str, flashpoint_id: str) -> List[Dict[str, Any]]:
        """
        Fetch feed entries for a specific date and flashpoint_id.
        
        Args:
            date: Date in YYYYMMDD format (e.g., "20250702")
            flashpoint_id: Flashpoint ID to filter by
            
        Returns:
            List of feed entry dictionaries
            
        Raises:
            DatabaseError: If fetch operation fails
        """
        if not self.client:
            raise ConnectionError("Database client not connected")
        
        try:
            table_name = f"feed_entries_{date}"
            
            # Try to fetch entries filtered by flashpoint_id
            # If table doesn't exist, Supabase will return an error
            result = self.client.table(table_name).select("*").eq("flashpoint_id", flashpoint_id).execute()
            
            if result.data is None:
                logger.warning(f"No feed entries found for flashpoint_id {flashpoint_id} in table {table_name}")
                return []
            
            logger.info(f"Fetched {len(result.data)} feed entries for flashpoint_id {flashpoint_id} from {table_name}")
            return result.data
            
        except Exception as e:
            error_msg = str(e)
            if "relation" in error_msg.lower() and "does not exist" in error_msg.lower():
                raise DatabaseError(f"Table feed_entries_{date} not available")
            else:
                logger.error(f"Failed to fetch feed entries for date {date} and flashpoint_id {flashpoint_id}: {e}")
                raise DatabaseError(f"Failed to fetch feed entries for date {date} and flashpoint_id {flashpoint_id}: {e}")

    async def fetch_articles_batch(
        self, 
        limit: int = None, 
        offset: int = 0,
        status: str = "pending"
    ) -> List[Dict[str, Any]]:
        """
        Fetch a batch of articles for processing.
        
        Args:
            limit: Maximum number of articles to fetch
            offset: Number of articles to skip
            status: Article status to filter by
            
        Returns:
            List of article dictionaries
            
        Raises:
            DatabaseError: If fetch operation fails
        """
        if not self.client:
            raise ConnectionError("Database client not connected")
        
        try:
            query = self.client.table("articles").select("*")
            
            # Apply filters
            if status:
                query = query.eq("status", status)
            
            # Apply pagination
            if limit:
                query = query.limit(limit)
            if offset:
                query = query.range(offset, offset + (limit or 100) - 1)
            
            # Order by creation date for consistent processing
            query = query.order("created_at", desc=False)
            
            result = query.execute()
            
            if result.data is None:
                logger.warning("No articles found in database")
                return []
            
            logger.info(f"Fetched {len(result.data)} articles from database")
            return result.data
            
        except Exception as e:
            logger.error(f"Failed to fetch articles: {e}")
            raise DatabaseError(f"Failed to fetch articles: {e}")
    
    async def fetch_article_by_id(self, article_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single article by ID.
        
        Args:
            article_id: Unique article identifier
            
        Returns:
            Article dictionary or None if not found
            
        Raises:
            DatabaseError: If fetch operation fails
        """
        if not self.client:
            raise ConnectionError("Database client not connected")
        
        try:
            result = self.client.table("articles").select("*").eq("id", article_id).execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]
            return None
            
        except Exception as e:
            logger.error(f"Failed to fetch article {article_id}: {e}")
            raise DatabaseError(f"Failed to fetch article {article_id}: {e}")
    
    async def update_articles_batch(
        self, 
        updates: List[Dict[str, Any]]
    ) -> Tuple[int, int]:
        """
        Update multiple articles in a single batch operation.
        
        Args:
            updates: List of article updates with 'id' field
            
        Returns:
            Tuple of (successful_updates, failed_updates)
            
        Raises:
            DatabaseError: If batch update fails
        """
        if not self.client:
            raise ConnectionError("Database client not connected")
        
        if not updates:
            return 0, 0
        
        successful_updates = 0
        failed_updates = 0
        
        try:
            # Process updates in batches to avoid overwhelming the database
            batch_size = settings.batch_size
            total_batches = (len(updates) + batch_size - 1) // batch_size
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(updates))
                batch = updates[start_idx:end_idx]
                
                try:
                    # Update each article individually for better error handling
                    for update in batch:
                        article_id = update.get("id")
                        if not article_id:
                            logger.warning("Skipping update without ID")
                            failed_updates += 1
                            continue
                        
                        # Remove ID from update data
                        update_data = {k: v for k, v in update.items() if k != "id"}
                        update_data["updated_at"] = datetime.utcnow().isoformat()
                        
                        result = self.client.table("articles").update(update_data).eq("id", article_id).execute()
                        
                        if result.data:
                            successful_updates += 1
                        else:
                            failed_updates += 1
                            
                except Exception as e:
                    logger.error(f"Failed to update batch {batch_idx + 1}: {e}")
                    failed_updates += len(batch)
            
            logger.info(f"Batch update completed: {successful_updates} successful, {failed_updates} failed")
            return successful_updates, failed_updates
            
        except Exception as e:
            logger.error(f"Failed to update articles batch: {e}")
            raise DatabaseError(f"Failed to update articles batch: {e}")
    
    async def update_article_status(
        self, 
        article_id: str, 
        status: str, 
        error_message: str = None
    ) -> bool:
        """
        Update a single article's status.
        
        Args:
            article_id: Unique article identifier
            status: New status (processing, completed, failed)
            error_message: Error message if status is failed
            
        Returns:
            True if update successful, False otherwise
        """
        if not self.client:
            raise ConnectionError("Database client not connected")
        
        try:
            update_data = {
                "status": status,
                "updated_at": datetime.utcnow().isoformat()
            }
            
            if error_message:
                update_data["error_message"] = error_message
            
            result = self.client.table("articles").update(update_data).eq("id", article_id).execute()
            
            if result.data:
                logger.debug(f"Updated article {article_id} status to {status}")
                return True
            else:
                logger.warning(f"Failed to update article {article_id} status")
                return False
                
        except Exception as e:
            logger.error(f"Failed to update article {article_id} status: {e}")
            return False
    
    async def save_processed_article(self, feed_entry: Dict[str, Any], enriched_data: Dict[str, Any]) -> bool:
        """
        Save processed article data back to the articles table.
        
        Args:
            feed_entry: Original feed entry data
            enriched_data: Processed and enriched article data
            
        Returns:
            True if save successful, False otherwise
        """
        if not self.client:
            raise ConnectionError("Database client not connected")
        
        try:
            # Prepare article data for saving
            article_data = {
                "id": feed_entry.get("id"),
                "url": feed_entry.get("url"),
                "title": enriched_data.get("title", feed_entry.get("title")),
                "content": enriched_data.get("content", ""),
                "author": enriched_data.get("author", ""),
                "published_date": enriched_data.get("published_date", feed_entry.get("seendate")),
                "status": "completed",
                "enriched_data": enriched_data,
                "metadata": {
                    "flashpoint_id": feed_entry.get("flashpoint_id"),
                    "domain": feed_entry.get("domain"),
                    "language": feed_entry.get("language"),
                    "source_country": feed_entry.get("sourcecountry"),
                    "description": feed_entry.get("description"),
                    "original_image": feed_entry.get("image"),
                    "processing_time": enriched_data.get("processing_time", 0),
                    "processing_steps": enriched_data.get("processing_steps", [])
                },
                "created_at": feed_entry.get("created_at"),
                "updated_at": datetime.utcnow().isoformat()
            }
            
            # Insert or update the article
            result = self.client.table("articles").upsert(article_data).execute()
            
            if result.data:
                logger.debug(f"Saved processed article for URL: {feed_entry.get('url')}")
                return True
            else:
                logger.warning(f"Failed to save processed article for URL: {feed_entry.get('url')}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to save processed article: {e}")
            return False

    async def get_processing_stats(self) -> Dict[str, int]:
        """
        Get processing statistics from the database.
        
        Returns:
            Dictionary with status counts
        """
        if not self.client:
            raise ConnectionError("Database client not connected")
        
        try:
            # Get counts for each status
            statuses = ["pending", "processing", "completed", "failed"]
            stats = {}
            
            for status in statuses:
                result = self.client.table("articles").select("id", count="exact").eq("status", status).execute()
                stats[status] = result.count or 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get processing stats: {e}")
            return {status: 0 for status in ["pending", "processing", "completed", "failed"]}


class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    pass


# Global database client instance
db_client = DatabaseClient()
