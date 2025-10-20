"""
Supabase database client for MASX AI ETL CPU Pipeline.

Provides high-performance batch operations for fetching and updating articles
with proper error handling and connection management.
"""

import asyncio
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from src.models import FeedModel

import asyncpg
import re
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions

from src.config import get_settings, get_service_logger
from src.utils import validate_and_raise, format_date_for_table
from src.core.exceptions import DatabaseError


class DatabaseClientAndPool:
    """
    High-performance Supabase client with batch operations.

    Handles connection management, batch fetching, and updating of articles
    with proper error handling and retry logic.
    """

    FEED_TABLE_PREFIX = "feed_entries"

    def __init__(self):
        """Initialize the Supabase client with connection pooling."""
        self.client: Optional[Client] = None
        self.pool: Optional[asyncpg.Pool] = None  # asyncpg pool
        self.settings = get_settings()
        self._max_retries = self.settings.retry_attempts
        self._retry_delay = self.settings.retry_delay
        self._test_table = "test_table"
        self.logger = get_service_logger(__name__)

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
                    schema="public",
                    headers={"X-Client-Info": "masx-ai-system"},
                )

                self.client = create_client(
                    self.settings.supabase_url,
                    self.settings.supabase_anon_key,
                    options=options,
                )

                database_url = self.settings.supabase_db_url
                if database_url:
                    if not self.is_valid_postgres_url(database_url):
                        raise DatabaseError(
                            f"Invalid PostgreSQL connection URL: {database_url}"
                        )

                    # Initialize connection pool for direct PostgreSQL access
                    self.pool = await asyncpg.create_pool(
                        database_url,
                        min_size=self.settings.database_min_connections,
                        max_size=self.settings.database_max_connections,
                        command_timeout=self.settings.request_timeout,
                        statement_cache_size=0,
                        server_settings={"application_name": "masx_ai_system"},
                    )

                # Test connection
                await self._test_client()
                self.logger.info("Successfully connected to Supabase")
                return

            except Exception as e:
                self.logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(self._retry_delay * (2**attempt))
                else:
                    raise ConnectionError(
                        f"Failed to connect to Supabase after {self._max_retries} attempts: {e}"
                    )

    async def _test_client(self) -> None:
        """Test the database connection with a simple query."""
        if not self.client:
            raise ConnectionError("Client not initialized")

        try:
            # Query system information table (always exists)
            table_name = self._test_table
            #'feed_entries_20250701'

            result = self.client.table(table_name).select("id").limit(1).execute()

            if result.data:
                self.logger.info(f"Database version: {result.data}")
            else:
                raise ConnectionError("Failed to get database version")
        except Exception as e:
            raise ConnectionError(f"Connection test failed: {e}")

        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    version = await conn.fetchval("SELECT version()")
                    self.logger.info(
                        f"PostgreSQL pool connection verified: {version[:50]}..."
                    )
            except Exception as e:
                raise ConnectionError(f"PostgreSQL pool test failed: {e}")

    async def disconnect(self):
        """Close database connections."""
        try:
            if self.pool:
                await self.pool.close()
                self.pool = None

            if self.client:
                self.client = None

            self.logger.info("Database connections closed")

        except Exception as e:
            self.logger.error(f"Error closing database connections: {e}")

    async def fetch_feed_entries(self, date: str) -> List[Dict[str, Any]]:
        """
        Fetch all feed entries for a specific date.

        Args:
            date: Date in YYYY-MM-DD format (e.g., "2025-07-02")

        Returns:
            List of feed entry dictionaries

        Raises:
            DatabaseError: If fetch operation fails
            ValueError: If date format is invalid
        """
        if not self.client:
            raise ConnectionError("Database client not connected")

        # Validate date format
        date = validate_and_raise(date, "date")

        try:
            table_name = format_date_for_table(date)

            # Try to fetch all entries from the table
            # If table doesn't exist, Supabase will return an error
            result = self.client.table(table_name).select("*").execute()

            if result.data is None:
                self.logger.warning(f"No feed entries found in table {table_name}")
                return []

            self.logger.info(
                f"Fetched {len(result.data)} feed entries from {table_name}"
            )
            return result.data

        except Exception as e:
            error_msg = str(e)
            if (
                "relation" in error_msg.lower()
                and "does not exist" in error_msg.lower()
            ):
                raise DatabaseError(f"Table feed_entries_{date} not available")
            else:
                self.logger.error(f"Failed to fetch feed entries for date {date}: {e}")
                raise DatabaseError(
                    f"Failed to fetch feed entries for date {date}: {e}"
                )

    async def fetch_feed_entries_by_flashpoint_id(
        self, date: str, flashpoint_id: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch feed entries for a specific date and flashpoint_id.

        Args:
            date: Date in YYYY-MM-DD format (e.g., "2025-07-02")
            flashpoint_id: Flashpoint ID to filter by

        Returns:
            List of feed entry dictionaries

        Raises:
            DatabaseError: If fetch operation fails
            ValueError: If date format is invalid
        """
        if not self.client:
            raise ConnectionError("Database client not connected")

        # Validate date format
        date = validate_and_raise(date, "date")

        try:
            table_name = format_date_for_table(date)

            # Try to fetch entries filtered by flashpoint_id
            # If table doesn't exist, Supabase will return an error
            result = (
                self.client.table(table_name)
                .select("*")
                .eq("flashpoint_id", flashpoint_id)
                .execute()
            )

            if result.data is None:
                self.logger.warning(
                    f"No feed entries found for flashpoint_id {flashpoint_id} in table {table_name}"
                )
                return []

            self.logger.info(
                f"Fetched {len(result.data)} feed entries for flashpoint_id {flashpoint_id} from {table_name}"
            )
            return result.data

        except Exception as e:
            error_msg = str(e)
            if (
                "relation" in error_msg.lower()
                and "does not exist" in error_msg.lower()
            ):
                raise DatabaseError(f"Table feed_entries_{date} not available")
            else:
                self.logger.error(
                    f"Failed to fetch feed entries for date {date} and flashpoint_id {flashpoint_id}: {e}"
                )
                raise DatabaseError(
                    f"Failed to fetch feed entries for date {date} and flashpoint_id {flashpoint_id}: {e}"
                )
                
               
            
    async def fetch_feed_entries_by_article_id(
        self, date: str, flashpoint_id: str, article_id: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch feed entries for a specific date, flashpoint_id and article_id.

        Args:
            date: Date in YYYY-MM-DD format (e.g., "2025-07-02")
            flashpoint_id: Flashpoint ID to filter by
            article_id: Article ID to filter by
        Returns:
            List of feed entry dictionaries

        Raises:
            DatabaseError: If fetch operation fails
            ValueError: If date format is invalid
        """
        if not self.client:
            raise ConnectionError("Database client not connected")

        # Validate date format
        date = validate_and_raise(date, "date")

        try:
            table_name = format_date_for_table(date)

            # Try to fetch entries filtered by flashpoint_id
            # If table doesn't exist, Supabase will return an error
            result = (
                self.client.table(table_name)
                .select("*")
                .eq("flashpoint_id", flashpoint_id)
                .eq("id", article_id)
                .execute()
            )

            if result.data is None:
                self.logger.warning(
                    f"No feed entries found for flashpoint_id {flashpoint_id} in table {table_name}"
                )
                return []

            self.logger.info(
                f"Fetched {len(result.data)} feed entries for flashpoint_id {flashpoint_id} from {table_name}"
            )
            return result.data

        except Exception as e:
            error_msg = str(e)
            if (
                "relation" in error_msg.lower()
                and "does not exist" in error_msg.lower()
            ):
                raise DatabaseError(f"Table feed_entries_{date} not available")
            else:
                self.logger.error(
                    f"Failed to fetch feed entries for date {date} and flashpoint_id {flashpoint_id}: {e}"
                )
                raise DatabaseError(
                    f"Failed to fetch feed entries for date {date} and flashpoint_id {flashpoint_id}: {e}"
                )            

    async def fetch_articles_batch(
        self, limit: int = None, offset: int = 0, status: str = "pending"
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
                self.logger.warning("No articles found in database")
                return []

            self.logger.info(f"Fetched {len(result.data)} articles from database")
            return result.data

        except Exception as e:
            self.logger.error(f"Failed to fetch articles: {e}")
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
            result = (
                self.client.table("articles").select("*").eq("id", article_id).execute()
            )

            if result.data and len(result.data) > 0:
                return result.data[0]
            return None

        except Exception as e:
            self.logger.error(f"Failed to fetch article {article_id}: {e}")
            raise DatabaseError(f"Failed to fetch article {article_id}: {e}")

    async def update_articles_batch(
        self, updates: List[Dict[str, Any]]
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
            batch_size = self.settings.db_batch_size
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
                            self.logger.warning("Skipping update without ID")
                            failed_updates += 1
                            continue

                        # Remove ID from update data
                        update_data = {k: v for k, v in update.items() if k != "id"}
                        update_data["updated_at"] = datetime.utcnow().isoformat()

                        result = (
                            self.client.table("articles")
                            .update(update_data)
                            .eq("id", article_id)
                            .execute()
                        )

                        if result.data:
                            successful_updates += 1
                        else:
                            failed_updates += 1

                except Exception as e:
                    self.logger.error(f"Failed to update batch {batch_idx + 1}: {e}")
                    failed_updates += len(batch)

            self.logger.info(
                f"Batch update completed: {successful_updates} successful, {failed_updates} failed"
            )
            return successful_updates, failed_updates

        except Exception as e:
            self.logger.error(f"Failed to updat  e articles batch: {e}")
            raise DatabaseError(f"Failed to update articles batch: {e}")

    async def update_article_status(
        self, article_id: str, status: str, error_message: str = None
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
                "updated_at": datetime.utcnow().isoformat(),
            }

            if error_message:
                update_data["error_message"] = error_message

            result = (
                self.client.table("articles")
                .update(update_data)
                .eq("id", article_id)
                .execute()
            )

            if result.data:
                self.logger.debug(f"Updated article {article_id} status to {status}")
                return True
            else:
                self.logger.warning(f"Failed to update article {article_id} status")
                return False

        except Exception as e:
            self.logger.error(f"Failed to update article {article_id} status: {e}")
            return False

    async def update_processed_article(
        self, enriched_data: FeedModel, date: str
    ) -> bool:
        """
        Update processed article data back to the feed_entries table.

        Args:
            enriched_data (FeedModel): Processed and enriched article data
            date: Date in YYYY-MM-DD format

        Returns:
            bool: True if save successful, False otherwise
        """
        if not self.client:
            raise ConnectionError("Database client not connected")

        # Validate date format
        date = validate_and_raise(date, "date")

        try:
            table_name = format_date_for_table(date)

            # Prepare article data for saving (only include schema-valid fields)
            article_data = {
                "url": enriched_data.url,
                "title": enriched_data.title,
                "title_en": enriched_data.title_en,
                "content": enriched_data.content,
                "language": enriched_data.language,
                "images": enriched_data.images or [],  # must be list[str]
                "hostname": enriched_data.hostname,
                # JSONB fields need dict/list
                "entities": (
                    enriched_data.entities.model_dump()
                    if enriched_data.entities
                    else None
                ),
                "geo_entities": (
                    [g.model_dump() for g in enriched_data.geo_entities]
                    if enriched_data.geo_entities
                    else None
                ),
            }

            # Update by id + flashpoint_id
            result = (
                self.client.table(table_name)
                .update(article_data)
                .eq("id", enriched_data.id)
                .eq("flashpoint_id", enriched_data.flashpoint_id)
                .execute()
            )

            if result.data:
                self.logger.debug(
                    f"✅ Saved processed article for URL: {enriched_data.url}"
                )
                return True
            else:
                self.logger.warning(
                    f"⚠️ Failed to save processed article for URL: {enriched_data.url}"
                )
                return False

        except Exception as e:
            self.logger.error(f"❌ Failed to save processed article: {e}")
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
                result = (
                    self.client.table("articles")
                    .select("id", count="exact")
                    .eq("status", status)
                    .execute()
                )
                stats[status] = result.count or 0

            return stats

        except Exception as e:
            self.logger.error(f"Failed to get processing stats: {e}")
            return {
                status: 0 for status in ["pending", "processing", "completed", "failed"]
            }

    def is_valid_postgres_url(self, url: str) -> bool:
        return bool(re.match(r"^postgres(?:ql)?://.+:.+@.+:\d+/.+", url))

    def __get_all_rls_policies_cmd(self, table_name: str) -> List[str]:
        """
        Generate all RLS-related SQL commands for the given table:
        - Enables and forces RLS
        - Creates SELECT, INSERT, UPDATE, DELETE policies for 'authenticated' role
        """

        enable_rls_query = f"ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY;"
        force_rls_query = f"ALTER TABLE {table_name} FORCE ROW LEVEL SECURITY;"

        create_select_policy_query = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_policies WHERE policyname = 'allow_select' AND tablename = '{table_name}'
            ) THEN
                EXECUTE format($sql$
                    CREATE POLICY allow_select ON {table_name}
                    FOR SELECT TO anon, authenticated USING (true);
                $sql$);
            END IF;
        END $$;
        """

        create_insert_policy_query = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_policies WHERE policyname = 'allow_insert' AND tablename = '{table_name}'
            ) THEN
                EXECUTE format($sql$
                    CREATE POLICY allow_insert ON {table_name}
                    FOR INSERT TO anon, authenticated WITH CHECK (true);
                $sql$);
            END IF;
        END $$;
        """

        create_update_policy_query = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_policies WHERE policyname = 'allow_update' AND tablename = '{table_name}'
            ) THEN
                EXECUTE format($sql$
                    CREATE POLICY allow_update ON {table_name}
                    FOR UPDATE TO anon, authenticated USING (true) WITH CHECK (true);
                $sql$);
            END IF;
        END $$;
        """

        create_delete_policy_query = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_policies WHERE policyname = 'allow_delete' AND tablename = '{table_name}'
            ) THEN
                EXECUTE format($sql$
                    CREATE POLICY allow_delete ON {table_name}
                    FOR DELETE TO anon, authenticated USING (true);
                $sql$);
            END IF;
        END $$;
        """

        return [
            enable_rls_query,
            force_rls_query,
            create_select_policy_query,
            create_insert_policy_query,
            create_update_policy_query,
            create_delete_policy_query,
        ]


class DatabaseError(Exception):
    """Custom exception for database-related errors."""

    pass


# Global database client instance
db_connection = DatabaseClientAndPool()
