"""
Unit tests for the pipeline manager module.

Tests the complete pipeline orchestration, batch processing, and error handling.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from src.pipeline.pipeline_manager import PipelineManager


class TestPipelineManager:
    """Test cases for PipelineManager class."""

    @pytest.fixture
    def pipeline_manager(self):
        """Create a pipeline manager instance for testing."""
        return PipelineManager()

    def test_init(self, pipeline_manager):
        """Test pipeline manager initialization."""
        assert pipeline_manager.batch_size == 100  # Default batch size
        assert pipeline_manager.max_workers == 32  # Default max workers
        assert pipeline_manager.retry_attempts == 3  # Default retries
        assert pipeline_manager.retry_delay == 1.0  # Default delay
        assert pipeline_manager.stats["total_processed"] == 0
        assert pipeline_manager.stats["successful"] == 0
        assert pipeline_manager.stats["failed"] == 0

    @pytest.mark.asyncio
    async def test_process_article_success(
        self, pipeline_manager, sample_article_data, sample_enriched_data
    ):
        """Test successful article processing."""
        with patch.object(pipeline_manager, "_scrape_article") as mock_scrape:
            with patch.object(pipeline_manager, "_clean_text") as mock_clean:
                with patch.object(pipeline_manager, "_geotag_article") as mock_geotag:
                    with patch.object(
                        pipeline_manager, "_find_images"
                    ) as mock_find_images:
                        # Mock the pipeline steps
                        mock_scrape.return_value = sample_article_data
                        mock_clean.return_value = sample_article_data
                        mock_geotag.return_value = sample_article_data
                        mock_find_images.return_value = sample_enriched_data

                        result = await pipeline_manager.process_article(
                            sample_article_data
                        )

                        assert result["article_id"] == sample_article_data["id"]
                        assert result["status"] == "completed"
                        assert result["processing_time"] > 0
                        assert "scraping" in result["processing_steps"]
                        assert "cleaning" in result["processing_steps"]
                        assert "geotagging" in result["processing_steps"]
                        assert "image_search" in result["processing_steps"]
                        assert result["enriched_data"] == sample_enriched_data
                        assert result["errors"] == []

                        # Check statistics
                        assert pipeline_manager.stats["total_processed"] == 1
                        assert pipeline_manager.stats["successful"] == 1
                        assert pipeline_manager.stats["failed"] == 0

    @pytest.mark.asyncio
    async def test_process_article_scraping_error(
        self, pipeline_manager, sample_article_data
    ):
        """Test article processing with scraping error."""
        with patch.object(pipeline_manager, "_scrape_article") as mock_scrape:
            mock_scrape.side_effect = Exception("Scraping failed")

            result = await pipeline_manager.process_article(sample_article_data)

            assert result["article_id"] == sample_article_data["id"]
            assert result["status"] == "failed"
            assert result["processing_time"] > 0
            assert result["enriched_data"] is None
            assert len(result["errors"]) > 0
            assert "Scraping failed" in result["errors"][0]

            # Check statistics
            assert pipeline_manager.stats["total_processed"] == 1
            assert pipeline_manager.stats["successful"] == 0
            assert pipeline_manager.stats["failed"] == 1

    @pytest.mark.asyncio
    async def test_process_article_cleaning_error(
        self, pipeline_manager, sample_article_data
    ):
        """Test article processing with cleaning error."""
        with patch.object(pipeline_manager, "_scrape_article") as mock_scrape:
            with patch.object(pipeline_manager, "_clean_text") as mock_clean:
                mock_scrape.return_value = sample_article_data
                mock_clean.side_effect = Exception("Cleaning failed")

                result = await pipeline_manager.process_article(sample_article_data)

                assert result["status"] == "failed"
                assert "Cleaning failed" in result["errors"][0]
                assert "scraping" in result["processing_steps"]
                assert "cleaning" not in result["processing_steps"]

    @pytest.mark.asyncio
    async def test_process_article_geotagging_error(
        self, pipeline_manager, sample_article_data
    ):
        """Test article processing with geotagging error."""
        with patch.object(pipeline_manager, "_scrape_article") as mock_scrape:
            with patch.object(pipeline_manager, "_clean_text") as mock_clean:
                with patch.object(pipeline_manager, "_geotag_article") as mock_geotag:
                    mock_scrape.return_value = sample_article_data
                    mock_clean.return_value = sample_article_data
                    mock_geotag.side_effect = Exception("Geotagging failed")

                    result = await pipeline_manager.process_article(sample_article_data)

                    assert result["status"] == "failed"
                    assert "Geotagging failed" in result["errors"][0]
                    assert "scraping" in result["processing_steps"]
                    assert "cleaning" in result["processing_steps"]
                    assert "geotagging" not in result["processing_steps"]

    @pytest.mark.asyncio
    async def test_process_article_image_search_error(
        self, pipeline_manager, sample_article_data
    ):
        """Test article processing with image search error."""
        with patch.object(pipeline_manager, "_scrape_article") as mock_scrape:
            with patch.object(pipeline_manager, "_clean_text") as mock_clean:
                with patch.object(pipeline_manager, "_geotag_article") as mock_geotag:
                    with patch.object(
                        pipeline_manager, "_find_images"
                    ) as mock_find_images:
                        mock_scrape.return_value = sample_article_data
                        mock_clean.return_value = sample_article_data
                        mock_geotag.return_value = sample_article_data
                        mock_find_images.side_effect = Exception("Image search failed")

                        result = await pipeline_manager.process_article(
                            sample_article_data
                        )

                        assert result["status"] == "failed"
                        assert "Image search failed" in result["errors"][0]
                        assert "scraping" in result["processing_steps"]
                        assert "cleaning" in result["processing_steps"]
                        assert "geotagging" in result["processing_steps"]
                        assert "image_search" not in result["processing_steps"]

    @pytest.mark.asyncio
    async def test_process_batch_success(self, pipeline_manager, sample_batch_data):
        """Test successful batch processing."""
        with patch.object(pipeline_manager, "_fetch_articles_batch") as mock_fetch:
            with patch.object(
                pipeline_manager, "_update_articles_batch"
            ) as mock_update:
                with patch.object(pipeline_manager, "process_article") as mock_process:
                    # Mock batch processing
                    mock_fetch.return_value = sample_batch_data
                    mock_update.return_value = None

                    # Mock individual article processing
                    mock_process.side_effect = [
                        {
                            "article_id": "test_article_1",
                            "status": "completed",
                            "processing_time": 1.0,
                            "processing_steps": [
                                "scraping",
                                "cleaning",
                                "geotagging",
                                "image_search",
                            ],
                            "enriched_data": {"test": "data1"},
                            "errors": [],
                            "timestamp": "2024-01-01T00:00:00Z",
                        },
                        {
                            "article_id": "test_article_2",
                            "status": "completed",
                            "processing_time": 1.5,
                            "processing_steps": [
                                "scraping",
                                "cleaning",
                                "geotagging",
                                "image_search",
                            ],
                            "enriched_data": {"test": "data2"},
                            "errors": [],
                            "timestamp": "2024-01-01T00:00:00Z",
                        },
                    ]

                    result = await pipeline_manager.process_batch(
                        ["test_article_1", "test_article_2"]
                    )

                    assert result["status"] == "completed"
                    assert result["total_articles"] == 2
                    assert result["processed"] == 2
                    assert result["successful"] == 2
                    assert result["failed"] == 0
                    assert result["processing_time"] > 0
                    assert len(result["results"]) == 2

    @pytest.mark.asyncio
    async def test_process_batch_partial_failure(
        self, pipeline_manager, sample_batch_data
    ):
        """Test batch processing with partial failures."""
        with patch.object(pipeline_manager, "_fetch_articles_batch") as mock_fetch:
            with patch.object(
                pipeline_manager, "_update_articles_batch"
            ) as mock_update:
                with patch.object(pipeline_manager, "process_article") as mock_process:
                    # Mock batch processing
                    mock_fetch.return_value = sample_batch_data
                    mock_update.return_value = None

                    # Mock individual article processing with one failure
                    mock_process.side_effect = [
                        {
                            "article_id": "test_article_1",
                            "status": "completed",
                            "processing_time": 1.0,
                            "processing_steps": [
                                "scraping",
                                "cleaning",
                                "geotagging",
                                "image_search",
                            ],
                            "enriched_data": {"test": "data1"},
                            "errors": [],
                            "timestamp": "2024-01-01T00:00:00Z",
                        },
                        {
                            "article_id": "test_article_2",
                            "status": "failed",
                            "processing_time": 0.5,
                            "processing_steps": ["scraping"],
                            "enriched_data": None,
                            "errors": ["Processing failed"],
                            "timestamp": "2024-01-01T00:00:00Z",
                        },
                    ]

                    result = await pipeline_manager.process_batch(
                        ["test_article_1", "test_article_2"]
                    )

                    assert result["status"] == "completed"
                    assert result["total_articles"] == 2
                    assert result["processed"] == 2
                    assert result["successful"] == 1
                    assert result["failed"] == 1
                    assert len(result["results"]) == 2

    @pytest.mark.asyncio
    async def test_process_batch_no_articles(self, pipeline_manager):
        """Test batch processing with no articles found."""
        with patch.object(pipeline_manager, "_fetch_articles_batch") as mock_fetch:
            mock_fetch.return_value = []

            result = await pipeline_manager.process_batch(["nonexistent_article"])

            assert result["status"] == "completed"
            assert result["total_articles"] == 1
            assert result["processed"] == 0
            assert result["successful"] == 0
            assert result["failed"] == 0
            assert result["results"] == []

    @pytest.mark.asyncio
    async def test_process_batch_fetch_error(self, pipeline_manager):
        """Test batch processing with fetch error."""
        with patch.object(pipeline_manager, "_fetch_articles_batch") as mock_fetch:
            mock_fetch.side_effect = Exception("Database error")

            result = await pipeline_manager.process_batch(["test_article"])

            assert result["status"] == "failed"
            assert "Database error" in result["error"]
            assert result["total_articles"] == 1
            assert result["processed"] == 0
            assert result["successful"] == 0
            assert result["failed"] == 0

    @pytest.mark.asyncio
    async def test_scrape_article_primary_success(self, pipeline_manager):
        """Test article scraping with primary scraper success."""
        with patch("src.pipeline.pipeline_manager.scraper") as mock_scraper:
            mock_scraper.scrape_article.return_value = {
                "url": "test",
                "content": "test content",
            }

            result = await pipeline_manager._scrape_article("https://example.com")

            assert result == {"url": "test", "content": "test content"}
            mock_scraper.scrape_article.assert_called_once_with("https://example.com")

    @pytest.mark.asyncio
    async def test_scrape_article_fallback_success(self, pipeline_manager):
        """Test article scraping with fallback scraper success."""
        with patch("src.pipeline.pipeline_manager.scraper") as mock_scraper:
            with patch(
                "src.pipeline.pipeline_manager.fallback_scraper"
            ) as mock_fallback:
                mock_scraper.scrape_article.side_effect = Exception("Primary failed")
                mock_fallback.scrape_article.return_value = {
                    "url": "test",
                    "content": "test content",
                }

                result = await pipeline_manager._scrape_article("https://example.com")

                assert result == {"url": "test", "content": "test content"}
                mock_scraper.scrape_article.assert_called_once_with(
                    "https://example.com"
                )
                mock_fallback.scrape_article.assert_called_once_with(
                    "https://example.com"
                )

    @pytest.mark.asyncio
    async def test_scrape_article_both_fail(self, pipeline_manager):
        """Test article scraping when both scrapers fail."""
        with patch("src.pipeline.pipeline_manager.scraper") as mock_scraper:
            with patch(
                "src.pipeline.pipeline_manager.fallback_scraper"
            ) as mock_fallback:
                mock_scraper.scrape_article.side_effect = Exception("Primary failed")
                mock_fallback.scrape_article.side_effect = Exception("Fallback failed")

                with pytest.raises(Exception, match="Both scrapers failed"):
                    await pipeline_manager._scrape_article("https://example.com")

    @pytest.mark.asyncio
    async def test_clean_text(self, pipeline_manager, sample_article_data):
        """Test text cleaning."""
        with patch("src.pipeline.pipeline_manager.text_cleaner") as mock_cleaner:
            mock_cleaner.clean_text.return_value = {
                "cleaned_text": "cleaned content",
                "original_length": 100,
                "cleaned_length": 90,
                "removed_elements": ["whitespace"],
                "compression_ratio": 0.9,
            }

            result = await pipeline_manager._clean_text(sample_article_data)

            assert result["content"] == "cleaned content"
            assert "cleaning_metadata" in result
            assert result["cleaning_metadata"]["original_length"] == 100
            assert result["cleaning_metadata"]["cleaned_length"] == 90
            assert result["cleaning_metadata"]["removed_elements"] == ["whitespace"]
            assert result["cleaning_metadata"]["compression_ratio"] == 0.9

    @pytest.mark.asyncio
    async def test_geotag_article(self, pipeline_manager, sample_article_data):
        """Test article geotagging."""
        with patch("src.pipeline.pipeline_manager.geotagger") as mock_geotagger:
            mock_geotagger.extract_geographic_entities.return_value = {
                "countries": ["United States"],
                "cities": ["New York"],
                "regions": [],
                "other_locations": [],
                "confidence_scores": {},
                "language": "en",
                "extraction_method": "spacy_ner",
            }

            result = await pipeline_manager._geotag_article(sample_article_data)

            assert "geographic_entities" in result
            assert result["geographic_entities"]["countries"] == ["United States"]
            assert result["geographic_entities"]["cities"] == ["New York"]

    @pytest.mark.asyncio
    async def test_find_images(self, pipeline_manager, sample_article_data):
        """Test image finding."""
        with patch("src.pipeline.pipeline_manager.image_finder") as mock_finder:
            mock_finder.generate_search_queries.return_value = [
                "test query",
                "test article",
            ]
            mock_finder.find_images.return_value = {
                "images": [
                    {"url": "https://example.com/image.jpg", "title": "Test Image"}
                ],
                "total_found": 1,
                "search_method": "test",
            }

            result = await pipeline_manager._find_images(sample_article_data)

            assert "images" in result
            assert len(result["images"]) == 1
            assert result["images"][0]["url"] == "https://example.com/image.jpg"
            assert "image_search_metadata" in result
            assert result["image_search_metadata"]["queries_used"] == [
                "test query",
                "test article",
            ]
            assert result["image_search_metadata"]["search_method"] == "test"
            assert result["image_search_metadata"]["total_found"] == 1

    @pytest.mark.asyncio
    async def test_fetch_articles_batch(self, pipeline_manager, sample_batch_data):
        """Test fetching articles batch."""
        with patch("src.pipeline.pipeline_manager.db_client") as mock_db:
            mock_db.client = AsyncMock()
            mock_db.connect.return_value = None
            mock_db.fetch_article_by_id.side_effect = [
                sample_batch_data[0],  # First article found
                sample_batch_data[1],  # Second article found
                None,  # Third article not found
            ]

            result = await pipeline_manager._fetch_articles_batch(
                ["article1", "article2", "article3"]
            )

            assert len(result) == 2  # Only found 2 articles
            assert result[0]["id"] == "test_article_1"
            assert result[1]["id"] == "test_article_2"

    @pytest.mark.asyncio
    async def test_update_articles_batch(self, pipeline_manager):
        """Test updating articles batch."""
        with patch("src.pipeline.pipeline_manager.db_client") as mock_db:
            mock_db.client = AsyncMock()
            mock_db.connect.return_value = None
            mock_db.update_articles_batch.return_value = (
                2,
                0,
            )  # 2 successful, 0 failed

            results = [
                {
                    "article_id": "test_article_1",
                    "status": "completed",
                    "enriched_data": {"test": "data1"},
                    "processing_time": 1.0,
                    "processing_steps": ["scraping", "cleaning"],
                },
                {
                    "article_id": "test_article_2",
                    "status": "failed",
                    "error_message": "Processing failed",
                    "processing_time": 0.5,
                },
            ]

            await pipeline_manager._update_articles_batch(results)

            mock_db.update_articles_batch.assert_called_once()
            call_args = mock_db.update_articles_batch.call_args[0][0]
            assert len(call_args) == 2
            assert call_args[0]["id"] == "test_article_1"
            assert call_args[0]["status"] == "completed"
            assert call_args[1]["id"] == "test_article_2"
            assert call_args[1]["status"] == "failed"

    @pytest.mark.asyncio
    async def test_get_pipeline_stats(self, pipeline_manager):
        """Test getting pipeline statistics."""
        with patch("src.pipeline.pipeline_manager.thread_pool") as mock_thread_pool:
            with patch.object(pipeline_manager, "_get_database_stats") as mock_db_stats:
                mock_thread_pool.get_stats.return_value = {"test": "thread_pool_stats"}
                mock_db_stats.return_value = {"test": "database_stats"}

                result = await pipeline_manager.get_pipeline_stats()

                assert "pipeline_stats" in result
                assert "thread_pool_stats" in result
                assert "database_stats" in result
                assert "uptime" in result
                assert result["thread_pool_stats"] == {"test": "thread_pool_stats"}
                assert result["database_stats"] == {"test": "database_stats"}

    @pytest.mark.asyncio
    async def test_get_database_stats(self, pipeline_manager):
        """Test getting database statistics."""
        with patch("src.pipeline.pipeline_manager.db_client") as mock_db:
            mock_db.client = AsyncMock()
            mock_db.connect.return_value = None
            mock_db.get_processing_stats.return_value = {
                "pending": 10,
                "completed": 50,
                "failed": 5,
            }

            result = await pipeline_manager._get_database_stats()

            assert result == {"pending": 10, "completed": 50, "failed": 5}

    @pytest.mark.asyncio
    async def test_get_database_stats_error(self, pipeline_manager):
        """Test getting database statistics with error."""
        with patch("src.pipeline.pipeline_manager.db_client") as mock_db:
            mock_db.client = AsyncMock()
            mock_db.connect.return_value = None
            mock_db.get_processing_stats.side_effect = Exception("Database error")

            result = await pipeline_manager._get_database_stats()

            assert "error" in result
            assert "Database error" in result["error"]

    @pytest.mark.asyncio
    async def test_health_check(self, pipeline_manager):
        """Test health check functionality."""
        with patch("src.pipeline.pipeline_manager.thread_pool") as mock_thread_pool:
            with patch("src.pipeline.pipeline_manager.db_client") as mock_db:
                with patch("src.pipeline.pipeline_manager.geotagger") as mock_geotagger:
                    with patch(
                        "src.pipeline.pipeline_manager.image_finder"
                    ) as mock_image_finder:
                        # Mock all components
                        mock_thread_pool.is_healthy.return_value = True
                        mock_thread_pool.get_stats.return_value = {"test": "stats"}
                        mock_db.client = AsyncMock()
                        mock_db.connect.return_value = None
                        mock_db._test_connection.return_value = None
                        mock_geotagger.enabled = True
                        mock_image_finder.enabled = True

                        result = await pipeline_manager.health_check()

                        assert result["overall"] == "healthy"
                        assert "components" in result
                        assert (
                            result["components"]["thread_pool"]["status"] == "healthy"
                        )
                        assert result["components"]["database"]["status"] == "healthy"
                        assert result["components"]["geotagger"]["status"] == "healthy"
                        assert (
                            result["components"]["image_finder"]["status"] == "healthy"
                        )

    @pytest.mark.asyncio
    async def test_health_check_database_error(self, pipeline_manager):
        """Test health check with database error."""
        with patch("src.pipeline.pipeline_manager.thread_pool") as mock_thread_pool:
            with patch("src.pipeline.pipeline_manager.db_client") as mock_db:
                mock_thread_pool.is_healthy.return_value = True
                mock_thread_pool.get_stats.return_value = {"test": "stats"}
                mock_db.client = None
                mock_db.connect.side_effect = Exception("Connection failed")

                result = await pipeline_manager.health_check()

                assert result["overall"] == "unhealthy"
                assert result["components"]["database"]["status"] == "unhealthy"
                assert (
                    "Connection failed" in result["components"]["database"]["details"]
                )

    @pytest.mark.asyncio
    async def test_shutdown(self, pipeline_manager):
        """Test pipeline manager shutdown."""
        with patch("src.pipeline.pipeline_manager.thread_pool") as mock_thread_pool:
            with patch("src.pipeline.pipeline_manager.db_client") as mock_db:
                mock_thread_pool.shutdown.return_value = None
                mock_db.disconnect.return_value = None

                await pipeline_manager.shutdown()

                mock_thread_pool.shutdown.assert_called_once_with(wait=True, timeout=30)
                mock_db.disconnect.assert_called_once()
