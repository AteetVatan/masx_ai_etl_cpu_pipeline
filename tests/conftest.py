"""
Pytest configuration and fixtures for MASX AI ETL CPU Pipeline tests.

Provides common fixtures and test configuration for all test modules.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Dict, Any, List

from src.config.settings import Settings
from src.db.db_client_and_pool import DatabaseClientAndPool
from src.scraping.scraper import BeautifulSoupExtractor
from src.processing.cleaner import TextCleaner
from src.processing.geotagger import Geotagger
from src.processing.image_finder import ImageFinder
from src.pipeline.pipeline_manager import PipelineManager
from src.utils.threadpool import DynamicThreadPool


@pytest.fixture
def test_settings():
    """Test settings configuration."""
    return Settings(
        supabase_url="https://test.supabase.co",
        supabase_anon_key="test_key",
        supabase_service_role_key="test_service_key",
        db_host="localhost",
        db_name="test_db",
        db_user="test_user",
        db_password="test_password",
        max_workers=4,
        batch_size=10,
        request_timeout=10,
        retry_attempts=2,
        retry_delay=0.1,
        log_level="DEBUG",
        enable_image_search=True,
        enable_geotagging=True,
        clean_text=True,
    )


@pytest.fixture
def mock_db_client():
    """Mock database client for testing."""
    client = AsyncMock(spec=DatabaseClientAndPool)
    client.client = AsyncMock()
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.fetch_articles_batch = AsyncMock(return_value=[])
    client.fetch_article_by_id = AsyncMock(return_value=None)
    client.update_articles_batch = AsyncMock(return_value=(0, 0))
    client.update_article_status = AsyncMock(return_value=True)
    client.get_processing_stats = AsyncMock(
        return_value={"pending": 0, "completed": 0, "failed": 0}
    )
    return client


@pytest.fixture
def mock_scraper():
    """Mock article scraper for testing."""
    scraper = AsyncMock(spec=BeautifulSoupExtractor)
    scraper.scrape_article = AsyncMock(
        return_value={
            "url": "https://example.com/article",
            "title": "Test Article",
            "content": "This is test content for the article.",
            "author": "Test Author",
            "published_date": "2024-01-01",
            "images": [],
            "metadata": {},
            "scraped_at": "2024-01-01T00:00:00Z",
            "word_count": 8,
            "language": "en",
        }
    )
    return scraper


@pytest.fixture
def mock_text_cleaner():
    """Mock text cleaner for testing."""
    cleaner = MagicMock(spec=TextCleaner)
    cleaner.clean_text.return_value = {
        "cleaned_text": "This is cleaned test content.",
        "original_length": 25,
        "cleaned_length": 30,
        "removed_elements": ["whitespace_normalization"],
        "language": "en",
        "cleaning_applied": True,
        "compression_ratio": 1.2,
    }
    return cleaner


@pytest.fixture
def mock_geotagger():
    """Mock geotagger for testing."""
    geotagger = MagicMock(spec=Geotagger)
    geotagger.enabled = True
    geotagger.extract_geographic_entities.return_value = {
        "countries": ["United States", "Canada"],
        "cities": ["New York", "Toronto"],
        "regions": ["North America"],
        "other_locations": [],
        "confidence_scores": {},
        "language": "en",
        "extraction_method": "spacy_ner",
    }
    return geotagger


@pytest.fixture
def mock_image_finder():
    """Mock image finder for testing."""
    finder = AsyncMock(spec=ImageFinder)
    finder.enabled = True
    finder.find_images = AsyncMock(
        return_value={
            "images": [
                {
                    "url": "https://example.com/image1.jpg",
                    "thumbnail_url": "https://example.com/thumb1.jpg",
                    "title": "Test Image 1",
                    "description": "Test image description",
                    "width": 800,
                    "height": 600,
                    "source": "test",
                }
            ],
            "total_found": 1,
            "search_method": "test",
            "query": "test query",
            "language": "en",
        }
    )
    finder.generate_search_queries.return_value = ["test query", "test article"]
    return finder


@pytest.fixture
def mock_thread_pool():
    """Mock thread pool for testing."""
    pool = MagicMock(spec=DynamicThreadPool)
    pool.start = MagicMock()
    pool.shutdown = MagicMock()
    pool.is_healthy.return_value = True
    pool.get_stats.return_value = {
        "pool_status": {"current_workers": 4, "max_workers": 8, "is_running": True},
        "performance": {"total_tasks_completed": 100, "tasks_per_second": 10.5},
    }
    return pool


@pytest.fixture
def sample_article_data():
    """Sample article data for testing."""
    return {
        "id": "test_article_1",
        "url": "https://example.com/article",
        "title": "Test Article Title",
        "content": "This is a test article about technology and innovation.",
        "author": "Test Author",
        "published_date": "2024-01-01",
        "status": "pending",
        "metadata": {"source": "test", "category": "technology"},
    }


@pytest.fixture
def sample_scraped_data():
    """Sample scraped article data for testing."""
    return {
        "url": "https://example.com/article",
        "title": "Test Article Title",
        "content": "This is a test article about technology and innovation in New York.",
        "author": "Test Author",
        "published_date": "2024-01-01",
        "images": [
            {
                "url": "https://example.com/image.jpg",
                "alt": "Test image",
                "title": "Test Image",
            }
        ],
        "metadata": {"source": "test", "category": "technology"},
        "scraped_at": "2024-01-01T00:00:00Z",
        "word_count": 12,
        "language": "en",
    }


@pytest.fixture
def sample_enriched_data():
    """Sample enriched article data for testing."""
    return {
        "url": "https://example.com/article",
        "title": "Test Article Title",
        "content": "This is cleaned test content about technology and innovation in New York.",
        "author": "Test Author",
        "published_date": "2024-01-01",
        "images": [
            {
                "url": "https://example.com/image.jpg",
                "alt": "Test image",
                "title": "Test Image",
                "width": 800,
                "height": 600,
                "source": "test",
            }
        ],
        "metadata": {"source": "test", "category": "technology"},
        "scraped_at": "2024-01-01T00:00:00Z",
        "word_count": 12,
        "language": "en",
        "cleaning_metadata": {
            "original_length": 50,
            "cleaned_length": 45,
            "removed_elements": ["whitespace_normalization"],
            "compression_ratio": 0.9,
        },
        "geographic_entities": {
            "countries": ["United States"],
            "cities": ["New York"],
            "regions": [],
            "other_locations": [],
            "confidence_scores": {},
            "language": "en",
            "extraction_method": "spacy_ner",
        },
        "image_search_metadata": {
            "queries_used": ["test query"],
            "search_method": "test",
            "total_found": 1,
        },
    }


@pytest.fixture
def sample_batch_data():
    """Sample batch data for testing."""
    return [
        {
            "id": "test_article_1",
            "url": "https://example.com/article1",
            "title": "Test Article 1",
            "content": "Test content 1",
            "status": "pending",
        },
        {
            "id": "test_article_2",
            "url": "https://example.com/article2",
            "title": "Test Article 2",
            "content": "Test content 2",
            "status": "pending",
        },
    ]


@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test names."""
    for item in items:
        if "test_" in item.name:
            if "unit" in item.name or "test_" in item.name:
                item.add_marker(pytest.mark.unit)
            if "integration" in item.name:
                item.add_marker(pytest.mark.integration)
            if "slow" in item.name or "performance" in item.name:
                item.add_marker(pytest.mark.slow)
