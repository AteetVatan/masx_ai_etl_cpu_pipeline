"""
Integration tests for the FastAPI server module.

Tests API endpoints, request/response handling, and error scenarios.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import FastAPI

from src.api.server import app
from src.config.settings import Settings


class TestAPIServer:
    """Test cases for FastAPI server endpoints."""
    
    @pytest.fixture
    def client(self):
        """Create a test client for the FastAPI app."""
        return TestClient(app)
    
    @pytest.fixture
    def test_settings(self):
        """Test settings for API testing."""
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
            clean_text=True
        )
    
    def test_root_endpoint(self, client):
        """Test the root endpoint."""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "MASX AI ETL CPU Pipeline API"
        assert data["version"] == "1.0.0"
        assert data["status"] == "operational"
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, client):
        """Test health check endpoint with all components healthy."""
        with patch('src.api.server.pipeline_manager') as mock_pipeline:
            mock_pipeline.health_check.return_value = {
                "overall": "healthy",
                "components": {
                    "thread_pool": {"status": "healthy", "details": "Running"},
                    "database": {"status": "healthy", "details": "Connected"},
                    "scraper": {"status": "healthy", "details": "Available"},
                    "text_cleaner": {"status": "healthy", "details": "Available"},
                    "geotagger": {"status": "healthy", "details": "Available"},
                    "image_finder": {"status": "healthy", "details": "Available"}
                },
                "timestamp": "2024-01-01T00:00:00Z"
            }
            
            response = client.get("/health")
            
            assert response.status_code == 200
            data = response.json()
            assert data["overall"] == "healthy"
            assert data["components"]["thread_pool"]["status"] == "healthy"
            assert data["components"]["database"]["status"] == "healthy"
    
    @pytest.mark.asyncio
    async def test_health_check_failure(self, client):
        """Test health check endpoint with component failure."""
        with patch('src.api.server.pipeline_manager') as mock_pipeline:
            mock_pipeline.health_check.side_effect = Exception("Health check failed")
            
            response = client.get("/health")
            
            assert response.status_code == 500
            data = response.json()
            assert "Health check failed" in data["detail"]
    
    @pytest.mark.asyncio
    async def test_get_stats_success(self, client):
        """Test stats endpoint with successful response."""
        with patch('src.api.server.pipeline_manager') as mock_pipeline:
            mock_pipeline.get_pipeline_stats.return_value = {
                "pipeline_stats": {"total_processed": 100, "successful": 95, "failed": 5},
                "thread_pool_stats": {"current_workers": 4, "max_workers": 8},
                "database_stats": {"pending": 10, "completed": 50, "failed": 5},
                "uptime": 3600.0
            }
            
            response = client.get("/stats")
            
            assert response.status_code == 200
            data = response.json()
            assert data["pipeline_stats"]["total_processed"] == 100
            assert data["thread_pool_stats"]["current_workers"] == 4
            assert data["database_stats"]["pending"] == 10
            assert data["uptime"] == 3600.0
    
    @pytest.mark.asyncio
    async def test_get_stats_failure(self, client):
        """Test stats endpoint with failure."""
        with patch('src.api.server.pipeline_manager') as mock_pipeline:
            mock_pipeline.get_pipeline_stats.side_effect = Exception("Stats failed")
            
            response = client.get("/stats")
            
            assert response.status_code == 500
            data = response.json()
            assert "Stats failed" in data["detail"]
    
    @pytest.mark.asyncio
    async def test_process_article_success(self, client):
        """Test process article endpoint with successful processing."""
        with patch('src.api.server.pipeline_manager') as mock_pipeline:
            mock_pipeline.process_article.return_value = {
                "article_id": "test_article_1",
                "status": "completed",
                "processing_time": 2.5,
                "processing_steps": ["scraping", "cleaning", "geotagging", "image_search"],
                "enriched_data": {
                    "url": "https://example.com/article",
                    "title": "Test Article",
                    "content": "Test content",
                    "geographic_entities": {"countries": ["United States"], "cities": ["New York"]},
                    "images": [{"url": "https://example.com/image.jpg", "title": "Test Image"}]
                },
                "errors": [],
                "timestamp": "2024-01-01T00:00:00Z"
            }
            
            request_data = {
                "url": "https://example.com/article",
                "article_id": "test_article_1",
                "metadata": {"source": "test"}
            }
            
            response = client.post("/process-article", json=request_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["article_id"] == "test_article_1"
            assert data["status"] == "completed"
            assert data["processing_time"] == 2.5
            assert len(data["processing_steps"]) == 4
            assert data["enriched_data"]["title"] == "Test Article"
            assert data["errors"] == []
    
    @pytest.mark.asyncio
    async def test_process_article_scraping_error(self, client):
        """Test process article endpoint with scraping error."""
        with patch('src.api.server.pipeline_manager') as mock_pipeline:
            mock_pipeline.process_article.side_effect = Exception("Scraping failed")
            
            request_data = {
                "url": "https://example.com/article",
                "article_id": "test_article_1"
            }
            
            response = client.post("/process-article", json=request_data)
            
            assert response.status_code == 500
            data = response.json()
            assert "Scraping failed" in data["detail"]
    
    def test_process_article_invalid_url(self, client):
        """Test process article endpoint with invalid URL."""
        request_data = {
            "url": "not-a-valid-url",
            "article_id": "test_article_1"
        }
        
        response = client.post("/process-article", json=request_data)
        
        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "detail" in data
    
    @pytest.mark.asyncio
    async def test_process_batch_success(self, client):
        """Test process batch endpoint with successful processing."""
        with patch('src.api.server.pipeline_manager') as mock_pipeline:
            mock_pipeline.process_batch.return_value = {
                "status": "completed",
                "total_articles": 2,
                "processed": 2,
                "successful": 2,
                "failed": 0,
                "processing_time": 5.0,
                "results": [
                    {
                        "article_id": "test_article_1",
                        "status": "completed",
                        "processing_time": 2.5,
                        "processing_steps": ["scraping", "cleaning", "geotagging", "image_search"],
                        "enriched_data": {"test": "data1"},
                        "errors": [],
                        "timestamp": "2024-01-01T00:00:00Z"
                    },
                    {
                        "article_id": "test_article_2",
                        "status": "completed",
                        "processing_time": 2.5,
                        "processing_steps": ["scraping", "cleaning", "geotagging", "image_search"],
                        "enriched_data": {"test": "data2"},
                        "errors": [],
                        "timestamp": "2024-01-01T00:00:00Z"
                    }
                ]
            }
            
            request_data = {
                "article_ids": ["test_article_1", "test_article_2"],
                "batch_size": 10
            }
            
            response = client.post("/process-batch", json=request_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "completed"
            assert data["total_articles"] == 2
            assert data["processed"] == 2
            assert data["successful"] == 2
            assert data["failed"] == 0
            assert data["processing_time"] == 5.0
            assert len(data["results"]) == 2
    
    @pytest.mark.asyncio
    async def test_process_batch_partial_failure(self, client):
        """Test process batch endpoint with partial failures."""
        with patch('src.api.server.pipeline_manager') as mock_pipeline:
            mock_pipeline.process_batch.return_value = {
                "status": "completed",
                "total_articles": 2,
                "processed": 2,
                "successful": 1,
                "failed": 1,
                "processing_time": 5.0,
                "results": [
                    {
                        "article_id": "test_article_1",
                        "status": "completed",
                        "processing_time": 2.5,
                        "processing_steps": ["scraping", "cleaning", "geotagging", "image_search"],
                        "enriched_data": {"test": "data1"},
                        "errors": [],
                        "timestamp": "2024-01-01T00:00:00Z"
                    },
                    {
                        "article_id": "test_article_2",
                        "status": "failed",
                        "processing_time": 0.5,
                        "processing_steps": ["scraping"],
                        "enriched_data": None,
                        "errors": ["Processing failed"],
                        "timestamp": "2024-01-01T00:00:00Z"
                    }
                ]
            }
            
            request_data = {
                "article_ids": ["test_article_1", "test_article_2"]
            }
            
            response = client.post("/process-batch", json=request_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "completed"
            assert data["successful"] == 1
            assert data["failed"] == 1
            assert len(data["results"]) == 2
    
    def test_process_batch_invalid_request(self, client):
        """Test process batch endpoint with invalid request."""
        request_data = {
            "article_ids": []  # Empty list
        }
        
        response = client.post("/process-batch", json=request_data)
        
        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "detail" in data
    
    def test_process_batch_too_many_articles(self, client):
        """Test process batch endpoint with too many articles."""
        request_data = {
            "article_ids": [f"article_{i}" for i in range(1001)]  # More than 1000
        }
        
        response = client.post("/process-batch", json=request_data)
        
        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "detail" in data
    
    @pytest.mark.asyncio
    async def test_get_article_success(self, client):
        """Test get article endpoint with successful response."""
        with patch('src.api.server.db_client') as mock_db:
            mock_db.client = AsyncMock()
            mock_db.connect.return_value = None
            mock_db.fetch_article_by_id.return_value = {
                "id": "test_article_1",
                "url": "https://example.com/article",
                "title": "Test Article",
                "content": "Test content",
                "status": "completed"
            }
            
            response = client.get("/articles/test_article_1")
            
            assert response.status_code == 200
            data = response.json()
            assert data["id"] == "test_article_1"
            assert data["title"] == "Test Article"
            assert data["status"] == "completed"
    
    @pytest.mark.asyncio
    async def test_get_article_not_found(self, client):
        """Test get article endpoint with article not found."""
        with patch('src.api.server.db_client') as mock_db:
            mock_db.client = AsyncMock()
            mock_db.connect.return_value = None
            mock_db.fetch_article_by_id.return_value = None
            
            response = client.get("/articles/nonexistent_article")
            
            assert response.status_code == 404
            data = response.json()
            assert "Article not found" in data["detail"]
    
    @pytest.mark.asyncio
    async def test_list_articles_success(self, client):
        """Test list articles endpoint with successful response."""
        with patch('src.api.server.db_client') as mock_db:
            mock_db.client = AsyncMock()
            mock_db.connect.return_value = None
            mock_db.fetch_articles_batch.return_value = [
                {"id": "article_1", "title": "Article 1", "status": "pending"},
                {"id": "article_2", "title": "Article 2", "status": "pending"}
            ]
            
            response = client.get("/articles?limit=10&offset=0&status=pending")
            
            assert response.status_code == 200
            data = response.json()
            assert "articles" in data
            assert len(data["articles"]) == 2
            assert data["total"] == 2
            assert data["limit"] == 10
            assert data["offset"] == 0
            assert data["status"] == "pending"
    
    def test_list_articles_invalid_limit(self, client):
        """Test list articles endpoint with invalid limit."""
        response = client.get("/articles?limit=2000")  # Too high
        
        assert response.status_code == 400
        data = response.json()
        assert "Maximum limit is 1000" in data["detail"]
    
    @pytest.mark.asyncio
    async def test_update_article_status_success(self, client):
        """Test update article status endpoint with successful response."""
        with patch('src.api.server.db_client') as mock_db:
            mock_db.client = AsyncMock()
            mock_db.connect.return_value = None
            mock_db.update_article_status.return_value = True
            
            response = client.post("/articles/test_article_1/status?status=completed")
            
            assert response.status_code == 200
            data = response.json()
            assert "Article test_article_1 status updated to completed" in data["message"]
    
    @pytest.mark.asyncio
    async def test_update_article_status_not_found(self, client):
        """Test update article status endpoint with article not found."""
        with patch('src.api.server.db_client') as mock_db:
            mock_db.client = AsyncMock()
            mock_db.connect.return_value = None
            mock_db.update_article_status.return_value = False
            
            response = client.post("/articles/nonexistent_article/status?status=completed")
            
            assert response.status_code == 404
            data = response.json()
            assert "Article not found or update failed" in data["detail"]
    
    def test_update_article_status_invalid_status(self, client):
        """Test update article status endpoint with invalid status."""
        response = client.post("/articles/test_article_1/status?status=invalid_status")
        
        assert response.status_code == 400
        data = response.json()
        assert "Invalid status" in data["detail"]
    
    @pytest.mark.asyncio
    async def test_test_text_cleaner_success(self, client):
        """Test text cleaner test endpoint with successful response."""
        with patch('src.api.server.text_cleaner') as mock_cleaner:
            mock_cleaner.clean_text.return_value = {
                "cleaned_text": "This is cleaned text",
                "original_length": 20,
                "cleaned_length": 18,
                "removed_elements": ["whitespace_normalization"],
                "language": "en",
                "cleaning_applied": True,
                "compression_ratio": 0.9
            }
            
            response = client.get("/text-cleaner/test?text=This%20is%20test%20text&language=en")
            
            assert response.status_code == 200
            data = response.json()
            assert data["cleaned_text"] == "This is cleaned text"
            assert data["original_length"] == 20
            assert data["cleaned_length"] == 18
            assert data["language"] == "en"
    
    @pytest.mark.asyncio
    async def test_test_geotagger_success(self, client):
        """Test geotagger test endpoint with successful response."""
        with patch('src.api.server.geotagger') as mock_geotagger:
            mock_geotagger.extract_geographic_entities.return_value = {
                "countries": ["United States"],
                "cities": ["New York"],
                "regions": [],
                "other_locations": [],
                "confidence_scores": {},
                "language": "en",
                "extraction_method": "spacy_ner"
            }
            
            response = client.get("/geotagger/test?text=New%20York%20is%20in%20the%20United%20States&language=en")
            
            assert response.status_code == 200
            data = response.json()
            assert "United States" in data["countries"]
            assert "New York" in data["cities"]
            assert data["language"] == "en"
    
    @pytest.mark.asyncio
    async def test_test_image_finder_success(self, client):
        """Test image finder test endpoint with successful response."""
        with patch('src.api.server.image_finder') as mock_finder:
            mock_finder.find_images.return_value = {
                "images": [
                    {
                        "url": "https://example.com/image.jpg",
                        "title": "Test Image",
                        "width": 800,
                        "height": 600
                    }
                ],
                "total_found": 1,
                "search_method": "test",
                "query": "test query",
                "language": "en"
            }
            
            response = client.get("/image-finder/test?query=test%20query&max_images=3&language=en")
            
            assert response.status_code == 200
            data = response.json()
            assert len(data["images"]) == 1
            assert data["images"][0]["url"] == "https://example.com/image.jpg"
            assert data["total_found"] == 1
            assert data["search_method"] == "test"
    
    def test_global_exception_handler(self, client):
        """Test global exception handler for unhandled errors."""
        with patch('src.api.server.pipeline_manager') as mock_pipeline:
            mock_pipeline.health_check.side_effect = Exception("Unexpected error")
            
            response = client.get("/health")
            
            assert response.status_code == 500
            data = response.json()
            assert data["detail"] == "Internal server error"
            assert data["type"] == "internal_error"
