"""
Unit tests for the image finder module.

Tests image search functionality, quality filtering, and API integration.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from src.processing.image_finder import ImageFinder


class TestImageFinder:
    """Test cases for ImageFinder class."""
    
    @pytest.fixture
    def image_finder(self):
        """Create an image finder instance for testing."""
        return ImageFinder()
    
    def test_init(self, image_finder):
        """Test image finder initialization."""
        assert image_finder.enabled == True  # Should be enabled by default
        assert image_finder.min_width == 300
        assert image_finder.min_height == 200
        assert image_finder.max_width == 4000
        assert image_finder.max_height == 4000
        assert image_finder.max_file_size == 5 * 1024 * 1024  # 5MB
        assert len(image_finder.supported_formats) > 0
    
    def test_is_high_quality_image_valid(self, image_finder):
        """Test quality validation for valid images."""
        image_data = {
            "url": "https://example.com/image.jpg",
            "width": 800,
            "height": 600,
            "format": "jpg",
            "file_size": "1.2 MB"
        }
        
        assert image_finder._is_high_quality_image(image_data) == True
    
    def test_is_high_quality_image_too_small(self, image_finder):
        """Test quality validation for images that are too small."""
        image_data = {
            "url": "https://example.com/image.jpg",
            "width": 200,  # Too small
            "height": 150,  # Too small
            "format": "jpg"
        }
        
        assert image_finder._is_high_quality_image(image_data) == False
    
    def test_is_high_quality_image_too_large(self, image_finder):
        """Test quality validation for images that are too large."""
        image_data = {
            "url": "https://example.com/image.jpg",
            "width": 5000,  # Too large
            "height": 4000,  # Too large
            "format": "jpg"
        }
        
        assert image_finder._is_high_quality_image(image_data) == False
    
    def test_is_high_quality_image_bad_aspect_ratio(self, image_finder):
        """Test quality validation for images with bad aspect ratio."""
        image_data = {
            "url": "https://example.com/image.jpg",
            "width": 1000,  # Very wide
            "height": 100,   # Very tall
            "format": "jpg"
        }
        
        assert image_finder._is_high_quality_image(image_data) == False
    
    def test_is_high_quality_image_unsupported_format(self, image_finder):
        """Test quality validation for unsupported formats."""
        image_data = {
            "url": "https://example.com/image.bmp",
            "width": 800,
            "height": 600,
            "format": "bmp"  # Unsupported
        }
        
        assert image_finder._is_high_quality_image(image_data) == False
    
    def test_is_high_quality_image_invalid_url(self, image_finder):
        """Test quality validation for invalid URLs."""
        image_data = {
            "url": "not-a-url",
            "width": 800,
            "height": 600,
            "format": "jpg"
        }
        
        assert image_finder._is_high_quality_image(image_data) == False
    
    def test_is_high_quality_image_large_file_size(self, image_finder):
        """Test quality validation for large file sizes."""
        image_data = {
            "url": "https://example.com/image.jpg",
            "width": 800,
            "height": 600,
            "format": "jpg",
            "file_size": "10 MB"  # Too large
        }
        
        assert image_finder._is_high_quality_image(image_data) == False
    
    def test_process_bing_image(self, image_finder):
        """Test processing of Bing search result."""
        bing_item = {
            "contentUrl": "https://example.com/image.jpg",
            "thumbnailUrl": "https://example.com/thumb.jpg",
            "name": "Test Image",
            "description": "Test description",
            "width": 800,
            "height": 600,
            "contentSize": "1.2 MB",
            "encodingFormat": "jpeg",
            "hostPageUrl": "https://example.com/page",
            "datePublished": "2024-01-01",
            "creator": {"name": "Test Creator"}
        }
        
        result = image_finder._process_bing_image(bing_item)
        
        assert result is not None
        assert result["url"] == "https://example.com/image.jpg"
        assert result["thumbnail_url"] == "https://example.com/thumb.jpg"
        assert result["title"] == "Test Image"
        assert result["description"] == "Test description"
        assert result["width"] == 800
        assert result["height"] == 600
        assert result["file_size"] == "1.2 MB"
        assert result["format"] == "jpeg"
        assert result["source"] == "bing"
        assert result["source_url"] == "https://example.com/page"
        assert result["date_published"] == "2024-01-01"
        assert result["creator"] == "Test Creator"
    
    def test_process_bing_image_invalid(self, image_finder):
        """Test processing of invalid Bing search result."""
        bing_item = {}  # Empty item
        
        result = image_finder._process_bing_image(bing_item)
        
        assert result is None
    
    def test_process_duckduckgo_image(self, image_finder):
        """Test processing of DuckDuckGo search result."""
        ddg_item = {
            "image": "https://example.com/image.jpg",
            "thumbnail": "https://example.com/thumb.jpg",
            "title": "Test Image",
            "description": "Test description",
            "width": 800,
            "height": 600,
            "size": "1.2 MB",
            "format": "jpeg",
            "url": "https://example.com/page",
            "date": "2024-01-01",
            "creator": "Test Creator"
        }
        
        result = image_finder._process_duckduckgo_image(ddg_item)
        
        assert result is not None
        assert result["url"] == "https://example.com/image.jpg"
        assert result["thumbnail_url"] == "https://example.com/thumb.jpg"
        assert result["title"] == "Test Image"
        assert result["description"] == "Test description"
        assert result["width"] == 800
        assert result["height"] == 600
        assert result["file_size"] == "1.2 MB"
        assert result["format"] == "jpeg"
        assert result["source"] == "duckduckgo"
        assert result["source_url"] == "https://example.com/page"
        assert result["date_published"] == "2024-01-01"
        assert result["creator"] == "Test Creator"
    
    def test_process_duckduckgo_image_invalid(self, image_finder):
        """Test processing of invalid DuckDuckGo search result."""
        ddg_item = {}  # Empty item
        
        result = image_finder._process_duckduckgo_image(ddg_item)
        
        assert result is None
    
    def test_generate_search_queries(self, image_finder):
        """Test search query generation."""
        title = "Test Article Title"
        content = "This is a test article about technology and innovation in New York City."
        
        queries = image_finder.generate_search_queries(title, content, "en")
        
        assert len(queries) > 0
        assert title in queries  # Title should be included
        assert "technology" in queries  # Keywords should be included
        assert "innovation" in queries
        assert "New York City" in queries
    
    def test_generate_search_queries_empty_input(self, image_finder):
        """Test search query generation with empty input."""
        queries = image_finder.generate_search_queries("", "", "en")
        
        assert len(queries) == 0
    
    def test_generate_search_queries_no_title(self, image_finder):
        """Test search query generation without title."""
        content = "This is a test article about technology and innovation."
        
        queries = image_finder.generate_search_queries("", content, "en")
        
        assert len(queries) > 0
        assert "technology" in queries
        assert "innovation" in queries
    
    def test_generate_search_queries_no_content(self, image_finder):
        """Test search query generation without content."""
        title = "Test Article Title"
        
        queries = image_finder.generate_search_queries(title, "", "en")
        
        assert len(queries) > 0
        assert title in queries
    
    def test_extract_keywords(self, image_finder):
        """Test keyword extraction from content."""
        content = "This is a test article about technology and innovation. Technology is important for the future. Innovation drives progress."
        
        keywords = image_finder._extract_keywords(content)
        
        assert len(keywords) > 0
        assert "technology" in keywords
        assert "innovation" in keywords
        assert "article" in keywords
        # Should not include common stop words
        assert "this" not in keywords
        assert "is" not in keywords
        assert "a" not in keywords
    
    def test_extract_keywords_empty(self, image_finder):
        """Test keyword extraction from empty content."""
        keywords = image_finder._extract_keywords("")
        
        assert len(keywords) == 0
    
    def test_extract_keywords_short(self, image_finder):
        """Test keyword extraction from short content."""
        keywords = image_finder._extract_keywords("Short text")
        
        assert len(keywords) == 0  # Too short for meaningful keywords
    
    @pytest.mark.asyncio
    async def test_find_images_disabled(self, image_finder):
        """Test image finding when disabled."""
        image_finder.enabled = False
        
        result = await image_finder.find_images("test query", 5, "en")
        
        assert result["images"] == []
        assert result["total_found"] == 0
        assert result["search_method"] == "disabled"
    
    @pytest.mark.asyncio
    async def test_find_images_empty_query(self, image_finder):
        """Test image finding with empty query."""
        result = await image_finder.find_images("", 5, "en")
        
        assert result["images"] == []
        assert result["total_found"] == 0
        assert result["search_method"] == "disabled"
    
    @pytest.mark.asyncio
    async def test_find_images_bing_success(self, image_finder):
        """Test successful Bing image search."""
        with patch.object(image_finder, '_search_bing') as mock_search:
            mock_search.return_value = [
                {
                    "url": "https://example.com/image1.jpg",
                    "title": "Test Image 1",
                    "width": 800,
                    "height": 600
                }
            ]
            
            result = await image_finder.find_images("test query", 5, "en")
            
            assert len(result["images"]) == 1
            assert result["total_found"] == 1
            assert result["search_method"] == "bing"
            assert result["images"][0]["url"] == "https://example.com/image1.jpg"
    
    @pytest.mark.asyncio
    async def test_find_images_bing_fallback(self, image_finder):
        """Test fallback from Bing to DuckDuckGo."""
        with patch.object(image_finder, '_search_bing') as mock_bing:
            with patch.object(image_finder, '_search_duckduckgo') as mock_ddg:
                mock_bing.side_effect = Exception("Bing failed")
                mock_ddg.return_value = [
                    {
                        "url": "https://example.com/image1.jpg",
                        "title": "Test Image 1",
                        "width": 800,
                        "height": 600
                    }
                ]
                
                result = await image_finder.find_images("test query", 5, "en")
                
                assert len(result["images"]) == 1
                assert result["total_found"] == 1
                assert result["search_method"] == "duckduckgo"
    
    @pytest.mark.asyncio
    async def test_find_images_all_fail(self, image_finder):
        """Test when all search methods fail."""
        with patch.object(image_finder, '_search_bing') as mock_bing:
            with patch.object(image_finder, '_search_duckduckgo') as mock_ddg:
                with patch.object(image_finder, '_search_fallback') as mock_fallback:
                    mock_bing.side_effect = Exception("Bing failed")
                    mock_ddg.side_effect = Exception("DuckDuckGo failed")
                    mock_fallback.return_value = []
                    
                    result = await image_finder.find_images("test query", 5, "en")
                    
                    assert result["images"] == []
                    assert result["total_found"] == 0
                    assert result["search_method"] == "none"
    
    @pytest.mark.asyncio
    async def test_search_bing_success(self, image_finder):
        """Test successful Bing search."""
        mock_response_data = {
            "value": [
                {
                    "contentUrl": "https://example.com/image1.jpg",
                    "thumbnailUrl": "https://example.com/thumb1.jpg",
                    "name": "Test Image 1",
                    "description": "Test description 1",
                    "width": 800,
                    "height": 600,
                    "contentSize": "1.2 MB",
                    "encodingFormat": "jpeg"
                }
            ]
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.json.return_value = mock_response_data
            mock_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await image_finder._search_bing("test query", 5, "en")
            
            assert len(result) == 1
            assert result[0]["url"] == "https://example.com/image1.jpg"
            assert result[0]["source"] == "bing"
    
    @pytest.mark.asyncio
    async def test_search_bing_no_api_key(self, image_finder):
        """Test Bing search without API key."""
        image_finder.bing_api_key = None
        
        with pytest.raises(Exception, match="Bing API key not configured"):
            await image_finder._search_bing("test query", 5, "en")
    
    @pytest.mark.asyncio
    async def test_search_duckduckgo_success(self, image_finder):
        """Test successful DuckDuckGo search."""
        mock_response_data = {
            "results": [
                {
                    "image": "https://example.com/image1.jpg",
                    "thumbnail": "https://example.com/thumb1.jpg",
                    "title": "Test Image 1",
                    "description": "Test description 1",
                    "width": 800,
                    "height": 600,
                    "size": "1.2 MB",
                    "format": "jpeg"
                }
            ]
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.json.return_value = mock_response_data
            mock_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await image_finder._search_duckduckgo("test query", 5, "en")
            
            assert len(result) == 1
            assert result[0]["url"] == "https://example.com/image1.jpg"
            assert result[0]["source"] == "duckduckgo"
    
    @pytest.mark.asyncio
    async def test_search_duckduckgo_no_api_key(self, image_finder):
        """Test DuckDuckGo search without API key."""
        image_finder.duckduckgo_api_key = None
        
        with pytest.raises(Exception, match="DuckDuckGo API key not configured"):
            await image_finder._search_duckduckgo("test query", 5, "en")
    
    @pytest.mark.asyncio
    async def test_validate_image_url_success(self, image_finder):
        """Test successful image URL validation."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.headers = {
                "content-type": "image/jpeg",
                "content-length": "1024000"
            }
            
            mock_client.return_value.__aenter__.return_value.head.return_value = mock_response
            
            result = await image_finder.validate_image_url("https://example.com/image.jpg")
            
            assert result["valid"] == True
            assert result["url"] == "https://example.com/image.jpg"
            assert result["content_type"] == "image/jpeg"
            assert result["content_length"] == 1024000
            assert result["status_code"] == 200
    
    @pytest.mark.asyncio
    async def test_validate_image_url_not_found(self, image_finder):
        """Test image URL validation for non-existent image."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.status_code = 404
            
            mock_client.return_value.__aenter__.return_value.head.return_value = mock_response
            
            result = await image_finder.validate_image_url("https://example.com/nonexistent.jpg")
            
            assert result["valid"] == False
            assert result["url"] == "https://example.com/nonexistent.jpg"
            assert result["error"] == "HTTP 404"
            assert result["status_code"] == 404
    
    @pytest.mark.asyncio
    async def test_validate_image_url_error(self, image_finder):
        """Test image URL validation with error."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_client.return_value.__aenter__.return_value.head.side_effect = Exception("Connection error")
            
            result = await image_finder.validate_image_url("https://example.com/image.jpg")
            
            assert result["valid"] == False
            assert result["url"] == "https://example.com/image.jpg"
            assert result["error"] == "Connection error"
            assert result["status_code"] is None
