"""
Unit tests for the article scraper module.

Tests the primary scraper functionality including HTML parsing,
content extraction, and error handling.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from bs4 import BeautifulSoup

from src.scraping.scraper import BeautifulSoupExtractor, ScrapingError


class TestArticleScraper:
    """Test cases for ArticleScraper class."""
    
    @pytest.fixture
    def scraper(self):
        """Create a scraper instance for testing."""
        return BeautifulSoupExtractor()
    
    def test_init(self, scraper):
        """Test scraper initialization."""
        assert scraper.timeout == 30  # Default timeout
        assert scraper.max_retries == 3  # Default retries
        assert scraper.retry_delay == 1.0  # Default delay
        assert "Mozilla" in scraper.headers["User-Agent"]
    
    def test_is_valid_url(self, scraper):
        """Test URL validation."""
        # Valid URLs
        assert scraper._is_valid_url("https://example.com/article")
        assert scraper._is_valid_url("http://example.com/article")
        assert scraper._is_valid_url("https://subdomain.example.com/path")
        
        # Invalid URLs
        assert not scraper._is_valid_url("")
        assert not scraper._is_valid_url("not-a-url")
        assert not scraper._is_valid_url("ftp://example.com")
        assert not scraper._is_valid_url("example.com")
    
    def test_clean_soup(self, scraper):
        """Test HTML cleaning functionality."""
        html = """
        <html>
            <head><title>Test</title></head>
            <body>
                <script>alert('test');</script>
                <style>body { color: red; }</style>
                <nav>Navigation</nav>
                <article>
                    <h1>Article Title</h1>
                    <p>Article content</p>
                </article>
                <aside>Sidebar</aside>
                <!-- This is a comment -->
            </body>
        </html>
        """
        
        soup = BeautifulSoup(html, 'html.parser')
        scraper._clean_soup(soup)
        
        # Check that unwanted elements are removed
        assert soup.find('script') is None
        assert soup.find('style') is None
        assert soup.find('nav') is None
        assert soup.find('aside') is None
        assert soup.find(string=lambda text: isinstance(text, Comment)) is None
        
        # Check that article content remains
        assert soup.find('article') is not None
        assert soup.find('h1') is not None
        assert soup.find('p') is not None
    
    def test_extract_title(self, scraper):
        """Test title extraction."""
        html = """
        <html>
            <head><title>Page Title</title></head>
            <body>
                <h1>Article Title</h1>
                <article>
                    <h2>Subtitle</h2>
                </article>
            </body>
        </html>
        """
        
        soup = BeautifulSoup(html, 'html.parser')
        title = scraper._extract_title(soup)
        
        assert title == "Article Title"
    
    def test_extract_author(self, scraper):
        """Test author extraction."""
        html = """
        <html>
            <body>
                <article>
                    <div class="author">John Doe</div>
                    <p>Article content</p>
                </article>
            </body>
        </html>
        """
        
        soup = BeautifulSoup(html, 'html.parser')
        author = scraper._extract_author(soup)
        
        assert author == "John Doe"
    
    def test_extract_published_date(self, scraper):
        """Test publication date extraction."""
        html = """
        <html>
            <body>
                <article>
                    <time datetime="2024-01-01T00:00:00Z">January 1, 2024</time>
                    <p>Article content</p>
                </article>
            </body>
        </html>
        """
        
        soup = BeautifulSoup(html, 'html.parser')
        date = scraper._extract_published_date(soup)
        
        assert date == "2024-01-01T00:00:00Z"
    
    def test_extract_content(self, scraper):
        """Test content extraction."""
        html = """
        <html>
            <body>
                <article>
                    <h1>Article Title</h1>
                    <p>First paragraph of content.</p>
                    <p>Second paragraph of content.</p>
                    <div class="advertisement">Ad content</div>
                </article>
            </body>
        </html>
        """
        
        soup = BeautifulSoup(html, 'html.parser')
        content = scraper._extract_content(soup)
        
        assert "Article Title" in content
        assert "First paragraph of content" in content
        assert "Second paragraph of content" in content
        assert "Ad content" not in content
    
    def test_extract_images(self, scraper):
        """Test image extraction."""
        html = """
        <html>
            <body>
                <article>
                    <img src="/image1.jpg" alt="Image 1" title="Title 1" width="800" height="600">
                    <img src="https://example.com/image2.jpg" alt="Image 2">
                    <img src="//cdn.example.com/image3.jpg" alt="Image 3">
                </article>
            </body>
        </html>
        """
        
        soup = BeautifulSoup(html, 'html.parser')
        images = scraper._extract_images(soup, "https://example.com/article")
        
        assert len(images) == 3
        
        # Check first image
        assert images[0]["url"] == "https://example.com/image1.jpg"
        assert images[0]["alt"] == "Image 1"
        assert images[0]["title"] == "Title 1"
        assert images[0]["width"] == "800"
        assert images[0]["height"] == "600"
        
        # Check second image (already absolute)
        assert images[1]["url"] == "https://example.com/image2.jpg"
        
        # Check third image (protocol-relative)
        assert images[2]["url"] == "https://cdn.example.com/image3.jpg"
    
    def test_detect_language(self, scraper):
        """Test language detection."""
        # English text
        english_text = "The quick brown fox jumps over the lazy dog. This is a test article about technology and innovation."
        assert scraper._detect_language(english_text) == "en"
        
        # Spanish text
        spanish_text = "El zorro marrón rápido salta sobre el perro perezoso. Este es un artículo de prueba sobre tecnología e innovación."
        assert scraper._detect_language(spanish_text) == "es"
        
        # French text
        french_text = "Le renard brun rapide saute par-dessus le chien paresseux. Ceci est un article de test sur la technologie et l'innovation."
        assert scraper._detect_language(french_text) == "fr"
        
        # German text
        german_text = "Der schnelle braune Fuchs springt über den faulen Hund. Dies ist ein Testartikel über Technologie und Innovation."
        assert scraper._detect_language(german_text) == "de"
        
        # Unknown language
        unknown_text = "Lorem ipsum dolor sit amet consectetur adipiscing elit"
        assert scraper._detect_language(unknown_text) == "unknown"
    
    @pytest.mark.asyncio
    async def test_scrape_article_success(self, scraper):
        """Test successful article scraping."""
        mock_html = """
        <html>
            <head><title>Test Article</title></head>
            <body>
                <article>
                    <h1>Test Article Title</h1>
                    <div class="author">Test Author</div>
                    <time datetime="2024-01-01T00:00:00Z">January 1, 2024</time>
                    <p>This is test article content with enough text to meet the minimum requirements.</p>
                    <p>This is another paragraph to ensure we have sufficient content for the article.</p>
                </article>
            </body>
        </html>
        """
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.content = mock_html.encode()
            mock_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await scraper.scrape_article("https://example.com/article")
            
            assert result["url"] == "https://example.com/article"
            assert result["title"] == "Test Article Title"
            assert result["author"] == "Test Author"
            assert result["published_date"] == "2024-01-01T00:00:00Z"
            assert "test article content" in result["content"]
            assert result["word_count"] > 0
            assert result["language"] == "en"
    
    @pytest.mark.asyncio
    async def test_scrape_article_404_error(self, scraper):
        """Test scraping with 404 error."""
        with patch('httpx.AsyncClient') as mock_client:
            from httpx import HTTPStatusError
            mock_response = AsyncMock()
            mock_response.status_code = 404
            mock_response.raise_for_status.side_effect = HTTPStatusError("Not Found", request=AsyncMock(), response=mock_response)
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            with pytest.raises(ScrapingError, match="Article not found"):
                await scraper.scrape_article("https://example.com/nonexistent")
    
    @pytest.mark.asyncio
    async def test_scrape_article_timeout(self, scraper):
        """Test scraping with timeout error."""
        with patch('httpx.AsyncClient') as mock_client:
            from httpx import TimeoutException
            mock_client.return_value.__aenter__.return_value.get.side_effect = TimeoutException("Request timeout")
            
            with pytest.raises(ScrapingError, match="Timeout"):
                await scraper.scrape_article("https://example.com/slow")
    
    @pytest.mark.asyncio
    async def test_scrape_article_insufficient_content(self, scraper):
        """Test scraping with insufficient content."""
        mock_html = """
        <html>
            <body>
                <article>
                    <h1>Short</h1>
                    <p>Too short</p>
                </article>
            </body>
        </html>
        """
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.content = mock_html.encode()
            mock_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            with pytest.raises(ScrapingError, match="Insufficient content"):
                await scraper.scrape_article("https://example.com/short")
    
    def test_extract_text_from_element(self, scraper):
        """Test text extraction from specific element."""
        html = """
        <div class="article-content">
            <h1>Title</h1>
            <p>First paragraph with <strong>bold text</strong>.</p>
            <p>Second paragraph with <em>italic text</em>.</p>
            <div class="advertisement">Ad content</div>
        </div>
        """
        
        soup = BeautifulSoup(html, 'html.parser')
        element = soup.find('div', class_='article-content')
        
        text = scraper._extract_text_from_element(element)
        
        assert "Title" in text
        assert "First paragraph with bold text" in text
        assert "Second paragraph with italic text" in text
        assert "Ad content" not in text
    
    def test_extract_metadata(self, scraper):
        """Test metadata extraction."""
        html = """
        <html>
            <head>
                <meta name="description" content="Test article description">
                <meta name="keywords" content="test, article, example">
                <meta property="og:title" content="OG Title">
                <meta property="og:description" content="OG Description">
            </head>
            <body>
                <article>Content</article>
            </body>
        </html>
        """
        
        soup = BeautifulSoup(html, 'html.parser')
        metadata = scraper._extract_metadata(soup)
        
        assert metadata["description"] == "Test article description"
        assert metadata["keywords"] == "test, article, example"
        assert metadata["og:title"] == "OG Title"
        assert metadata["og:description"] == "OG Description"
