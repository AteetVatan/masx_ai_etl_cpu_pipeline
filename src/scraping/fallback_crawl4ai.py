"""
Fallback scraper using Crawl4AI for MASX AI ETL CPU Pipeline.

Provides a robust fallback when the primary scraper fails,
using Crawl4AI's advanced extraction capabilities.
"""

import asyncio
from typing import Dict, Any, Optional
import logging

try:
    from crawl4ai import AsyncWebCrawler
    from crawl4ai.extraction_strategy import LLMExtractionStrategy
    CRAWL4AI_AVAILABLE = True
except ImportError:
    CRAWL4AI_AVAILABLE = False

from ..config.settings import settings


logger = logging.getLogger(__name__)


class Crawl4AIFallback:
    """
    Fallback scraper using Crawl4AI for complex articles.
    
    Uses Crawl4AI's advanced extraction capabilities when the primary
    scraper fails or when dealing with complex JavaScript-heavy sites.
    """
    
    def __init__(self):
        """Initialize the Crawl4AI fallback scraper."""
        if not CRAWL4AI_AVAILABLE:
            logger.warning("Crawl4AI not available - fallback scraper disabled")
            self.enabled = False
            return
        
        self.enabled = True
        self.timeout = settings.request_timeout
        self.max_retries = settings.retry_attempts
        self.retry_delay = settings.retry_delay
        
        # Crawl4AI configuration
        self.crawler_config = {
            "verbose": False,
            "headless": True,
            "browser_type": "chromium",
            "browser_config": {
                "args": [
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-web-security",
                    "--disable-features=VizDisplayCompositor"
                ]
            }
        }
        
        logger.info("Crawl4AI fallback scraper initialized")
    
    async def scrape_article(self, url: str) -> Dict[str, Any]:
        """
        Scrape an article using Crawl4AI.
        
        Args:
            url: Article URL to scrape
            
        Returns:
            Dictionary containing scraped content and metadata
            
        Raises:
            ScrapingError: If scraping fails
        """
        if not self.enabled:
            raise ScrapingError("Crawl4AI fallback not available")
        
        if not url:
            raise ScrapingError("Invalid URL provided")
        
        logger.info(f"Using Crawl4AI fallback for: {url}")
        
        for attempt in range(self.max_retries):
            try:
                async with AsyncWebCrawler(**self.crawler_config) as crawler:
                    # Configure extraction strategy
                    extraction_strategy = LLMExtractionStrategy(
                        provider="openai",  # Can be configured
                        api_token=settings.bing_search_api_key,  # Placeholder
                        instruction="""
                        Extract the main article content, title, author, and publication date.
                        Focus on the primary article text and ignore navigation, ads, and sidebars.
                        Return clean, readable text without HTML tags.
                        """
                    )
                    
                    # Crawl the page
                    result = await crawler.arun(
                        url=url,
                        extraction_strategy=extraction_strategy,
                        wait_for="networkidle",
                        timeout=self.timeout * 1000  # Convert to milliseconds
                    )
                    
                    if not result.success:
                        raise ScrapingError(f"Crawl4AI failed to load page: {result.error_message}")
                    
                    # Extract content from result
                    article_data = self._extract_from_crawl4ai_result(result, url)
                    
                    if not article_data.get('content') or len(article_data['content'].strip()) < 100:
                        raise ScrapingError("Insufficient content extracted by Crawl4AI")
                    
                    logger.info(f"Successfully scraped with Crawl4AI: {url}")
                    return article_data
                    
            except Exception as e:
                logger.warning(f"Crawl4AI attempt {attempt + 1} failed for {url}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                    continue
                raise ScrapingError(f"Crawl4AI failed to scrape {url}: {e}")
        
        raise ScrapingError(f"Crawl4AI failed to scrape {url} after {self.max_retries} attempts")
    
    def _extract_from_crawl4ai_result(self, result, url: str) -> Dict[str, Any]:
        """
        Extract article data from Crawl4AI result.
        
        Args:
            result: Crawl4AI crawl result
            url: Original URL for reference
            
        Returns:
            Dictionary with extracted article data
        """
        # Extract basic information
        title = self._extract_title_from_result(result)
        content = self._extract_content_from_result(result)
        author = self._extract_author_from_result(result)
        published_date = self._extract_date_from_result(result)
        images = self._extract_images_from_result(result, url)
        
        return {
            "url": url,
            "title": title,
            "author": author,
            "published_date": published_date,
            "content": content,
            "images": images,
            "metadata": {
                "extraction_method": "crawl4ai",
                "success": result.success,
                "status_code": getattr(result, 'status_code', None),
                "final_url": getattr(result, 'final_url', url)
            },
            "scraped_at": result.timestamp.isoformat() if hasattr(result, 'timestamp') else None,
            "word_count": len(content.split()) if content else 0,
            "language": self._detect_language(content)
        }
    
    def _extract_title_from_result(self, result) -> Optional[str]:
        """Extract title from Crawl4AI result."""
        # Try different title sources
        title_sources = [
            getattr(result, 'title', None),
            getattr(result, 'metadata', {}).get('title'),
            getattr(result, 'extracted_content', {}).get('title')
        ]
        
        for title in title_sources:
            if title and isinstance(title, str) and title.strip():
                return title.strip()
        
        return None
    
    def _extract_content_from_result(self, result) -> str:
        """Extract main content from Crawl4AI result."""
        # Try different content sources
        content_sources = [
            getattr(result, 'cleaned_html', ''),
            getattr(result, 'markdown', ''),
            getattr(result, 'extracted_content', {}).get('content', ''),
            getattr(result, 'extracted_content', {}).get('text', '')
        ]
        
        for content in content_sources:
            if content and isinstance(content, str) and len(content.strip()) > 200:
                return content.strip()
        
        # Fallback to raw HTML if available
        if hasattr(result, 'html') and result.html:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(result.html, 'html.parser')
            return soup.get_text(separator=' ', strip=True)
        
        return ""
    
    def _extract_author_from_result(self, result) -> Optional[str]:
        """Extract author from Crawl4AI result."""
        # Try different author sources
        author_sources = [
            getattr(result, 'metadata', {}).get('author'),
            getattr(result, 'extracted_content', {}).get('author'),
            getattr(result, 'extracted_content', {}).get('byline')
        ]
        
        for author in author_sources:
            if author and isinstance(author, str) and author.strip():
                return author.strip()
        
        return None
    
    def _extract_date_from_result(self, result) -> Optional[str]:
        """Extract publication date from Crawl4AI result."""
        # Try different date sources
        date_sources = [
            getattr(result, 'metadata', {}).get('published_time'),
            getattr(result, 'metadata', {}).get('modified_time'),
            getattr(result, 'extracted_content', {}).get('published_date'),
            getattr(result, 'extracted_content', {}).get('date')
        ]
        
        for date in date_sources:
            if date and isinstance(date, str) and date.strip():
                return date.strip()
        
        return None
    
    def _extract_images_from_result(self, result, base_url: str) -> list:
        """Extract images from Crawl4AI result."""
        images = []
        
        # Try to get images from different sources
        image_sources = [
            getattr(result, 'extracted_content', {}).get('images', []),
            getattr(result, 'metadata', {}).get('images', [])
        ]
        
        for image_list in image_sources:
            if isinstance(image_list, list):
                for img in image_list:
                    if isinstance(img, dict):
                        images.append({
                            "url": img.get('url', ''),
                            "alt": img.get('alt', ''),
                            "title": img.get('title', ''),
                            "width": img.get('width'),
                            "height": img.get('height')
                        })
                    elif isinstance(img, str):
                        images.append({
                            "url": img,
                            "alt": "",
                            "title": "",
                            "width": None,
                            "height": None
                        })
        
        return images
    
    def _detect_language(self, text: str) -> str:
        """Simple language detection based on common words."""
        if not text:
            return "unknown"
        
        # Simple heuristic based on common words
        text_lower = text.lower()
        
        # English indicators
        english_words = ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by']
        english_count = sum(1 for word in english_words if word in text_lower)
        
        # Spanish indicators
        spanish_words = ['el', 'la', 'de', 'que', 'y', 'a', 'en', 'un', 'es', 'se', 'no', 'te', 'lo', 'le']
        spanish_count = sum(1 for word in spanish_words if word in text_lower)
        
        # French indicators
        french_words = ['le', 'de', 'et', 'à', 'un', 'il', 'être', 'et', 'en', 'avoir', 'que', 'pour', 'dans']
        french_count = sum(1 for word in french_words if word in text_lower)
        
        # German indicators
        german_words = ['der', 'die', 'und', 'in', 'den', 'von', 'zu', 'das', 'mit', 'sich', 'des', 'auf', 'für']
        german_count = sum(1 for word in german_words if word in text_lower)
        
        # Determine language based on word counts
        if english_count > max(spanish_count, french_count, german_count):
            return "en"
        elif spanish_count > max(english_count, french_count, german_count):
            return "es"
        elif french_count > max(english_count, spanish_count, german_count):
            return "fr"
        elif german_count > max(english_count, spanish_count, german_count):
            return "de"
        else:
            return "unknown"


class ScrapingError(Exception):
    """Custom exception for scraping-related errors."""
    pass


# Global fallback scraper instance
fallback_scraper = Crawl4AIFallback()
