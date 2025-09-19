"""
High-performance web scraper for MASX AI ETL CPU Pipeline.

Uses httpx and BeautifulSoup for fast, reliable article scraping with
comprehensive error handling, retry logic, and content extraction.
"""

import asyncio
import re
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urljoin, urlparse
from datetime import datetime
import logging

import httpx
from bs4 import BeautifulSoup, Comment
from bs4.element import NavigableString

from ..config.settings import settings


logger = logging.getLogger(__name__)


class ArticleScraper:
    """
    High-performance article scraper with multiple extraction strategies.
    
    Handles various article formats, extracts clean text content,
    metadata, and handles common anti-bot measures.
    """
    
    def __init__(self):
        """Initialize the scraper with optimized settings."""
        self.timeout = settings.request_timeout
        self.max_retries = settings.retry_attempts
        self.retry_delay = settings.retry_delay
        
        # Common headers to avoid detection
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        # Common article selectors for different sites
        self.article_selectors = [
            'article',
            '[role="main"]',
            '.article-content',
            '.post-content',
            '.entry-content',
            '.content',
            '.main-content',
            '#content',
            '.article-body',
            '.story-body',
            '.article-text',
            '.post-body'
        ]
        
        # Metadata selectors
        self.title_selectors = [
            'h1',
            '.article-title',
            '.post-title',
            '.entry-title',
            '.headline',
            'title'
        ]
        
        self.author_selectors = [
            '.author',
            '.byline',
            '.article-author',
            '.post-author',
            '[rel="author"]',
            '.writer'
        ]
        
        self.date_selectors = [
            'time',
            '.date',
            '.published',
            '.article-date',
            '.post-date',
            '.timestamp'
        ]
    
    async def scrape_article(self, url: str) -> Dict[str, Any]:
        """
        Scrape an article from the given URL.
        
        Args:
            url: Article URL to scrape
            
        Returns:
            Dictionary containing scraped content and metadata
            
        Raises:
            ScrapingError: If scraping fails after retries
        """
        if not url or not self._is_valid_url(url):
            raise ScrapingError(f"Invalid URL: {url}")
        
        logger.info(f"Starting to scrape article: {url}")
        
        for attempt in range(self.max_retries):
            try:
                async with httpx.AsyncClient(
                    headers=self.headers,
                    timeout=self.timeout,
                    follow_redirects=True
                ) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    
                    # Parse the HTML content
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Extract article content
                    article_data = self._extract_article_data(soup, url)
                    
                    # Validate extracted content
                    if not article_data.get('content') or len(article_data['content'].strip()) < 100:
                        raise ScrapingError("Insufficient content extracted")
                    
                    logger.info(f"Successfully scraped article: {url}")
                    return article_data
                    
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise ScrapingError(f"Article not found (404): {url}")
                elif e.response.status_code >= 500:
                    logger.warning(f"Server error {e.response.status_code} for {url}, attempt {attempt + 1}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.retry_delay * (2 ** attempt))
                        continue
                    raise ScrapingError(f"Server error {e.response.status_code}: {url}")
                else:
                    raise ScrapingError(f"HTTP error {e.response.status_code}: {url}")
                    
            except httpx.TimeoutException:
                logger.warning(f"Timeout for {url}, attempt {attempt + 1}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                    continue
                raise ScrapingError(f"Timeout after {self.max_retries} attempts: {url}")
                
            except Exception as e:
                logger.error(f"Unexpected error scraping {url}: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                    continue
                raise ScrapingError(f"Failed to scrape {url}: {e}")
        
        raise ScrapingError(f"Failed to scrape {url} after {self.max_retries} attempts")
    
    def _is_valid_url(self, url: str) -> bool:
        """Validate if the URL is properly formatted."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    def _extract_article_data(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Extract article data from parsed HTML.
        
        Args:
            soup: BeautifulSoup parsed HTML
            url: Original URL for reference
            
        Returns:
            Dictionary with extracted article data
        """
        # Remove unwanted elements
        self._clean_soup(soup)
        
        # Extract title
        title = self._extract_title(soup)
        
        # Extract author
        author = self._extract_author(soup)
        
        # Extract publication date
        published_date = self._extract_published_date(soup)
        
        # Extract main content
        content = self._extract_content(soup)
        
        # Extract images
        images = self._extract_images(soup, url)
        
        # Extract metadata
        metadata = self._extract_metadata(soup)
        
        return {
            "url": url,
            "title": title,
            "author": author,
            "published_date": published_date,
            "content": content,
            "images": images,
            "metadata": metadata,
            "scraped_at": datetime.utcnow().isoformat(),
            "word_count": len(content.split()) if content else 0,
            "language": self._detect_language(content)
        }
    
    def _clean_soup(self, soup: BeautifulSoup) -> None:
        """Remove unwanted elements from the soup."""
        # Remove script and style elements
        for element in soup(["script", "style", "nav", "header", "footer", "aside"]):
            element.decompose()
        
        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        # Remove elements with common unwanted classes
        unwanted_classes = [
            "advertisement", "ad", "ads", "sidebar", "menu", "navigation",
            "social", "share", "comments", "related", "recommended"
        ]
        
        for class_name in unwanted_classes:
            for element in soup.find_all(class_=re.compile(class_name, re.I)):
                element.decompose()
    
    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article title."""
        for selector in self.title_selectors:
            element = soup.select_one(selector)
            if element and element.get_text(strip=True):
                return element.get_text(strip=True)
        return None
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article author."""
        for selector in self.author_selectors:
            element = soup.select_one(selector)
            if element and element.get_text(strip=True):
                return element.get_text(strip=True)
        return None
    
    def _extract_published_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract publication date."""
        for selector in self.date_selectors:
            element = soup.select_one(selector)
            if element:
                # Try to get datetime attribute first
                datetime_attr = element.get('datetime')
                if datetime_attr:
                    return datetime_attr
                
                # Fallback to text content
                text = element.get_text(strip=True)
                if text:
                    return text
        return None
    
    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Extract main article content."""
        # Try different article selectors
        for selector in self.article_selectors:
            article_element = soup.select_one(selector)
            if article_element:
                content = self._extract_text_from_element(article_element)
                if len(content.strip()) > 200:  # Minimum content length
                    return content
        
        # Fallback: try to find the largest text block
        paragraphs = soup.find_all('p')
        if paragraphs:
            content = ' '.join(p.get_text(strip=True) for p in paragraphs)
            if len(content.strip()) > 200:
                return content
        
        # Last resort: get all text
        return soup.get_text(separator=' ', strip=True)
    
    def _extract_text_from_element(self, element) -> str:
        """Extract clean text from a specific element."""
        # Remove unwanted child elements
        for unwanted in element.find_all(['script', 'style', 'nav', 'aside']):
            unwanted.decompose()
        
        # Extract text with proper spacing
        text_parts = []
        for child in element.descendants:
            if isinstance(child, NavigableString):
                text = child.strip()
                if text:
                    text_parts.append(text)
            elif child.name in ['p', 'div', 'br']:
                text_parts.append(' ')
        
        return ' '.join(text_parts)
    
    def _extract_images(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
        """Extract images from the article."""
        images = []
        img_elements = soup.find_all('img')
        
        for img in img_elements:
            src = img.get('src')
            if not src:
                continue
            
            # Convert relative URLs to absolute
            if src.startswith('//'):
                src = 'https:' + src
            elif src.startswith('/'):
                src = urljoin(base_url, src)
            elif not src.startswith('http'):
                src = urljoin(base_url, src)
            
            # Extract alt text and other attributes
            alt_text = img.get('alt', '')
            title = img.get('title', '')
            
            images.append({
                "url": src,
                "alt": alt_text,
                "title": title,
                "width": img.get('width'),
                "height": img.get('height')
            })
        
        return images
    
    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract additional metadata from the page."""
        metadata = {}
        
        # Extract meta tags
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            name = meta.get('name') or meta.get('property')
            content = meta.get('content')
            if name and content:
                metadata[name] = content
        
        # Extract Open Graph tags
        og_tags = soup.find_all('meta', property=re.compile(r'^og:'))
        for tag in og_tags:
            property_name = tag.get('property')
            content = tag.get('content')
            if property_name and content:
                metadata[property_name] = content
        
        return metadata
    
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
        elif german_count > max(english_count, spanish_count, french_count):
            return "de"
        else:
            return "unknown"


class ScrapingError(Exception):
    """Custom exception for scraping-related errors."""
    pass


# Global scraper instance
scraper = ArticleScraper()
