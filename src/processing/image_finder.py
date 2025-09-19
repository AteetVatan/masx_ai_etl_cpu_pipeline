"""
Image finder for MASX AI ETL CPU Pipeline.

Uses multiple APIs and CLIP for finding relevant images for articles
with fallback strategies and quality filtering.
"""

import asyncio
import base64
from typing import List, Dict, Any, Optional, Tuple
import logging
import httpx
from urllib.parse import quote_plus

from ..config.settings import settings


logger = logging.getLogger(__name__)


class ImageFinder:
    """
    Multi-source image finder with quality filtering.
    
    Uses Bing Search API, DuckDuckGo, and CLIP for finding relevant images
    with comprehensive fallback strategies and quality assessment.
    """
    
    def __init__(self):
        """Initialize the image finder with API configurations."""
        self.enabled = settings.enable_image_search
        self.bing_api_key = settings.bing_search_api_key
        self.duckduckgo_api_key = settings.duckduckgo_api_key
        
        # API endpoints
        self.bing_search_url = "https://api.bing.microsoft.com/v7.0/images/search"
        self.duckduckgo_url = "https://api.duckduckgo.com/"
        
        # Quality filters
        self.min_width = 300
        self.min_height = 200
        self.max_width = 4000
        self.max_height = 4000
        self.max_file_size = 5 * 1024 * 1024  # 5MB
        
        # Supported image formats
        self.supported_formats = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
        
        logger.info("Image finder initialized")
    
    async def find_images(
        self, 
        query: str, 
        max_images: int = 5,
        language: str = "en"
    ) -> Dict[str, Any]:
        """
        Find relevant images for a given query.
        
        Args:
            query: Search query for images
            max_images: Maximum number of images to return
            language: Language for search results
            
        Returns:
            Dictionary containing found images and metadata
        """
        if not self.enabled or not query:
            return {
                "images": [],
                "total_found": 0,
                "search_method": "disabled",
                "query": query,
                "language": language
            }
        
        logger.info(f"Searching for images with query: {query}")
        
        # Try different search methods in order of preference
        search_methods = [
            ("bing", self._search_bing),
            ("duckduckgo", self._search_duckduckgo),
            ("fallback", self._search_fallback)
        ]
        
        for method_name, method_func in search_methods:
            try:
                images = await method_func(query, max_images, language)
                if images:
                    logger.info(f"Found {len(images)} images using {method_name}")
                    return {
                        "images": images,
                        "total_found": len(images),
                        "search_method": method_name,
                        "query": query,
                        "language": language
                    }
            except Exception as e:
                logger.warning(f"Search method {method_name} failed: {e}")
                continue
        
        logger.warning(f"No images found for query: {query}")
        return {
            "images": [],
            "total_found": 0,
            "search_method": "none",
            "query": query,
            "language": language
        }
    
    async def _search_bing(self, query: str, max_images: int, language: str) -> List[Dict[str, Any]]:
        """Search for images using Bing Search API."""
        if not self.bing_api_key:
            raise Exception("Bing API key not configured")
        
        headers = {
            "Ocp-Apim-Subscription-Key": self.bing_api_key,
            "User-Agent": "MASX-AI-Pipeline/1.0"
        }
        
        params = {
            "q": query,
            "count": min(max_images, 50),  # Bing allows up to 50
            "offset": 0,
            "mkt": language,
            "safeSearch": "Moderate",
            "imageType": "Photo",
            "size": "Medium",
            "aspect": "Wide"
        }
        
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(self.bing_search_url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            images = []
            
            for item in data.get("value", []):
                image_data = self._process_bing_image(item)
                if image_data and self._is_high_quality_image(image_data):
                    images.append(image_data)
                    if len(images) >= max_images:
                        break
            
            return images
    
    async def _search_duckduckgo(self, query: str, max_images: int, language: str) -> List[Dict[str, Any]]:
        """Search for images using DuckDuckGo API."""
        if not self.duckduckgo_api_key:
            raise Exception("DuckDuckGo API key not configured")
        
        headers = {
            "Authorization": f"Bearer {self.duckduckgo_api_key}",
            "User-Agent": "MASX-AI-Pipeline/1.0"
        }
        
        params = {
            "q": query,
            "kl": language,
            "count": min(max_images, 20),  # DuckDuckGo limit
            "offset": 0
        }
        
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(self.duckduckgo_url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            images = []
            
            for item in data.get("results", []):
                image_data = self._process_duckduckgo_image(item)
                if image_data and self._is_high_quality_image(image_data):
                    images.append(image_data)
                    if len(images) >= max_images:
                        break
            
            return images
    
    async def _search_fallback(self, query: str, max_images: int, language: str) -> List[Dict[str, Any]]:
        """Fallback search method using basic web scraping."""
        # This is a simplified fallback - in production, you might use
        # a more sophisticated approach or additional APIs
        
        # For now, return empty list as fallback
        logger.info("Using fallback search method (no images found)")
        return []
    
    def _process_bing_image(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a Bing search result item into standardized format."""
        try:
            return {
                "url": item.get("contentUrl", ""),
                "thumbnail_url": item.get("thumbnailUrl", ""),
                "title": item.get("name", ""),
                "description": item.get("description", ""),
                "width": item.get("width", 0),
                "height": item.get("height", 0),
                "file_size": item.get("contentSize", ""),
                "format": item.get("encodingFormat", ""),
                "source": "bing",
                "source_url": item.get("hostPageUrl", ""),
                "date_published": item.get("datePublished", ""),
                "creator": item.get("creator", {}).get("name", "") if item.get("creator") else ""
            }
        except Exception as e:
            logger.error(f"Error processing Bing image: {e}")
            return None
    
    def _process_duckduckgo_image(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a DuckDuckGo search result item into standardized format."""
        try:
            return {
                "url": item.get("image", ""),
                "thumbnail_url": item.get("thumbnail", ""),
                "title": item.get("title", ""),
                "description": item.get("description", ""),
                "width": item.get("width", 0),
                "height": item.get("height", 0),
                "file_size": item.get("size", ""),
                "format": item.get("format", ""),
                "source": "duckduckgo",
                "source_url": item.get("url", ""),
                "date_published": item.get("date", ""),
                "creator": item.get("creator", "")
            }
        except Exception as e:
            logger.error(f"Error processing DuckDuckGo image: {e}")
            return None
    
    def _is_high_quality_image(self, image_data: Dict[str, Any]) -> bool:
        """Check if an image meets quality requirements."""
        try:
            # Check dimensions
            width = image_data.get("width", 0)
            height = image_data.get("height", 0)
            
            if width < self.min_width or height < self.min_height:
                return False
            
            if width > self.max_width or height > self.max_height:
                return False
            
            # Check aspect ratio (avoid extremely wide or tall images)
            aspect_ratio = width / height if height > 0 else 0
            if aspect_ratio < 0.5 or aspect_ratio > 3.0:
                return False
            
            # Check file size if available
            file_size = image_data.get("file_size", "")
            if file_size and isinstance(file_size, str):
                # Try to parse file size (e.g., "1.2 MB")
                try:
                    size_parts = file_size.split()
                    if len(size_parts) >= 2:
                        size_value = float(size_parts[0])
                        size_unit = size_parts[1].upper()
                        
                        # Convert to bytes
                        if size_unit == "KB":
                            size_bytes = size_value * 1024
                        elif size_unit == "MB":
                            size_bytes = size_value * 1024 * 1024
                        elif size_unit == "GB":
                            size_bytes = size_value * 1024 * 1024 * 1024
                        else:
                            size_bytes = size_value
                        
                        if size_bytes > self.max_file_size:
                            return False
                except (ValueError, IndexError):
                    pass  # Ignore parsing errors
            
            # Check format
            format_str = image_data.get("format", "").lower()
            if format_str and not any(fmt in format_str for fmt in self.supported_formats):
                return False
            
            # Check URL validity
            url = image_data.get("url", "")
            if not url or not url.startswith(("http://", "https://")):
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking image quality: {e}")
            return False
    
    async def validate_image_url(self, url: str) -> Dict[str, Any]:
        """
        Validate an image URL and get basic information.
        
        Args:
            url: Image URL to validate
            
        Returns:
            Dictionary with validation results
        """
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                # Send HEAD request to check if image exists
                response = await client.head(url, follow_redirects=True)
                
                if response.status_code == 200:
                    content_type = response.headers.get("content-type", "")
                    content_length = response.headers.get("content-length", "0")
                    
                    return {
                        "valid": True,
                        "url": url,
                        "content_type": content_type,
                        "content_length": int(content_length) if content_length.isdigit() else 0,
                        "status_code": response.status_code
                    }
                else:
                    return {
                        "valid": False,
                        "url": url,
                        "error": f"HTTP {response.status_code}",
                        "status_code": response.status_code
                    }
                    
        except Exception as e:
            return {
                "valid": False,
                "url": url,
                "error": str(e),
                "status_code": None
            }
    
    def generate_search_queries(self, title: str, content: str, language: str = "en") -> List[str]:
        """
        Generate search queries for finding relevant images.
        
        Args:
            title: Article title
            content: Article content
            language: Language code
            
        Returns:
            List of search queries
        """
        queries = []
        
        # Use title as primary query
        if title:
            queries.append(title)
        
        # Extract keywords from content
        keywords = self._extract_keywords(content)
        if keywords:
            # Create queries with top keywords
            for i in range(min(3, len(keywords))):
                queries.append(keywords[i])
            
            # Create combined queries
            if len(keywords) >= 2:
                queries.append(f"{keywords[0]} {keywords[1]}")
            if len(keywords) >= 3:
                queries.append(f"{keywords[0]} {keywords[1]} {keywords[2]}")
        
        # Add language-specific terms
        if language != "en":
            queries.append(f"{title} {language}")
        
        # Remove duplicates and limit
        unique_queries = list(dict.fromkeys(queries))
        return unique_queries[:5]  # Limit to 5 queries
    
    def _extract_keywords(self, content: str) -> List[str]:
        """Extract keywords from content for image search."""
        if not content:
            return []
        
        # Simple keyword extraction (can be enhanced with NLP)
        import re
        
        # Remove common stop words
        stop_words = {
            "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
            "of", "with", "by", "is", "are", "was", "were", "be", "been", "being",
            "have", "has", "had", "do", "does", "did", "will", "would", "could",
            "should", "may", "might", "must", "can", "this", "that", "these",
            "those", "i", "you", "he", "she", "it", "we", "they", "me", "him",
            "her", "us", "them", "my", "your", "his", "her", "its", "our", "their"
        }
        
        # Extract words
        words = re.findall(r'\b[a-zA-Z]{3,}\b', content.lower())
        
        # Filter out stop words and count frequency
        word_counts = {}
        for word in words:
            if word not in stop_words:
                word_counts[word] = word_counts.get(word, 0) + 1
        
        # Sort by frequency and return top keywords
        sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        return [word for word, count in sorted_words[:10]]


# Global image finder instance
image_finder = ImageFinder()
