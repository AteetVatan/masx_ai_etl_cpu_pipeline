"""
Image finder for MASX AI ETL CPU Pipeline.

Uses DuckDuckGo image search with quality filtering
for finding relevant images for articles.
"""

import os
from typing import List, Dict, Any, Optional
from random import choice
import httpx
import pycountry
from ddgs import DDGS
from ddgs.exceptions import DDGSException
from babel.core import get_global

from src.config import get_settings, get_service_logger
from src.models import ExtractResult, EntityModel, EntityAttributes

logger = get_service_logger(__name__)
settings = get_settings()


class ImageFinder:
    """
    Image finder using DuckDuckGo only, with quality filtering.
    """

    def __init__(self):
        """Initialize the image finder with config."""
        self.enabled = settings.enable_image_search

        # Quality filters
        self.min_width = 500
        self.min_height = 500
        self.max_width = 4000
        self.max_height = 4000
        self.max_file_size = 5 * 1024 * 1024  # 5MB
        self.ddgs = None

        # Supported image formats
        self.supported_formats = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

        logger.info("DuckDuckGo ImageFinder initialized")

    async def test_duckduckgo(self):
        """Test DuckDuckGo image search."""
        try:
            results = await self.ddgs.images("brazil", max_results=5, backend="lite")
            return results
        except Exception as e:
            logger.error(f"DuckDuckGo test failed: {e}")
            return None

    async def get_images_from_duckduckgo(
        self,
        query: str,
        max_images: int = 5,
        proxies: list[str] | None = None,
        duckduckgo_region: str = "us-en",
    ) -> List[Dict[str, Any]] | None:
        # Remove proxy env vars
        for var in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ):
            os.environ.pop(var, None)

        try:
            client = httpx.Client(http2=False, verify=True)
            with DDGS(proxy=None, timeout=20, verify=True) as ddgs:
                results = ddgs.images(
                    query, max_results=max_images, region=duckduckgo_region
                )
                return results
        except Exception as e:
            pass

        proxy = None
        if proxies:
            proxy = choice(proxies)
            proxy = f"http://{proxy}"

        if proxy:
            try:
                client = httpx.Client(http2=False, verify=True)
                with DDGS(proxy=proxy, timeout=20, verify=True) as ddgs:
                    results = ddgs.images(
                        query, max_results=max_images, region=duckduckgo_region
                    )
                    return results
            except Exception as e:
                pass

        return None

    async def find_images(
        self,
        query: str,
        max_images: int = 5,
        duckduckgo_region: str = None,  # "en-us",
        proxies: list[str] = None,
    ) -> Dict[str, Any]:
        """
        Find relevant images for a given query using DuckDuckGo.

        Args:
            query: Search query for images
            max_images: Maximum number of images to return
            duckduckgo_region: duckduckgo_region for search results

        Returns:
            Dictionary containing found images and metadata
        """
        if not self.enabled or not query:
            return {
                "images": [],
                "total_found": 0,
                "search_method": "disabled",
                "query": query,
                "duckduckgo_region": duckduckgo_region,
            }

        # logger.info(f"Searching DuckDuckGo for images with query: {query}")

        try:
            images: List[str] = []
            images_data_set: List[Dict[str, Any]] = []
            results = await self.get_images_from_duckduckgo(
                query, max_images, proxies, duckduckgo_region
            )
            if not results:
                return {
                    "images": [],
                    "total_found": 0,
                    "images_data": [],
                    "search_method": "duckduckgo_error",
                    "query": query,
                    "duckduckgo_region": duckduckgo_region,
                }

            for item in results:
                image_data = self._process_duckduckgo_image(item)
                if image_data and self._is_high_quality_image(image_data):
                    images.append(image_data.get("image"))
                    images_data_set.append(image_data)
                    if len(images) >= max_images:
                        break

            return {
                "images": images,
                "total_found": len(images),
                "images_data": images_data_set,
                "search_method": "duckduckgo",
                "query": query,
                "duckduckgo_region": duckduckgo_region,
            }
        except DDGSException as e:
            logger.error(f"DuckDuckGo: {e}")
            return {
                "images": [],
                "total_found": 0,
                "images_data": [],
                "search_method": "duckduckgo_error",
                "query": query,
                "duckduckgo_region": duckduckgo_region,
            }
        except Exception as e:
            logger.error(f"DuckDuckGo image search failed: {e}")
            return {
                "images": [],
                "total_found": 0,
                "images_data": [],
                "search_method": "duckduckgo_error",
                "query": query,
                "duckduckgo_region": duckduckgo_region,
            }

    async def download_image(self, image_url: str) -> Optional[str]:
        """Download an image from the given URL."""
        try:
            response = await httpx.get(image_url)
            return response.content
        except Exception as e:
            logger.error(f"Error downloading image: {e}")
            return None

    def _process_duckduckgo_image(
        self, item: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Process a DuckDuckGo result into standardized format."""
        try:
            image_data = item.get("image", "")
            width_data = item.get("width", 0)
            height_data = item.get("height", 0)

            return {
                "image": image_data,
                "width": width_data,
                "height": height_data,
            }
        except Exception as e:
            logger.error(f"Error processing DuckDuckGo image: {e}")
            return None

    def _is_high_quality_image(self, image_data: Dict[str, Any]) -> bool:
        """Check if an image meets quality requirements."""
        try:
            width = image_data.get("width", 0) or 0
            height = image_data.get("height", 0) or 0

            if width < self.min_width or height < self.min_height:
                return False
            if width > self.max_width or height > self.max_height:
                return False

            # Check aspect ratio
            aspect_ratio = width / height if height > 0 else 0
            if aspect_ratio < 0.5 or aspect_ratio > 3.0:
                return False

            # Check format
            url = image_data.get("image", "")
            if not url or not url.startswith(("http://", "https://")):
                return False

            return True
        except Exception as e:
            logger.error(f"Error checking image quality: {e}")
            return False

    def generate_search_queries(self, extracted_data: ExtractResult) -> List[str]:
        """
        Generate search queries for finding relevant images.
        """
        queries = []

        keywords = self._extract_keywords(extracted_data.entities)
        if keywords:
            for i in range(min(3, len(keywords))):
                queries.append(keywords[i])
            if len(keywords) >= 2:
                queries.append(f"{keywords[0]} {keywords[1]}")
            if len(keywords) >= 3:
                queries.append(f"{keywords[0]} {keywords[1]} {keywords[2]}")

        # Deduplicate
        return list(dict.fromkeys(queries))[:5]

    def _extract_keywords(self, entities: Optional[EntityModel]) -> List[str]:
        """
        Extract keywords from entities for image search.
        Ordered by score (highest first).
        """
        if not entities:
            return []

        candidate_labels = [
            "PERSON",
            "ORG",
            "GPE",
            "LOC",
            "EVENT",
            "LAW",
            "NORP",
            "PRODUCT",
            "WORK_OF_ART",
        ]

        # Collect (text, score) pairs
        keywords_with_scores = []
        for label in candidate_labels:
            ents: List[EntityAttributes] = getattr(entities, label, []) or []
            for ent in ents:
                if ent.score >= 0.85:  # threshold
                    text = ent.text.strip()
                    if 3 <= len(text) <= 40:
                        keywords_with_scores.append((text, ent.score))

        # Sort by score (highest first)
        keywords_with_scores.sort(key=lambda x: x[1], reverse=True)

        # Deduplicate, keep ordering
        seen = set()
        keywords = []
        for text, _ in keywords_with_scores:
            norm = text.lower()
            if norm not in seen:
                seen.add(norm)
                keywords.append(text)

        return keywords

    def _to_duckduckgo_region(self, lang: str, country: Optional[str] = None) -> str:
        """
        Convert language + optional country into DuckDuckGo region code.
        Example: en + US -> us-en, pt + BR -> br-pt, ru + RU -> ru-ru
        """
        lang = lang.lower()
        region = ""

        if country:
            try:
                region = pycountry.countries.lookup(country).alpha_2.lower()
            except LookupError:
                # maybe already like "br"
                region = country.lower()

        # Fallback defaults if no region provided
        if not region:
            return None

        return f"{region}-{lang}"

    def _regions_for_language(self, lang: str) -> List[str]:
        """
        Return all ISO-3166 territory codes where a language is spoken
        (official, de facto, or with population info) using Babel's CLDR.
        """
        lang = lang.lower()
        result = []
        territory_languages = get_global("territory_languages")

        for terr, langs in territory_languages.items():
            if lang in langs:
                result.append(terr.lower())

        return result

    def get_all_duckduckgo_regions(
        self, lang: str, country: Optional[str] = None
    ) -> List[str]:
        """
        Generate all possible DuckDuckGo region codes for a given language (+optional country).
        Rules:
        1) Always add default 'us-en'
        2) lang + explicit country
        3) lang + all regions where spoken
        4) english + explicit country
        """
        codes = set()
        lang = lang.lower()

        # 1) Default
        codes.add("us-en")

        # 2) lang + region code
        if country:
            code = self._to_duckduckgo_region(lang, country)
            if code:
                codes.add(code)

        # 3) lang + regions inferred from Babel
        if lang != "en":
            regions = self._regions_for_language(lang)
            for region in regions:
                code = self._to_duckduckgo_region(lang, region)
                if code:
                    codes.add(code)

        # 4) English + given region
        if country:
            code = self._to_duckduckgo_region("en", country)
            if code:
                codes.add(code)

        return sorted(codes)
