"""
trafilatura_extractor.py - High-performance article extractor for MASX AI ETL CPU Pipeline.

This implementation relies solely on `trafilatura` for fetching and content extraction.
It wraps trafilatura in an async interface, adds retries, timeouts, and returns a
consistent schema (content + metadata) suitable for downstream processing.

Why trafilatura?
- Multilingual article extraction
- Boilerplate removal (cookie banners, nav, ads)
- Optional metadata extraction (title, author, date, main image)
"""

from __future__ import annotations
import os
import asyncio
import json
from functools import partial
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Union

import trafilatura
from trafilatura.utils import normalize
from trafilatura import fetch_response
from trafilatura.settings import use_config

from src.config import get_settings, get_service_logger
from src.models import ExtractResult
from src.scraping import WebScraperUtils

logger = get_service_logger(__name__)
settings = get_settings()


class ScrapingError(Exception):
    """Custom exception for scraping-related errors."""

    pass


class TrafilaturaExtractor:
    """
    High-performance article extractor using only trafilatura.
    - Async-safe via asyncio.to_thread
    - Retries with exponential backoff
    - Uses trafilatura's JSON output for richer metadata
    """

    def __init__(self) -> None:
        self.timeout = settings.request_timeout  # seconds
        self.max_retries = 0  # settings.retry_attempts
        self.retry_delay = settings.retry_delay  # base seconds
        self.min_chars = getattr(
            settings, "min_chars", 200
        )  # minimal content size to accept

        # Build a trafilatura config (optional but recommended)
        # Favor precision by default; tune if you want more recall.
        # self._config = trafilatura.settings.use_config(
        #     canonicalize=True,
        #     favor_recall=False,
        #     include_comments=False,
        #     include_tables=False,
        #     target_language=None,   # multilingual
        # )
        self._config = use_config()

        # User-Agent override: use your rotating UA (or leave default)
        self._user_agent = getattr(
            settings, "default_user_agent", "masx-enrich/1.0 (+https://example.com)"
        )

    async def scrape_article(
        self, url: str, proxy: str, user_agent: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch and extract an article using trafilatura with retries.

        Args:
            url: URL to scrape
            user_agent: Optional UA override (if you pass proxy UA strings)

        Returns:
            dict with keys: url, title, author, published_date, content, main_image, metadata, scraped_at, word_count

        Raises:
            ScrapingError on failure or insufficient content.
        """
        if not self._is_valid_url(url):
            raise ScrapingError(f"Invalid URL: {url[:50]}...")

        ua = user_agent or self._user_agent
        logger.info(f"[trafilatura] scraping: {url[:50]}...")

        last_err: Optional[BaseException] = None
        # if proxy:
        #     os.environ["http_proxy"] = f"http://{proxy}"
        #     os.environ["https_proxy"] = f"https://{proxy}"
        
        
        #trafilatura.downloads.PROXY_URL = f"http://{proxy}"

        for attempt in range(1 + self.max_retries):
            try:
                # resp = fetch_response(url, config=self._config)

                # fetch_url is synchronous → wrap with to_thread to avoid blocking event loop
                downloaded = await asyncio.wait_for(
                    asyncio.to_thread(trafilatura.fetch_url, url, config=self._config),
                    timeout=5,
                )

                if not downloaded:
                    downloaded = self.just_scrape(url, proxy)
                    if not downloaded:
                        raise ScrapingError("Empty response or blocked by site")

                result: ExtractResult = await self.trafilatura_from_html(
                    downloaded, url
                )
                logger.info(f"[trafilatura] success: {url[:50]}... (words={result.word_count})")
                return result

            except (asyncio.TimeoutError, ScrapingError) as e:
                last_err = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)
                    logger.warning(
                        f"[trafilatura] retry {attempt+1}/{self.max_retries} in {delay}s: {url[:50]}... ({e})"
                    )
                    await asyncio.sleep(delay)
                    continue
                break
            except Exception as e:
                last_err = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)
                    logger.error(
                        f"[trafilatura] unexpected error, retry {attempt+1}/{self.max_retries} in {delay}s: {url[:50]}... ({e})"
                    )
                    await asyncio.sleep(delay)
                    continue
                break

        logger.error(f"[trafilatura] failed to scrape {url[:50]}...: {last_err}")
        return None

    async def trafilatura_from_html(
        self, html: str, url: str
    ) -> Optional[Dict[str, Any]]:
        # Use JSON output to capture metadata
        # See: https://trafilatura.readthedocs.io/en/latest/usage-python.html#extract
        try:
            html = self._ensure_bytes(html)
            data = await asyncio.to_thread(
                partial(
                    trafilatura.bare_extraction,
                    html,
                    config=self._config,
                    include_images=True,
                    with_metadata=True,
                    favor_recall=True,
                    include_comments=False,
                    include_tables=False,
                    target_language=None,
                    url=url,
                )
            )
            if not data:
                raise ScrapingError("Trafilatura extraction returned no content")

            # if json_str:
            #     json_str = normalize("NFC",json_str)

            # data = json.loads(json_str)

            # Normalize fields
            content = (data.get("text") or "").strip()
            if WebScraperUtils.find_error_pattern(content):
                content = "error_pattern_found"

            # we will scrap not only content
            # but imgeas , title, author, published_date, metadata
            # if len(content) < self.min_chars:
            #     raise ScrapingError("Insufficient content extracted")

            title = (data.get("title") or "").strip()
            author = (data.get("author") or "").strip()
            published = (data.get("date") or "").strip()
            main_image = (data.get("image") or "").strip()
            hostname = (data.get("hostname") or "").strip()

            meta: Dict[str, Any] = {
                "url": data.get("source") or url,
                "language": data.get("language"),
                "sitename": data.get("sitename"),
                "categories": data.get("categories"),
                "links": data.get("links"),
            }

            result = ExtractResult(
                url=url,
                title=title,
                author=author,
                published_date=published,
                content=content,
                images=[main_image],
                hostname=hostname,
                metadata=meta,
                scraped_at=datetime.utcnow().isoformat(),
                word_count=len(content.split()),
            )
            return result
        except Exception as e:
            logger.error(f"Failed to scrape {url[:50]}...: {e}")
            return None

    def just_scrape(self, url, proxy):
        """
        Scrape the article using BeautifulSoup.
        """
        try:
            import requests

            headers = {"User-Agent": proxy}
            # url = "https://baijiahao.baidu.com/s?id=1834495451984756877"
            response = requests.get(
                url, headers=headers, timeout=self.timeout
            )  # 1 hour for complex pages
            response.encoding = response.apparent_encoding
            response.raise_for_status()
            return response.content
        except Exception as e:
            logger.error(f"Failed to scrape {url[:50]}...: {e}")
            return None

    @staticmethod
    def _ensure_bytes(s: Union[str, bytes]) -> bytes:
        if isinstance(s, bytes):
            return s
        return (s or "").encode("utf-8", "ignore")

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        # Minimal validation; upstream callers should validate too.
        return url.startswith(("http://", "https://")) and "." in url

    @staticmethod
    def _as_dict(res: ExtractResult) -> Dict[str, Any]:
        return {
            "url": res.url,
            "title": res.title,
            "author": res.author,
            "published_date": res.published_date,
            "content": res.content,
            "images": ([{"url": image} for image in res.images] if res.images else []),
            "metadata": res.metadata,
            "scraped_at": res.scraped_at,
            "word_count": res.word_count,
            "hostname": res.hostname,
        }
