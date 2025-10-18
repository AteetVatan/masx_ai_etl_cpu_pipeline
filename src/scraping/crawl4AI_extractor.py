# ┌───────────────────────────────────────────────────────────────┐
# │  Copyright (c) 2025 Ateet Vatan Bahmani                       │
# │  Project: MASX AI – Strategic Agentic AI System               │
# │  All rights reserved.                                         │
# └───────────────────────────────────────────────────────────────┘
#
# MASX AI is a proprietary software system developed and owned by Ateet Vatan Bahmani.
# The source code, documentation, workflows, designs, and naming (including "MASX AI")
# are protected by applicable copyright and trademark laws.
#
# Redistribution, modification, commercial use, or publication of any portion of this
# project without explicit written consent is strictly prohibited.
#
# This project is not open-source and is intended solely for internal, research,
# or demonstration use by the author.
#
# Contact: ab@masxai.com | MASXAI.com

"""
This module contains the Crawl4AIExtractor class, which is a class that extracts content from a URL using the Crawl4AI API.
"""

import asyncio
from asyncio import TimeoutError

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

from src.models import ExtractResult
from src.config import get_service_logger
from .crawl4AI_extractor_configs import Crawl4AIExtractorConfigs as c4a_configs

class Crawl4AIExtractor:
    """
    This class contains the Crawl4AIExtractor class, which is a class that extracts content from a URL using the Crawl4AI API.
    """

    def __init__(self, proxy: dict | None = None):
        self.logger = get_service_logger("Crawl4AIExtractor")
        self.proxy = proxy


    async def _try_once(self, url: str, browser_cfg: BrowserConfig, run_cfg: CrawlerRunConfig, timeout_sec: int):        
        async with AsyncWebCrawler() as crawler:
            return await crawler.arun(url=url,browser_config=browser_cfg, config=run_cfg, timeout=timeout_sec)

    async def crawl4ai_scrape(
        self, url: str, timeout_sec: int = 3600
    ):
        from src.scraping import WebScraperUtils  # your existing utility

        #is_gnews = c4a_configs.is_google_news_url(url)
        #run_cfg = c4a_configs.get_run_config(is_gnews)
        run_cfg = c4a_configs.get_crawl4ai_config()
        browsers = c4a_configs.get_browser_presets()

        last_err = None
        browser_config_index = 0
        for browser_cfg in browsers:
            try:
                browser_config_index += 1
                result = await self._try_once(url, browser_cfg, run_cfg, timeout_sec)               
                if not result or not result.success:
                    raise RuntimeError(result.error_message or "unknown error")

                scrap_result = await self.trafilatura_from_html(result.cleaned_html, url)
                cleaned = WebScraperUtils.remove_ui_junk(scrap_result.content)
                scrap_result.content = "error_pattern_found" if WebScraperUtils.find_error_pattern(cleaned) else cleaned
                scrap_result.word_count = len(scrap_result.content.split())
                return scrap_result

            except Exception as e:
                self.logger.error(f"[C4AI] Error for browser config {browser_config_index}: {e}")
                last_err = e
                #await asyncio.sleep(1.5)

        self.logger.error(f"[C4AI] All attempts failed for {url}: {last_err}")
        return None


    

    async def crawl4ai_scrape_old(
        self, url: str, timeout_sec: int = 3600,  # maximum 1 minute
    ) -> ExtractResult:
        try:
            from src.scraping import WebScraperUtils

            config = c4a_configs.get_crawl4ai_config()
            async with AsyncWebCrawler() as crawler:
                result = await crawler.arun(url=url, config=config, browser_config=c4a_configs.get_crawl4ai_browser_config(), timeout=timeout_sec)
            if not result:
                raise RuntimeError("Crawler returned no result")

            if not result.success:
                raise RuntimeError(
                    f"Crawl failed with error: {result.error_message or 'unknown error'}"
                )

            scrap_result: ExtractResult = await self.trafilatura_from_html(
                result.cleaned_html, url
            )
            images = WebScraperUtils.extract_image_urls(scrap_result.content)
            scrap_result.images = images
            if WebScraperUtils.find_error_pattern(scrap_result.content):
                scrap_result.content = "error_pattern_found"
            cleaned = WebScraperUtils.remove_ui_junk(scrap_result.content)

            scrap_result.content = cleaned
            word_count = len(cleaned.split())
            scrap_result.word_count = word_count
            return scrap_result

        except TimeoutError:
            self.logger.warning(
                f"crawl4AI_extractor.py:Crawl4AIExtractor:timed out after {timeout_sec}s for URL: {url}"
            )
        except Exception as e:
            self.logger.error(
                f"crawl4AI_extractor.py:Crawl4AIExtractor:failed for URL {url}: {e}"
            )

        self.logger.error(
            f"crawl4AI_extractor.py:Crawl4AIExtractor:crawl attempts failed for URL: {url}"
        )
        return None

    async def crawl4ai_scrape_with_retry(
        self,
        url: str,
        max_retries: int = 1,
        timeout_sec: int = 3600,  # maximum 1 minute
    ) -> ExtractResult:
        from src.scraping import WebScraperUtils

        config = c4a_configs.get_crawl4ai_config()

        for attempt in range(1, max_retries + 1):
            try:
                async with AsyncWebCrawler() as crawler:
                    result = await crawler.arun(
                        url=url,
                        config=config, 
                        browser_config=c4a_configs.get_crawl4ai_browser_config(),
                        timeout=timeout_sec
                    )
                if not result:
                    raise RuntimeError("Crawler returned no result")

                if not result.success:
                    raise RuntimeError(
                        f"Crawl failed with error: {result.error_message or 'unknown error'}"
                    )  

                scrap_result: ExtractResult = await self.trafilatura_from_html(
                    result.cleaned_html, url
                )
                # images = WebScraperUtils.extract_image_urls(scrap_result.content)
                # scrap_result.images = images
                if WebScraperUtils.find_error_pattern(scrap_result.content):
                    scrap_result.content = "error_pattern_found"
                cleaned = WebScraperUtils.remove_ui_junk(scrap_result.content)

                scrap_result.content = cleaned
                word_count = len(cleaned.split())
                scrap_result.word_count = word_count
                return scrap_result

            except TimeoutError:
                self.logger.warning(
                    f"crawl4AI_extractor.py:Crawl4AIExtractor:Attempt {attempt} timed out after {timeout_sec}s for URL: {url}"
                )
            except Exception as e:
                self.logger.error(
                    f"crawl4AI_extractor.py:Crawl4AIExtractor:Attempt {attempt} failed for URL {url}: {e}"
                )

            # after last attempt
            if attempt < max_retries:
                await asyncio.sleep(2**attempt)  # exponential back-off

        self.logger.error(
            f"crawl4AI_extractor.py:Crawl4AIExtractor:All {max_retries} crawl attempts failed for URL: {url}"
        )
        return None

    async def trafilatura_from_html(self, html: str, url: str) -> ExtractResult:
        try:
            from src.scraping import TrafilaturaExtractor

            trafilatura_extractor = TrafilaturaExtractor()
            result: ExtractResult = await trafilatura_extractor.trafilatura_from_html(
                html, url
            )
            return result
        except Exception as e:
            self.logger.error(f"Failed to scrape {url}: {e}")
            return None

    async def beautifulSoup_from_html(self, html: str, url: str) -> ExtractResult:
        try:
            from src.scraping import BeautifulSoupExtractor

            beautiful_soap_extractor = BeautifulSoupExtractor()
            result: ExtractResult = (
                await beautiful_soap_extractor.beautifulSoup_from_html(html, url)
            )
            return result
        except Exception as e:
            self.logger.error(f"Failed to scrape {url}: {e}")
            return None
