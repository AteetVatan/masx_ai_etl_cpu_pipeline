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
import requests

class Crawl4AIExtractor:
    """
    This class contains the Crawl4AIExtractor class, which is a class that extracts content from a URL using the Crawl4AI API.
    """

    def __init__(self):
        self.logger = get_service_logger("Crawl4AIExtractor")

    def _get_crawl4ai_config(self):
        """
        Get the Crawl4AI configuration.
        """
        # prune_filter = PruningContentFilter(
        #     threshold_type="dynamic",
        #     threshold=0.4,
        #     min_word_threshold=60,   # a bit higher to drop boilerplate
        # )

        prune_filter = PruningContentFilter(
            threshold=0.20,  # Lower value retains more text
            threshold_type="fixed",  # Try switching from "dynamic" to "fixed"
            min_word_threshold=25,  # Increase threshold to focus on denser blocks
        )

        # md_generator = DefaultMarkdownGenerator(
        #     content_filter=prune_filter,
        #     options={"ignore_links": True, "ignore_images": True, "escape_html": True},
        # )

        md_generator = DefaultMarkdownGenerator(
            content_filter=prune_filter,
            options={"ignore_links": True, "escape_html": True},
        )

        c4a_script = """# Give banners time to render
        WAIT 2
        
        # Block navigation events that cause content changes
        EVAL `(() => {
            // Prevent navigation events
            window.addEventListener('beforeunload', (e) => e.preventDefault());
            window.addEventListener('unload', (e) => e.preventDefault());
            
            // Disable auto-refresh and navigation
            if (window.location.href.includes('news.google.com')) {
                // Google News specific handling
                const observer = new MutationObserver(() => {
                    // Block any navigation attempts
                    if (window.location.href !== window.location.href) {
                        window.stop();
                    }
                });
                observer.observe(document, {subtree: true, childList: true});
            }
        })()`

        # OneTrust accept
        IF (EXISTS `#onetrust-accept-btn-handler, .onetrust-accept-btn-handler`) THEN CLICK `#onetrust-accept-btn-handler, .onetrust-accept-btn-handler`

        # Quantcast accept
        IF (EXISTS `.qc-cmp2-container, .qc-cmp2-ui, .qc-cmp2-summary`) THEN CLICK `.qc-cmp2-accept-all, .qc-cmp2-summary-buttons .qc-cmp2-accept-all`

        # Generic cookie banners
        IF (EXISTS `[id*="cookie"], [class*="cookie"], .consent-banner`) THEN CLICK `.accept, .accept-all, [data-testid*="accept"]`

        # Fallback: click any visible button whose text contains "accept" (JS must use EVAL)
        EVAL `(() => {
        const btns = Array.from(document.querySelectorAll('button, [role="button"]'));
        const el = btns.find(b => /accept/i.test(b.textContent || ''));
        if (el) el.click();
        })()`

        # Remove overlays and unlock scroll (if site set overflow:hidden/backdrops)
        EVAL `(() => {
        const rm = s => document.querySelectorAll(s).forEach(el => el.remove());
        rm('#onetrust-banner-sdk, .onetrust-pc-dark-filter, .onetrust-pc-lightbox, .qc-cmp2-container, .qc-cmp2-ui, .qc-cmp2-summary, .consent-banner, [id*="cookie"], [class*="cookie"], .backdrop, .modal, .overlay');
        [document.documentElement, document.body].forEach(el => { el.style.overflow='visible'; el.style.position='static'; el.classList.remove('modal-open','scroll-locked'); });
        })()`

        # Let layout settle
        WAIT 0.5
        """

        # readiness probe for main content
        wait_for = "js:(() => !!document.querySelector('main, article, [role=main], .article, .article-body'))"

        generic_ready = """js:() => {
        // one-time setup
        if (!window.__masx) {
            window.__masx = {
            lastMut: performance.now(),
            stableMs: 1000,  // Increased stability window
            textMin: 300,    // Lower text requirement
            pMin: 2,         // Lower paragraph requirement
            navBlocked: false
            };
            const obs = new MutationObserver(() => { window.__masx.lastMut = performance.now(); });
            obs.observe(document, {subtree: true, childList: true, characterData: true, attributes: true});
        }

        if (document.readyState === 'loading' || !document.body) return false;

        // 1) Fast-path: if a landmark exists, we’re good
        const landmark = document.querySelector('main, article, [role=main], .article, .article-body, [itemprop="articleBody"]');
        if (landmark) {
            // require a tiny calm window so we don't read mid-route
            return performance.now() - window.__masx.lastMut > window.__masx.stableMs;
        }

        // 2) Otherwise rely on **lightweight** density checks (avoid innerText each poll)
        // Use a capped sample of textContent to reduce layout cost
        const tc = (document.body.textContent || '').trim();
        const textLen = tc.length;
        const pCount  = document.getElementsByTagName('p').length;

        const contentful = (textLen >= window.__masx.textMin) || (pCount >= window.__masx.pMin);

        // 3) Require short stability window
        const stable = performance.now() - window.__masx.lastMut > window.__masx.stableMs;
        return contentful && stable;
        }"""

        config = CrawlerRunConfig(
            markdown_generator=md_generator,
            wait_for=generic_ready,
            # wait_for_images=True,
            # adjust_viewport_to_content=True,
            # wait_for='js:() => !!document.querySelector("main, article, [role=\'main\'], .article, .article-body")',
            delay_before_return_html=2.5,  # <-- the “works in debug” delay, but explicit
            page_timeout=60000,  # ms — give slow SPAs time to settle
            scan_full_page=True,  # crawl beyond just the viewport
            # 1) Remove whole tags up front (kills inline JSON, scripts, styles)
            excluded_tags=["script", "style", "noscript"],
            # 2) Drop obvious chrome/banners/footers/headers
            # excluded_selector="header, footer, #cookie-banner, .cookie-banner, .consent-banner, .gdpr, .ads, .advert, .newsletter",
            excluded_selector=(
                "header, footer, nav, aside, "
                "#cookie-banner, .cookie-banner, .consent-banner, .gdpr, "
                ".cmp-container, .ads, .advert, .newsletter, .subscribe, "
                "[data-testid*='navigation'], [role='navigation']"
            ),
            # 3) Skip trivial text blocks (great for boilerplate)
            word_count_threshold=50,
            # 4) (Optional) interact to clear overlays
            c4a_script=c4a_script,
            # 5) Stable, reproducible runs
            cache_mode=CacheMode.BYPASS,  # or SMART if you want caching
        )
        return config

    async def crawl4ai_scrape(
        self, url: str, timeout_sec: int = 3600,  # maximum 1 minute
    ) -> ExtractResult:
        try:
            from src.scraping import WebScraperUtils

            config = self._get_crawl4ai_config()
            async with AsyncWebCrawler() as crawler:
                result = await crawler.arun(url=url, config=config, timeout=timeout_sec)
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

        config = self._get_crawl4ai_config()

        for attempt in range(1, max_retries + 1):
            try:
                async with AsyncWebCrawler() as crawler:
                    result = await crawler.arun(
                        url=url, config=config, timeout=timeout_sec
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
