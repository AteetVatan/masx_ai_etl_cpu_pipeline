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
import asyncio
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from crawl4ai import CrawlerRunConfig, CacheMode, BrowserConfig, ProxyConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter
from .simple_proxy_rotator import SimpleProxyRotator
from src.config import get_settings


class Crawl4AIExtractorConfigs:
    # ------------------------------------------------------------------------------
    # DOMAIN DETECTOR
    # ------------------------------------------------------------------------------
    @staticmethod
    def is_google_news_url(url: str) -> bool:
        try:
            host = urlparse(url).netloc
            return host.startswith("news.google.") or host.endswith(".news.google.com")
        except Exception:
            return False

    # ------------------------------------------------------------------------------
    # BROWSER CONFIG PRESETS
    # ------------------------------------------------------------------------------

    @staticmethod
    def get_crawl4ai_browser_config():
        browser_cfg = BrowserConfig(
            headless=True,
            extra_args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-setuid-sandbox",
                "--disable-software-rasterizer",
            ],
            ignore_https_errors=True,
            enable_stealth=True,  # helps bypass bot detection
        )
        return browser_cfg

    @staticmethod
    def get_browser_presets() -> List[BrowserConfig]:
        """
        Returns a prioritized list of BrowserConfig objects for resilient crawling.
        1. Undetected + stealth (best for Google)
        2. Firefox
        3. Chromium
        """
        base_args = [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--disable-blink-features=AutomationControlled",
        ]

        configs = [
            Crawl4AIExtractorConfigs.get_crawl4ai_browser_config(),
            BrowserConfig(
                browser_type="undetected",
                headless=True,
                enable_stealth=True,
                ignore_https_errors=True,
                extra_args=base_args,
            ),
            BrowserConfig(
                browser_type="firefox",
                headless=True,
                ignore_https_errors=True,
                extra_args=["--no-sandbox", "--disable-dev-shm-usage"],
            ),
            BrowserConfig(
                browser_type="chromium",
                headless=True,
                enable_stealth=True,
                ignore_https_errors=True,
                extra_args=base_args,
            ),
        ]
        return configs

    # ------------------------------------------------------------------------------
    # RUN CONFIG PRESETS
    # ------------------------------------------------------------------------------
    @staticmethod
    def get_crawl4ai_config(proxies: list[str] = None):
        """
        Get the Crawl4AI configuration.
        """
        # prune_filter = PruningContentFilter(
        #     threshold_type="dynamic",
        #     threshold=0.4,
        #     min_word_threshold=60,   # a bit higher to drop boilerplate
        # )
        settings = get_settings()
        rotator = None
        if proxies:
            rotator = SimpleProxyRotator(proxies)

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

        common_kwargs = dict(
            log_console=settings.debug,
            markdown_generator=md_generator,
            wait_for=generic_ready,
            # wait_for_images=True,
            # adjust_viewport_to_content=True,
            # wait_for='js:() => !!document.querySelector("main, article, [role=\'main\'], .article, .article-body")',
            delay_before_return_html=2.5,  # <-- the “works in debug” delay, but explicit
            page_timeout=100000,  # ms — give slow SPAs time to settle
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
        if rotator:
            config = CrawlerRunConfig(proxy_rotation_strategy=rotator, **common_kwargs)
        else:
            config = CrawlerRunConfig(**common_kwargs)
        return config

    @staticmethod
    def get_run_config(is_gnews: bool) -> CrawlerRunConfig:
        """Returns a tuned CrawlerRunConfig for either normal or Google redirect pages."""

        def _markdown_pruner():
            prune_filter = PruningContentFilter(
                threshold=0.20, threshold_type="fixed", min_word_threshold=25
            )
            return DefaultMarkdownGenerator(
                content_filter=prune_filter,
                options={"ignore_links": True, "escape_html": True},
            )

        if is_gnews and False:
            # Fast-fail config for redirect-heavy pages
            return CrawlerRunConfig(
                markdown_generator=_markdown_pruner(),
                cache_mode=CacheMode.BYPASS,
                page_timeout=8000,  # 8 seconds max
                wait_until="commit",
                scan_full_page=False,
                excluded_tags=["script", "style", "noscript"],
                excluded_selector="header, footer, nav, aside, .cookie-banner, .consent-banner",
                word_count_threshold=50,
            )

        return Crawl4AIExtractorConfigs.get_crawl4ai_config()

        # Default config for real articles
        return CrawlerRunConfig(
            markdown_generator=_markdown_pruner(),
            cache_mode=CacheMode.BYPASS,
            page_timeout=60000,
            wait_until="domcontentloaded",
            scan_full_page=True,
            excluded_tags=["script", "style", "noscript"],
            excluded_selector="header, footer, nav, aside, .cookie-banner, .consent-banner",
            word_count_threshold=50,
        )
