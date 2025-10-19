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

"""This module handles all article-related operations in the MASX AI News ETL pipeline."""


from random import choice
import asyncio

from src.services import ProxyService
from src.models import ExtractResult

from src.config import settings
from src.config import get_service_logger
from src.scraping import (
    Crawl4AIExtractor,
    BeautifulSoupExtractor,
    TrafilaturaExtractor,
)
from src.scraping import WebScraperUtils


class NewsContentExtractor:
    """
    Extracts raw text from article URLs using BeautifulSoup and Crawl4AI as fallback.
    """

    def __init__(self):
        # self.news_articles = self.context.pull(DagContextEnum.NEWS_ARTICLES.value)
        self.settings = settings
        self.crawl4AIExtractor = Crawl4AIExtractor()
        self.logger = get_service_logger("NewsContentExtractor")
        self.proxy_service = ProxyService.get_instance()

    async def extract_feed(self, url: str) -> ExtractResult:
        """
        Extract raw text for each article using proxy-enabled scraping with async concurrency.
        """
        try:
            self.logger.info(
                f"news_content_extractor.py:NewsContentExtractor:---- ProxyService initiated ----"
            )
            # proxies = await ProxyManager.proxies_async()

            proxies = await self.proxy_service.get_proxy_cache(force_refresh=True)
            

            self.logger.info(
                f"news_content_extractor.py:NewsContentExtractor:---- {len(proxies)} proxies found ----"
            )

            if not proxies:
                self.logger.error(
                    "news_content_extractor.py:NewsContentExtractor:No valid proxies found in context."
                )
                raise ValueError("No valid proxies found in context.")

            result = await self._scrape_multilang_feeds(url, proxies)

            return result
        except Exception as e:
            self.logger.error(
                f"news_content_extractor.py:NewsContentExtractor:Batch processing failed: {e}"
            )
            raise Exception(f"extract_feed processing failed: {e}")
        finally:
            pass

    async def _scrape_multilang_feeds(
        self, url: str, proxies: list[str]
    ) -> ExtractResult:
        """
        Use BeautifulSoup first, fallback to Crawl4AI if needed.
        """
        try:
            # url = "https://www.ihu.unisinos.br/656066-negacionismo-parlamentar-poe-no-lixo-44-anos-da-politica-ambiental-brasileira-e-pl-da-devastacao-abre-brecha-a-criacao-de-vales-da-morte-de-norte-a-sul-entrevista-especial-com-suely-araujo"

            # url = await WebScraperUtils.resolve_news_url_async(url)
            # url = "https://mbd.baidu.com/newspage/data/landingsuper?context=%7B%22nid%22%3A%22news_8662219630019457105%22,%22sourceFrom%22%3A%22wise_feedlist%22%7D"

            self.logger.info(f"NewsContentExtractor:Scraping ------ {url[:50]}...")

            # Step 1: Try trafilatura extraction

            try:
                trafilatura_extractor = TrafilaturaExtractor()
                proxy = choice(proxies)
                traf_result: ExtractResult = await trafilatura_extractor.scrape_article(
                    url, proxy
                )

                if traf_result and traf_result.word_count > 1000:
                    self.logger.info(
                        f"news_content_extractor.py:NewsContentExtractor:Successfully scraped via trafilatura: {url[:50]}..."
                    )
                    return traf_result

                else:
                    self.logger.info(
                        f"trafilatura extractor :unsuccessfull - scraped failed or scraped content is very less, falling back to crawlfor AI"
                    )

            except Exception as e:
                self.logger.error(
                    f"NewsContentExtractor:Trafilatura scraping failed for {url[:50]}...: {e}"
                )

            # Step 2: Fallback to Crawl4AI
            self.logger.info(
                f"NewsContentExtractor:[Fallback] Invoking Crawl4AI for: {url[:50]}..."
            )
            try:                
                # first try quick crawl4ai scrape
                try:
                    crawl_result: ExtractResult = await self.crawl4AIExtractor.crawl4ai_scrape(url)
                    crawl_result = None
                except Exception as e:
                    self.logger.error(f"NewsContentExtractor:Normal Crawl4AI scraping failed for {url[:50]}...: {e}")
                    
                # if not successful, try with proxy and retry with longer timeout
                if not crawl_result and len(proxies) > 0:
                    proxy = choice(proxies)
                    crawl_result: ExtractResult = (
                        await self.crawl4AIExtractor.crawl4ai_scrape_with_retry_and_proxy(url, proxies)
                    )
                    if crawl_result and crawl_result.word_count < 2000:
                        crawl_result = None
                        
                        
                if not crawl_result:  # sanity check
                    raise ValueError("Crawl4AI returned empty or too short content.")

                self.logger.info(
                    f"news_content_extractor.py:NewsContentExtractor:Successfully scraped via Crawl4AI: {url[:50]}..."
                )

                # if traf_result is not None, then merge traf_result and crawl_result
                final_result = self._merge_results(traf_result, crawl_result)
                if final_result:
                    self.logger.info(f" crawl4ai successfull")
                else:
                    self.logger.error(f"crawl4ai failed")

                return final_result

            except Exception as c4_err:
                self.logger.error(
                    f"news_content_extractor.py:NewsContentExtractor:Crawl4AI scraping failed for {url[:50]}...: {c4_err}"
                )
                raise Exception(f"Crawl4AI scraping failed for {url[:50]}...: {c4_err}")

        except Exception as e:
            self.logger.error(
                f"news_content_extractor.py:NewsContentExtractor:[Error] Failed to scrape {url[:50]}...: {e}",
                exc_info=True,
            )
            raise Exception(f"Failed to scrape {url[:50]}...: {e}")

    def _merge_results(
        self, traf_result: ExtractResult, crawl_result: ExtractResult
    ) -> ExtractResult:
        """
        Merge traf_result attributeto crawl_result if crawl_result attribute is  None.
        """

        if traf_result:
            if not crawl_result.author and traf_result.author:
                crawl_result.author = traf_result.author
            if not crawl_result.published_date and traf_result.published_date:
                crawl_result.published_date = traf_result.published_date
            if not crawl_result.images[0] and traf_result.images:
                crawl_result.images = traf_result.images
            if not crawl_result.content and traf_result.content:
                crawl_result.content = traf_result.content
            if not crawl_result.scraped_at and traf_result.scraped_at:
                crawl_result.scraped_at = traf_result.scraped_at

        return crawl_result
