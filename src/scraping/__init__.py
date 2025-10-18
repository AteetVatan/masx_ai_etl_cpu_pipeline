"""Web scraping module for MASX AI ETL CPU Pipeline."""
from .beautiful_soap_extractor import BeautifulSoupExtractor
from .crawl4AI_extractor import Crawl4AIExtractor
from .web_scraper_utils import WebScraperUtils
from .trafilatura_extractor import TrafilaturaExtractor
from .error_patterns import ERROR_REGEX

__all__ = [
    "BeautifulSoupExtractor",
    "Crawl4AIExtractor",
    "WebScraperUtils",
    "TrafilaturaExtractor",
    "ERROR_REGEX",
]
