"""Web scraping module for MASX AI ETL CPU Pipeline."""
from .beautiful_soap_extractor import get_beautiful_soap_extractor
from .crawl4AI_extractor import Crawl4AIExtractor
from .web_scraper_utils import WebScraperUtils
from .trafilatura_extractor import get_trafilatura_extractor
from .unwrapped_url_resolver import get_final_url

__all__ = ["get_beautiful_soap_extractor", "Crawl4AIExtractor", "WebScraperUtils", "get_trafilatura_extractor", "get_final_url"]