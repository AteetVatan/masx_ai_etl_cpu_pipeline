"""
Pipeline manager for MASX AI ETL CPU Pipeline.

Orchestrates the complete article processing pipeline with parallel execution,
error handling, and batch operations for high-performance processing.
"""

import asyncio
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import logging

from ..db.db_client import db_client, DatabaseError
from ..scraping.scraper import scraper, ScrapingError
from ..scraping.fallback_crawl4ai import fallback_scraper
from ..processing.cleaner import text_cleaner
from ..processing.geotagger import geotagger
from ..processing.image_finder import image_finder
from ..utils.threadpool import thread_pool
from ..config.settings import settings


logger = logging.getLogger(__name__)


class PipelineManager:
    """
    High-performance pipeline manager for article processing.
    
    Orchestrates the complete pipeline: scrape → clean → geotag → find images,
    with parallel processing, error handling, and batch operations.
    """
    
    def __init__(self):
        """Initialize the pipeline manager."""
        self.batch_size = settings.batch_size
        self.max_workers = settings.max_workers
        self.retry_attempts = settings.retry_attempts
        self.retry_delay = settings.retry_delay
        
        # Pipeline statistics
        self.stats = {
            "total_processed": 0,
            "successful": 0,
            "failed": 0,
            "start_time": None,
            "last_activity": None
        }
        
        # Initialize thread pool
        thread_pool.start()
        
        logger.info("Pipeline manager initialized")
    
    async def process_article(self, article_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single article through the complete pipeline.
        
        Args:
            article_data: Article data with URL and metadata
            
        Returns:
            Dictionary containing processing results
        """
        article_id = article_data.get("id", "unknown")
        url = article_data.get("url", "")
        
        logger.info(f"Starting pipeline processing for article {article_id}: {url}")
        
        start_time = time.time()
        processing_steps = []
        errors = []
        
        try:
            # Step 1: Scrape article content
            logger.debug(f"Step 1: Scraping article {article_id}")
            scraped_data = await self._scrape_article(url)
            processing_steps.append("scraping")
            
            # Step 2: Clean text content
            logger.debug(f"Step 2: Cleaning text for article {article_id}")
            cleaned_data = await self._clean_text(scraped_data)
            processing_steps.append("cleaning")
            
            # Step 3: Extract geographic entities
            logger.debug(f"Step 3: Geotagging article {article_id}")
            geotagged_data = await self._geotag_article(cleaned_data)
            processing_steps.append("geotagging")
            
            # Step 4: Find relevant images
            logger.debug(f"Step 4: Finding images for article {article_id}")
            enriched_data = await self._find_images(geotagged_data)
            processing_steps.append("image_search")
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Update statistics
            self.stats["total_processed"] += 1
            self.stats["successful"] += 1
            self.stats["last_activity"] = datetime.utcnow()
            
            result = {
                "article_id": article_id,
                "status": "completed",
                "processing_time": processing_time,
                "processing_steps": processing_steps,
                "enriched_data": enriched_data,
                "errors": errors,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            logger.info(f"Successfully processed article {article_id} in {processing_time:.2f}s")
            return result
            
        except Exception as e:
            # Update statistics
            self.stats["total_processed"] += 1
            self.stats["failed"] += 1
            self.stats["last_activity"] = datetime.utcnow()
            
            error_msg = f"Pipeline processing failed for article {article_id}: {str(e)}"
            logger.error(error_msg)
            
            return {
                "article_id": article_id,
                "status": "failed",
                "processing_time": time.time() - start_time,
                "processing_steps": processing_steps,
                "enriched_data": None,
                "errors": [error_msg],
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def process_batch(self, article_ids: List[str]) -> Dict[str, Any]:
        """
        Process a batch of articles in parallel.
        
        Args:
            article_ids: List of article IDs to process
            
        Returns:
            Dictionary containing batch processing results
        """
        logger.info(f"Starting batch processing for {len(article_ids)} articles")
        
        start_time = time.time()
        results = []
        successful = 0
        failed = 0
        
        try:
            # Fetch articles from database
            articles = await self._fetch_articles_batch(article_ids)
            
            if not articles:
                logger.warning("No articles found for batch processing")
                return {
                    "status": "completed",
                    "total_articles": len(article_ids),
                    "processed": 0,
                    "successful": 0,
                    "failed": 0,
                    "processing_time": time.time() - start_time,
                    "results": []
                }
            
            # Process articles in parallel using thread pool
            tasks = []
            for article in articles:
                task = thread_pool.submit_task(
                    self.process_article,
                    article,
                    task_name=f"process_article_{article.get('id', 'unknown')}"
                )
                tasks.append(task)
            
            # Wait for all tasks to complete
            for task in tasks:
                try:
                    result = await asyncio.wrap_future(task)
                    results.append(result)
                    
                    if result["status"] == "completed":
                        successful += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"Task failed: {e}")
                    failed += 1
                    results.append({
                        "article_id": "unknown",
                        "status": "failed",
                        "processing_time": 0,
                        "processing_steps": [],
                        "enriched_data": None,
                        "errors": [str(e)],
                        "timestamp": datetime.utcnow().isoformat()
                    })
            
            # Update database with results
            await self._update_articles_batch(results)
            
            processing_time = time.time() - start_time
            
            logger.info(f"Batch processing completed: {successful} successful, {failed} failed in {processing_time:.2f}s")
            
            return {
                "status": "completed",
                "total_articles": len(article_ids),
                "processed": len(results),
                "successful": successful,
                "failed": failed,
                "processing_time": processing_time,
                "results": results
            }
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            return {
                "status": "failed",
                "total_articles": len(article_ids),
                "processed": len(results),
                "successful": successful,
                "failed": failed,
                "processing_time": time.time() - start_time,
                "error": str(e),
                "results": results
            }
    
    async def _scrape_article(self, url: str) -> Dict[str, Any]:
        """Scrape article content with fallback."""
        try:
            # Try primary scraper first
            return await scraper.scrape_article(url)
        except ScrapingError as e:
            logger.warning(f"Primary scraper failed for {url}: {e}")
            
            # Try fallback scraper
            try:
                return await fallback_scraper.scrape_article(url)
            except Exception as fallback_error:
                logger.error(f"Fallback scraper also failed for {url}: {fallback_error}")
                raise ScrapingError(f"Both scrapers failed: {e}, {fallback_error}")
    
    async def _clean_text(self, scraped_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize text content."""
        content = scraped_data.get("content", "")
        language = scraped_data.get("language", "en")
        
        # Clean the text
        cleaned_result = text_cleaner.clean_text(content, language)
        
        # Update scraped data with cleaned content
        scraped_data["content"] = cleaned_result["cleaned_text"]
        scraped_data["cleaning_metadata"] = {
            "original_length": cleaned_result["original_length"],
            "cleaned_length": cleaned_result["cleaned_length"],
            "removed_elements": cleaned_result["removed_elements"],
            "compression_ratio": cleaned_result["compression_ratio"]
        }
        
        return scraped_data
    
    async def _geotag_article(self, article_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract geographic entities from article."""
        content = article_data.get("content", "")
        language = article_data.get("language", "en")
        
        # Extract geographic entities
        geo_entities = geotagger.extract_geographic_entities(content, language)
        
        # Add geographic data to article
        article_data["geographic_entities"] = geo_entities
        
        return article_data
    
    async def _find_images(self, article_data: Dict[str, Any]) -> Dict[str, Any]:
        """Find relevant images for the article."""
        title = article_data.get("title", "")
        content = article_data.get("content", "")
        language = article_data.get("language", "en")
        
        # Generate search queries
        search_queries = image_finder.generate_search_queries(title, content, language)
        
        # Find images using the best query
        images_data = None
        for query in search_queries:
            try:
                images_data = await image_finder.find_images(query, max_images=3, language=language)
                if images_data.get("images"):
                    break
            except Exception as e:
                logger.warning(f"Image search failed for query '{query}': {e}")
                continue
        
        # Add image data to article
        article_data["images"] = images_data.get("images", []) if images_data else []
        article_data["image_search_metadata"] = {
            "queries_used": search_queries,
            "search_method": images_data.get("search_method", "none") if images_data else "none",
            "total_found": images_data.get("total_found", 0) if images_data else 0
        }
        
        return article_data
    
    async def _fetch_articles_batch(self, article_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch articles from database by IDs."""
        try:
            # Connect to database if not already connected
            if not db_client.client:
                await db_client.connect()
            
            articles = []
            for article_id in article_ids:
                article = await db_client.fetch_article_by_id(article_id)
                if article:
                    articles.append(article)
            
            return articles
            
        except DatabaseError as e:
            logger.error(f"Failed to fetch articles: {e}")
            raise
    
    async def _update_articles_batch(self, results: List[Dict[str, Any]]) -> None:
        """Update articles in database with processing results."""
        try:
            # Connect to database if not already connected
            if not db_client.client:
                await db_client.connect()
            
            updates = []
            for result in results:
                if result["status"] == "completed":
                    update_data = {
                        "id": result["article_id"],
                        "status": "completed",
                        "enriched_data": result["enriched_data"],
                        "processing_time": result["processing_time"],
                        "processing_steps": result["processing_steps"]
                    }
                else:
                    update_data = {
                        "id": result["article_id"],
                        "status": "failed",
                        "error_message": "; ".join(result["errors"]),
                        "processing_time": result["processing_time"]
                    }
                
                updates.append(update_data)
            
            if updates:
                successful_updates, failed_updates = await db_client.update_articles_batch(updates)
                logger.info(f"Updated {successful_updates} articles, {failed_updates} failed")
            
        except DatabaseError as e:
            logger.error(f"Failed to update articles: {e}")
            raise
    
    async def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics."""
        thread_pool_stats = thread_pool.get_stats()
        
        return {
            "pipeline_stats": self.stats,
            "thread_pool_stats": thread_pool_stats,
            "database_stats": await self._get_database_stats(),
            "uptime": time.time() - (self.stats["start_time"] or time.time())
        }
    
    async def _get_database_stats(self) -> Dict[str, Any]:
        """Get database processing statistics."""
        try:
            if not db_client.client:
                await db_client.connect()
            
            return await db_client.get_processing_stats()
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {"error": str(e)}
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check."""
        health_status = {
            "overall": "healthy",
            "components": {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Check thread pool
        thread_pool_healthy = thread_pool.is_healthy()
        health_status["components"]["thread_pool"] = {
            "status": "healthy" if thread_pool_healthy else "unhealthy",
            "details": thread_pool.get_stats()
        }
        
        # Check database connection
        try:
            if not db_client.client:
                await db_client.connect()
            
            await db_client._test_connection()
            health_status["components"]["database"] = {
                "status": "healthy",
                "details": "Connected successfully"
            }
        except Exception as e:
            health_status["components"]["database"] = {
                "status": "unhealthy",
                "details": str(e)
            }
            health_status["overall"] = "unhealthy"
        
        # Check processing modules
        health_status["components"]["scraper"] = {
            "status": "healthy",
            "details": "Available"
        }
        
        health_status["components"]["text_cleaner"] = {
            "status": "healthy",
            "details": "Available"
        }
        
        health_status["components"]["geotagger"] = {
            "status": "healthy" if geotagger.enabled else "disabled",
            "details": "Available" if geotagger.enabled else "Disabled"
        }
        
        health_status["components"]["image_finder"] = {
            "status": "healthy" if image_finder.enabled else "disabled",
            "details": "Available" if image_finder.enabled else "Disabled"
        }
        
        return health_status
    
    async def shutdown(self):
        """Gracefully shutdown the pipeline manager."""
        logger.info("Shutting down pipeline manager")
        
        # Shutdown thread pool
        thread_pool.shutdown(wait=True, timeout=30)
        
        # Disconnect from database
        await db_client.disconnect()
        
        logger.info("Pipeline manager shutdown completed")


# Global pipeline manager instance
pipeline_manager = PipelineManager()
