"""
Pipeline manager for MASX AI ETL CPU Pipeline.

Orchestrates the complete article processing pipeline with parallel execution,
error handling, and batch operations for high-performance processing.
"""

import asyncio
import time
from typing import Dict, Any, List, Optional, Tuple
import random
from datetime import datetime


from src.db import db_connection, DatabaseError
from src.processing import NewsContentExtractor,EntityTagger, TextCleaner, Geotagger, ImageFinder, ImageDownloader
from src.services import ProxyService, TranslationManager
from src.utils.threadpool import thread_pool
from src.config import get_service_logger, get_settings
from src.models import FeedModel, ExtractResult, EntityModel, EntityAttributes, GeoEntity
from src.utils import NlpUtils, LanguageUtils


logger = get_service_logger(__name__)
settings = get_settings()


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
        
        self.news_content_extractor = NewsContentExtractor()
        self.text_cleaner = TextCleaner()
        self.geotagger = Geotagger()
        self.entity_tagger = EntityTagger()
        self.image_finder = ImageFinder()
        self.translation_manager = TranslationManager()
        self.image_downloader = ImageDownloader()
        
        #service
        self.proxy_service = ProxyService.get_instance()
        self.nlp_utils = NlpUtils()
        
        asyncio.run(self.proxy_service.ping_start_proxy())
        
        
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
    
    async def process_article(self, article_data: FeedModel, date: str) -> Dict[str, Any]:
        """
        Process a single article through the complete pipeline.
        
        Args:
            article_data: Article data with URL and metadata
            
        Returns:
            Dictionary containing processing results
        """
        article_id = article_data.id
        flashpoint_id = article_data.flashpoint_id
        url = article_data.url
        title = article_data.title
        original_image = article_data.original_image

        logger.info(f"Starting pipeline processing for article {article_id}: {url}")
        
        start_time = time.time()
        processing_steps = []
        errors = []
        extracted_data: ExtractResult = None
        try:
            
            # test = 'Negacionismo parlamentar põe no lixo 44 anos da política ambiental brasileira e PL da Devastação abre brecha à criação de "vales da morte" de Norte a Sul. Entrevista especial com Suely Araújo'
            # #test = 'Negacionismo parlamentar põe no lixo 44 anos da'
            # test_res = await self.translation_manager.translate(test, target="en")
            # logger.info(f"Translation: {test_res}")
           
            extracted_data = ExtractResult()
            extracted_data.id = article_id
            extracted_data.parent_id = flashpoint_id
            extracted_data.url = url
            extracted_data.title = title
            extracted_data.images = [original_image]
            extracted_data.article_source_country = article_data.source_country
            
            
            # Step 1: Scrape article content
            logger.debug(f"Step 1: Scraping article {article_id}")
            extracted_data = await self._scrape_article(extracted_data)
            processing_steps.append("scraping")
            
            # Step 2: Set the language of the extracted data
            logger.debug(f"Step 2: Setting the language of the article {article_id}")
            extracted_data = await self._set_extracted_language(extracted_data)
            processing_steps.append("language_setting")            
            
            # Step 3: Translate title 
            logger.debug(f"Step 3: Translating title and language detection for article {article_id}")
            extracted_data = await self._translate_title(extracted_data)
            processing_steps.append("translation")
                        
            # Step 4: Metadata extraction
            logger.debug(f"Step 4: Extracting entities for article {article_id}")
            extracted_data = await self._extract_entities(extracted_data)
            processing_steps.append("entity_extraction")
            
            
            # Step 5: Extract geographic entities
            logger.debug(f"Step 5: Geotagging article {article_id}")
            extracted_data = await self._geotag_article(extracted_data)
            processing_steps.append("geotagging")
            
            # Step 6: Find relevant images
            logger.debug(f"Step 6: Finding images for article {article_id}") 
            extracted_data = await self._find_images(extracted_data)
            processing_steps.append("image_search")
            
            
            # Step 7: download images to supabase
            logger.debug(f"Step 7: Downloading images to supabase for article {article_id}")
            extracted_data = await self._download_images(date, flashpoint_id, extracted_data)
            processing_steps.append("image_download")
   
            
            #here update the article data with the extracted data
            article_data.title = extracted_data.title
            article_data.title_en = extracted_data.title_en
            article_data.language = extracted_data.language
            article_data.author = extracted_data.author
            article_data.published_date = extracted_data.published_date
            article_data.content = extracted_data.content
            article_data.images = extracted_data.images
            article_data.hostname = extracted_data.hostname if extracted_data.hostname else article_data.hostname
            article_data.entities = extracted_data.entities
            article_data.geo_entities = extracted_data.geo_entities
                      
            
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
                "enriched_data": article_data,
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
    
    async def process_batch(self, article_ids: List[str], date: str) -> Dict[str, Any]:
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
                    date
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
                    
                    
            #enriched_data is the article_data with the extracted data
            
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
    
    async def _scrape_article(self, extracted_data: ExtractResult) -> ExtractResult:
        """Scrape article content with fallback."""
        try:
            # Try primary scraper first
            extracted_data: ExtractResult = await self.news_content_extractor.extract_feed(extracted_data.url)
            return extracted_data
        except Exception as e:  
            logger.error(f"Fallback scraper also failed for {extracted_data.ur}: {e}")
            raise e
        
        
    async def _set_extracted_language(self, extracted: ExtractResult) -> ExtractResult:
        """Scrape article content with fallback."""        
        try:            
            title = extracted.title
            text = extracted.content[:500]
            
            sentences = self.nlp_utils.split_sentences(text)            
            # take random 5 sentences
            if len(sentences) > 3:
                sample_sentences = random.sample(sentences, 3)
            else:
                sample_sentences = sentences

            sample_sentences.append(title)
            
            languages = []
            for sentence in sample_sentences:
                language = LanguageUtils.detect_language(sentence)                
                if language:
                    languages.append(language)
                    
            #get most common language from languages list
            from collections import Counter
            most_common_language = Counter(languages).most_common(1)[0][0]
            
            extracted.language = most_common_language

            return extracted
        
        except Exception as e:
            logger.error(f"Language detection failed: {e}")
            extracted.language = ""
        return extracted
            
            
  
    
    # async def _scrape_article(self, url: str) -> Dict[str, Any]:
    #     """Scrape article content with fallback."""
    #     try:
    #         # Try primary scraper first
    #         return await scraper.scrape_article(url)
    #     except ScrapingError as e:
    #         logger.warning(f"Primary scraper failed for {url}: {e}")
            
    #         # Try fallback scraper
    #         try:
    #             return await fallback_scraper.scrape_article(url)
    #         except Exception as fallback_error:
    #             logger.error(f"Fallback scraper also failed for {url}: {fallback_error}")
    #             raise ScrapingError(f"Both scrapers failed: {e}, {fallback_error}")
    
    async def _clean_text(self, scraped_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize text content."""
        content = scraped_data.get("content", "")
        language = scraped_data.get("language", "en")
        
        # Clean the text
        cleaned_result = self.text_cleaner.clean_text(content, language)
        
        # Update scraped data with cleaned content
        scraped_data["content"] = cleaned_result["cleaned_text"]
        scraped_data["cleaning_metadata"] = {
            "original_length": cleaned_result["original_length"],
            "cleaned_length": cleaned_result["cleaned_length"],
            "removed_elements": cleaned_result["removed_elements"],
            "compression_ratio": cleaned_result["compression_ratio"]
        }
        
        return scraped_data
    
    async def _translate_title(self, extracted_data: ExtractResult) -> ExtractResult:
        """Translate title and language detection."""
        if extracted_data.language == "en":
            extracted_data.title_en = extracted_data.title
            return extracted_data
        
        title = extracted_data.title
        language = extracted_data.language
        
        proxies = await self.proxy_service.get_proxy_cache()
       
        
        title_en = await self.translation_manager.translate(title,source=language, target="en",  proxies=proxies	)
        extracted_data.title_en = title_en if title_en else ""
        return extracted_data
    
    async def _extract_entities(self, extracted_data: ExtractResult) -> EntityModel:
        """Extract entities from article."""        
        content = extracted_data.content        
        entities: EntityModel = self.entity_tagger.extract_entities(content)
        extracted_data.entities = entities
        return extracted_data
    
    async def _geotag_article(self, extracted_data: ExtractResult) -> ExtractResult:
        """Extract geographic entities from article."""
        #content = article_data.get("content", "")
        #language = article_data.get("language", "en")
        title = extracted_data.title
        content = extracted_data.content
        locations = extracted_data.entities.LOC        
        
        #processing_steps.append("geotagging")
        # Extract geographic entities
        geo_entities: list[GeoEntity] = self.geotagger.extract_geographic_entities(title, content, locations)        
        extracted_data.geo_entities = geo_entities
        
        return extracted_data
    
    async def _find_images(self, extracted_data: ExtractResult) -> ExtractResult:
        """Find relevant images for the article."""
        try:          
            language = extracted_data.language
            images = list()
            # Generate search queries
            search_queries = self.image_finder.generate_search_queries(extracted_data)            
            # Find images using the best query
            images_data = None
            
            regions = set()
            
            if extracted_data.geo_entities and len(extracted_data.geo_entities) > 0:
                country = extracted_data.geo_entities[0].name            
            else:
                country = None
            
            regions =  self.image_finder.get_all_duckduckgo_regions(language, country)
            
            images = set()
            #images_data_set = list()
            
            for region in regions:
            
                if extracted_data.title:
                    try:            
                        images_data = await self.image_finder.find_images(extracted_data.title, 
                                                                    max_images=10, 
                                                                    duckduckgo_region=region)
                        if images_data.get("images"):
                            images.update(images_data.get("images"))
                            #images_data_set.extend(images_data.get("images_data"))
                    except Exception as e:
                        logger.warning(f"Image search failed for title '{extracted_data.title}': {e}")
                        continue
                    
                    
            if len(images) < 3 and extracted_data.title_en:               
                for region in regions:                
                    if extracted_data.title_en:
                        try:
                            images_data = await self.image_finder.find_images(extracted_data.title_en, 
                                                                        max_images=5, 
                                                                        duckduckgo_region=region)
                            if images_data.get("images"):
                                images.extend(images_data.get("images"))
                        except Exception as e:
                            logger.warning(f"Image search failed for title_en '{extracted_data.title_en}': {e}")
                            continue
                    
                
            # if len(images) < 3:
            #     for region in regions:
            #         for query in search_queries:
            #             try:
            #                 images_data = await self.image_finder.find_images(query, 
            #                                                                 max_images=1, 
            #                                                                 duckduckgo_region=region)
            #                 if images_data.get("images"):
            #                     images.extend(images_data.get("images"))
            #                     break
            #             except Exception as e:
            #                 logger.warning(f"Image search failed for query '{query}': {e}")
            #                 continue

                        
            if len(images) > 0:          
                extracted_data.images.extend(list(images))                
                # remove  empty
                extracted_data.images = [image for image in extracted_data.images if image]

            return extracted_data
        except Exception as e:
            logger.error(f"Image search failed: {e}")
            extracted_data.images = []
            return extracted_data
    
    async def _download_images(self, date: str, flashpoint_id: str, extracted_data: ExtractResult) -> ExtractResult:
        """Download images to supabase."""
        try:
            
            extracted_data = await self.image_downloader.download_images(date, flashpoint_id, extracted_data)
                
                
                
        except Exception as e:
            logger.error(f"Image download failed: {e}")
            extracted_data.images = []
        return extracted_data
    
    async def _fetch_articles_batch(self, article_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch articles from database by IDs."""
        try:
            # Connect to database if not already connected
            if not db_connection.client:
                await db_connection.connect()
            
            articles = []
            for article_id in article_ids:
                article = await db_connection.fetch_article_by_id(article_id)
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
            if not db_connection.client:
                await db_connection.connect()
            
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
                successful_updates, failed_updates = await db_connection.update_articles_batch(updates)
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
            if not db_connection.client:
                await db_connection.connect()
            
            return await db_connection.get_processing_stats()
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
            if not db_connection.client:
                await db_connection.connect()
            
            await db_connection._test_client()
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
            "status": "healthy" if self.geotagger.enabled else "disabled",
            "details": "Available" if self.geotagger.enabled else "Disabled"
        }
        
        health_status["components"]["image_finder"] = {
            "status": "healthy" if self.image_finder.enabled else "disabled",
            "details": "Available" if self.image_finder.enabled else "Disabled"
        }
        
        return health_status
    
    async def shutdown(self):
        """Gracefully shutdown the pipeline manager."""
        logger.info("Shutting down pipeline manager")
        
        # Shutdown thread pool
        thread_pool.shutdown(wait=True, timeout=30)
        
        # Disconnect from database
        await db_connection.disconnect()
        
        logger.info("Pipeline manager shutdown completed")


# Global pipeline manager instance
pipeline_manager = PipelineManager()
