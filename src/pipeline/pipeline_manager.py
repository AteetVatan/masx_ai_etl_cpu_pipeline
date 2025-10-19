"""
Pipeline manager for MASX AI ETL CPU Pipeline.

Orchestrates the complete article processing pipeline with parallel execution,
error handling, and batch operations for high-performance processing.
"""

import asyncio
import time
import os
from typing import Dict, Any, List, Optional, Tuple
import random
from datetime import datetime


from src.db import db_connection, DatabaseError
from src.processing import (
    NewsContentExtractor,
    EntityTagger,
    TextCleaner,
    Geotagger,
    ImageFinder,
    ImageDownloader,
)
from src.services import ProxyService, TranslationManager
from src.config import get_service_logger, get_settings
from src.models import (
    FeedModel,
    ExtractResult,
    EntityModel,
    GeoEntity,
)


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
        self.db_batch_size = settings.db_batch_size
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

        # service
        self.proxy_service = ProxyService.get_instance()
        self.nlp_utils = NlpUtils()

        # try:
        #     loop = asyncio.get_event_loop()
        #     if loop.is_running():
        #         loop.create_task(self.proxy_service.ping_start_proxy())
        #     else:
        #         loop.run_until_complete(self.proxy_service.ping_start_proxy())
        # except RuntimeError:
        #     asyncio.run(self.proxy_service.ping_start_proxy())

        # Note: Statistics removed to avoid race conditions in parallel execution

        logger.info("Pipeline manager initialized")

    async def process_article(
        self, article_data: FeedModel, date: str
    ) -> Dict[str, Any]:
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
            extracted_data = ExtractResult()
            extracted_data.id = article_id
            extracted_data.parent_id = flashpoint_id
            extracted_data.url = url
            extracted_data.title = title
            extracted_data.images = []
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
            logger.debug(
                f"Step 3: Translating title and language detection for article {article_id}"
            )
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
            if len(extracted_data.images) > 0:
                logger.debug(
                    f"Step 7: Downloading images to supabase for article {article_id}"
                )
                extracted_data = await self._download_images(
                    date, flashpoint_id, extracted_data
                )
                processing_steps.append("image_download")

            # here update the article data with the extracted data
            article_data.title = extracted_data.title
            article_data.title_en = extracted_data.title_en
            article_data.language = extracted_data.language
            article_data.author = extracted_data.author
            article_data.published_date = extracted_data.published_date
            article_data.content = extracted_data.content
            article_data.images = extracted_data.images
            article_data.hostname = (
                extracted_data.hostname
                if extracted_data.hostname
                else article_data.hostname
            )
            article_data.entities = extracted_data.entities
            article_data.geo_entities = extracted_data.geo_entities

            # Calculate processing time
            processing_time = time.time() - start_time

            # Statistics removed to avoid race conditions in parallel execution

            result = {
                "article_id": article_id,
                "status": "completed",
                "processing_time": processing_time,
                "processing_steps": processing_steps,
                "enriched_data": article_data,
                "errors": errors,
                "timestamp": datetime.utcnow().isoformat(),
            }

            logger.info(
                f"Successfully processed article {article_id} in {processing_time:.2f}s"
            )
            return result

        except Exception as e:
            # Statistics removed to avoid race conditions in parallel execution

            error_msg = f"Pipeline processing failed for article {article_id}: {str(e)}"
            logger.error(error_msg)

            return {
                "article_id": article_id,
                "status": "failed",
                "processing_time": time.time() - start_time,
                "processing_steps": processing_steps,
                "enriched_data": None,
                "errors": [error_msg],
                "timestamp": datetime.utcnow().isoformat(),
            }

    async def process_batch(
        self, article_data_list: List[FeedModel], date: str
    ) -> Dict[str, Any]:
        """
        Process a batch of articles in parallel with intelligent batching.

        Creates CPU-optimized sub-batches and processes them sequentially to avoid
        overwhelming the system while maintaining high throughput.

        Args:
            article_data_list: List of article data to process
            date: Processing date

        Returns:
            Dictionary containing batch processing results
        """
        logger.info(
            f"Starting intelligent batch processing for {len(article_data_list)} articles"
        )

        start_time = time.time()
        all_results = []
        total_successful = 0
        total_failed = 0

        try:
            # Calculate optimal batch size based on CPU cores and available workers
            optimal_batch_size = self._calculate_optimal_batch_size(
                len(article_data_list)
            )
            logger.info(
                f"Using batch size of {optimal_batch_size} articles per sub-batch"
            )

            # Split articles into optimal sub-batches
            sub_batches = self._create_sub_batches(
                article_data_list, optimal_batch_size
            )
            logger.info(f"Created {len(sub_batches)} sub-batches for processing")

            # Process each sub-batch sequentially to avoid resource contention
            for batch_idx, sub_batch in enumerate(sub_batches, 1):
                logger.info(
                    f"Processing sub-batch {batch_idx}/{len(sub_batches)} with {len(sub_batch)} articles"
                )

                batch_start_time = time.time()
                batch_results = await self._process_sub_batch(
                    sub_batch, date, batch_idx
                )

                # Aggregate results
                all_results.extend(batch_results["results"])
                total_successful += batch_results["successful"]
                total_failed += batch_results["failed"]

                batch_time = time.time() - batch_start_time
                logger.info(
                    f"Sub-batch {batch_idx} completed: {batch_results['successful']} successful, "
                    f"{batch_results['failed']} failed in {batch_time:.2f}s"
                )

                # Small delay between batches to prevent resource exhaustion
                if batch_idx < len(sub_batches):
                    await asyncio.sleep(0.1)

            total_processing_time = time.time() - start_time

            logger.info(
                f"Batch processing completed: {total_successful} successful, "
                f"{total_failed} failed in {total_processing_time:.2f}s"
            )

            return {
                "status": "completed",
                "total_articles": len(article_data_list),
                "processed": len(all_results),
                "successful": total_successful,
                "failed": total_failed,
                "processing_time": total_processing_time,
                "sub_batches_processed": len(sub_batches),
                "optimal_batch_size": optimal_batch_size,
                "results": all_results,
            }

        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            return {
                "status": "failed",
                "total_articles": len(article_data_list),
                "processed": len(all_results),
                "successful": total_successful,
                "failed": total_failed,
                "processing_time": time.time() - start_time,
                "error": str(e),
                "results": all_results,
            }

    def _calculate_optimal_batch_size(self, total_articles: int) -> int:
        """
        Calculate optimal batch size based on system resources and article count.

        Args:
            total_articles: Total number of articles to process

        Returns:
            Optimal batch size for processing
        """
        # Base batch size on available CPU cores and thread pool capacity
        
        return self.max_workers
        
        cpu_cores = os.cpu_count() or 4
        max_workers = self.max_workers

        # Calculate base batch size (2-3x the number of workers for good throughput)
        base_batch_size = max_workers * 2
        
        batch_size = 6

        # Adjust based on total articles
        if total_articles <= 10:
            batch_size = min(total_articles, 5)  # Small batches for small workloads
        elif total_articles <= 50:
            batch_size = min(base_batch_size, total_articles)
        elif total_articles <= 200:
            batch_size = min(base_batch_size * 2, total_articles)
        else:
            # For large workloads, use larger batches but cap at reasonable size
            batch_size = min(base_batch_size * 3, 100)
        
        logger.info(f"Calculated optimal batch size: {batch_size}")
        return batch_size
        
        

    def _create_sub_batches(
        self, article_data_list: List[FeedModel], batch_size: int
    ) -> List[List[FeedModel]]:
        """
        Split article list into sub-batches of optimal size.

        Args:
            article_data_list: List of articles to split
            batch_size: Size of each sub-batch

        Returns:
            List of sub-batches
        """
        sub_batches = []
        for i in range(0, len(article_data_list), batch_size):
            sub_batch = article_data_list[i : i + batch_size]
            sub_batches.append(sub_batch)
        return sub_batches

    async def _process_sub_batch(
        self, sub_batch: List[FeedModel], date: str, batch_idx: int
    ) -> Dict[str, Any]:
        """
        Process a single sub-batch of articles in parallel using asyncio.

        Args:
            sub_batch: List of articles in this sub-batch
            date: Processing date
            batch_idx: Index of this sub-batch for logging

        Returns:
            Dictionary containing sub-batch processing results
        """
        results = []
        successful = 0
        failed = 0

        # Create asyncio tasks for all articles in this sub-batch
        tasks = []
        for article_data in sub_batch:
            task = asyncio.create_task(
                self.process_article(article_data, date),
                name=f"article_{article_data.id}",
            )
            tasks.append(task)

        # Wait for all tasks in this sub-batch to complete
        try:
            # Use asyncio.gather to wait for all tasks with return_exceptions=True
            # to handle individual task failures gracefully
            task_results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(task_results):
                if isinstance(result, Exception):
                    # Handle task exceptions
                    logger.error(f"Task failed in sub-batch {batch_idx}: {result}")
                    failed += 1
                    results.append(
                        {
                            "article_id": sub_batch[i].id
                            if i < len(sub_batch)
                            else "unknown",
                            "status": "failed",
                            "processing_time": 0,
                            "processing_steps": [],
                            "enriched_data": None,
                            "errors": [str(result)],
                            "timestamp": datetime.utcnow().isoformat(),
                        }
                    )
                else:
                    # Handle successful results
                    results.append(result)
                    if result["status"] == "completed":
                        successful += 1
                    else:
                        failed += 1

        except Exception as e:
            # Handle any unexpected errors in the gather operation
            logger.error(f"Unexpected error in sub-batch {batch_idx}: {e}")
            # Mark all remaining tasks as failed
            for i in range(len(results), len(sub_batch)):
                failed += 1
                results.append(
                    {
                        "article_id": sub_batch[i].id
                        if i < len(sub_batch)
                        else "unknown",
                        "status": "failed",
                        "processing_time": 0,
                        "processing_steps": [],
                        "enriched_data": None,
                        "errors": [str(e)],
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                )

        return {"successful": successful, "failed": failed, "results": results}

    async def _scrape_article(self, extracted_data: ExtractResult) -> ExtractResult:
        """Scrape article content with fallback."""
        try:
            # Try primary scraper first
            extracted_data: ExtractResult = (
                await self.news_content_extractor.extract_feed(extracted_data.url)
            )
            if not extracted_data:
                raise Exception("Failed to scrape article")
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

            # get most common language from languages list
            from collections import Counter

            most_common_language = Counter(languages).most_common(1)[0][0]

            extracted.language = most_common_language

            return extracted

        except Exception as e:
            logger.error(f"Language detection failed: {e}")
            extracted.language = ""
        return extracted

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
            "compression_ratio": cleaned_result["compression_ratio"],
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

        title_en = await self.translation_manager.translate(
            title, source=language, target="en", proxies=proxies
        )
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
        # content = article_data.get("content", "")
        # language = article_data.get("language", "en")
        title = extracted_data.title
        content = extracted_data.content
        locations = extracted_data.entities.LOC

        # processing_steps.append("geotagging")
        # Extract geographic entities
        geo_entities: list[GeoEntity] = self.geotagger.extract_geographic_entities(
            title, content, locations
        )
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

            regions = self.image_finder.get_all_duckduckgo_regions(language, country)
            images = set()
            # images_data_set = list()
            proxies = await self.proxy_service.get_proxy_cache()

            for region in regions:
                if len(images) >= 5:
                    break  # 5 images are enough

                if extracted_data.title:
                    try:
                        images_data = await self.image_finder.find_images(
                            extracted_data.title,
                            max_images=10,
                            proxies=proxies,
                            duckduckgo_region=region,
                        )
                        if images_data.get("images"):
                            images.update(images_data.get("images"))
                            # images_data_set.extend(images_data.get("images_data"))
                    except Exception as e:
                        logger.warning(
                            f"Image search failed for title '{extracted_data.title}': {e}"
                        )

                if (
                    len(images) < 3
                    and extracted_data.language != "en"
                    and extracted_data.title_en
                ):
                    if extracted_data.title_en:
                        try:
                            images_data = await self.image_finder.find_images(
                                extracted_data.title_en,
                                max_images=10,
                                proxies=proxies,
                                duckduckgo_region=region,
                            )
                            if images_data.get("images"):
                                images.extend(images_data.get("images"))
                        except Exception as e:
                            logger.warning(
                                f"Image search failed for title_en '{extracted_data.title_en}': {e}"
                            )

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
                extracted_data.images = [
                    image for image in extracted_data.images if image
                ]

            return extracted_data
        except Exception as e:
            logger.error(f"Image search failed: {e}")
            extracted_data.images = []
            return extracted_data

    async def _download_images(
        self, date: str, flashpoint_id: str, extracted_data: ExtractResult
    ) -> ExtractResult:
        """Download images to supabase."""
        try:
            extracted_data = await self.image_downloader.download_images(
                date, flashpoint_id, extracted_data
            )

        except Exception as e:
            logger.error(f"Image download failed: {e}")
            extracted_data.images = []
        return extracted_data

    async def _fetch_articles_batch(
        self, article_ids: List[str]
    ) -> List[Dict[str, Any]]:
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
                        "processing_steps": result["processing_steps"],
                    }
                else:
                    update_data = {
                        "id": result["article_id"],
                        "status": "failed",
                        "error_message": "; ".join(result["errors"]),
                        "processing_time": result["processing_time"],
                    }

                updates.append(update_data)

            if updates:
                (
                    successful_updates,
                    failed_updates,
                ) = await db_connection.update_articles_batch(updates)
                logger.info(
                    f"Updated {successful_updates} articles, {failed_updates} failed"
                )

        except DatabaseError as e:
            logger.error(f"Failed to update articles: {e}")
            raise

    async def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics."""
        return {
            "pipeline_stats": {
                "note": "Statistics removed to avoid race conditions in parallel execution"
            },
            "thread_pool_stats": {"note": "Statistics disabled for parallel execution"},
            "database_stats": await self._get_database_stats(),
            "uptime": 0.0,  # Statistics disabled, return 0.0 instead of string
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
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Check database connection
        try:
            if not db_connection.client:
                await db_connection.connect()

            await db_connection._test_client()
            health_status["components"]["database"] = {
                "status": "healthy",
                "details": "Connected successfully",
            }
        except Exception as e:
            health_status["components"]["database"] = {
                "status": "unhealthy",
                "details": str(e),
            }
            health_status["overall"] = "unhealthy"

        # Check processing modules
        health_status["components"]["scraper"] = {
            "status": "healthy",
            "details": "Available",
        }

        health_status["components"]["text_cleaner"] = {
            "status": "healthy",
            "details": "Available",
        }

        health_status["components"]["geotagger"] = {
            "status": "healthy" if self.geotagger.enabled else "disabled",
            "details": "Available" if self.geotagger.enabled else "Disabled",
        }

        health_status["components"]["image_finder"] = {
            "status": "healthy" if self.image_finder.enabled else "disabled",
            "details": "Available" if self.image_finder.enabled else "Disabled",
        }

        return health_status

    async def shutdown(self):
        """Gracefully shutdown the pipeline manager."""
        logger.info("Shutting down pipeline manager")

        # Disconnect from database
        await db_connection.disconnect()

        logger.info("Pipeline manager shutdown completed")


# Global pipeline manager instance
try:
    pipeline_manager = PipelineManager()
    print("PipelineManager created successfully")

    # Check if process_batch method exists
    if hasattr(pipeline_manager, "process_batch"):
        print("process_batch method exists")
    else:
        print("process_batch method does NOT exist")
        print(
            f"Available methods: {[method for method in dir(pipeline_manager) if not method.startswith('_')]}"
        )

except Exception as e:
    print(f"Error creating PipelineManager: {e}")
    import traceback

    traceback.print_exc()
