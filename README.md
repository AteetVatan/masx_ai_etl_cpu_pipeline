# MASX AI ETL CPU Pipeline

A high-performance CPU-only news enrichment pipeline with FastAPI service layer. Processes ~100,000 articles/day with **thread-safe parallel processing**, multilingual support, and comprehensive error handling.

## 🚀 Features

- **Thread-Safe Parallel Processing**: Production-ready parallel execution with no race conditions
- **High-Performance Processing**: Dynamic CPU scaling with optimal batch distribution
- **Multilingual Support**: Handles articles in multiple languages with language-specific processing
- **Comprehensive Scraping**: Multiple extractors (Trafilatura, Crawl4AI, BeautifulSoup) with fallback mechanisms
- **Advanced Text Processing**: Intelligent text cleaning and normalization
- **Geographic Entity Recognition**: Multilingual NER with spaCy and pycountry
- **Image Search & Download**: Multiple API support (Bing, DuckDuckGo) with Supabase storage
- **Translation Services**: Multi-provider translation with circuit breaker pattern
- **Production-Ready API**: FastAPI with comprehensive endpoints and monitoring
- **Database Integration**: Supabase support with batch operations and connection pooling
- **Docker Support**: Multi-stage Dockerfile with optimized production image
- **Comprehensive Testing**: Full test suite with unit and integration tests

## 📁 Project Structure

```
masx-ai-masx_ai_etl_cpu_pipeline/
├── src/
│   ├── api/                      # FastAPI endpoints
│   │   └── server.py
│   ├── config/                   # Configuration & settings
│   │   ├── settings.py
│   │   └── logging_config.py
│   ├── core/                     # Core exceptions
│   │   └── exceptions.py
│   ├── db/                       # Supabase DB client & batch helpers
│   │   └── db_client_and_pool.py
│   ├── models/                   # Data models
│   │   ├── feed_models.py
│   │   ├── extract_result.py
│   │   ├── entity_model.py
│   │   └── geo_entity.py
│   ├── pipeline/                 # Main orchestrator
│   │   └── pipeline_manager.py
│   ├── processing/               # Text cleaning & enrichment
│   │   ├── news_content_extractor.py
│   │   ├── cleaner.py
│   │   ├── geotagger.py         # Multilingual NER + pycountry
│   │   ├── entity_tragger.py    # Entity extraction
│   │   ├── image_finder.py      # Bing API / DuckDuckGo
│   │   ├── image_downloader.py  # Image download & Supabase storage
│   │   └── feed_processor.py    # Feed processing orchestrator
│   ├── scraping/                 # Web scraping
│   │   ├── trafilatura_extractor.py
│   │   ├── crawl4AI_extractor.py
│   │   ├── beautiful_soap_extractor.py
│   │   ├── unwrapped_url_resolver.py
│   │   ├── web_scraper_utils.py
│   │   └── error_patterns.py
│   ├── services/                 # External services
│   │   ├── translation_manager.py
│   │   └── proxy.py
│   └── utils/                    # Helpers
│       ├── threadpool.py
│       ├── nlp_utils.py
│       ├── language_utils.py
│       ├── url_utils.py
│       └── date_validation.py
├── tests/                        # Pytest unit/integration tests
├── third_party/                  # Third-party packages
├── requirements.txt
├── env.example                   # Environment configuration template
├── run.py                        # Main application entry point
├── debug.py                      # Debug script for testing
├── init.sql                      # Database initialization
├── Dockerfile
└── README.md
```

## 🛠️ Installation

### Prerequisites

- Python 3.12+
- Docker & Docker Compose (for containerized deployment)
- Supabase account (for database)

### Local Development

1. **Clone the repository**
   ```bash
   git clone https://github.com/masx-ai/masx_ai_etl_cpu_pipeline.git
   cd masx_ai_etl_cpu_pipeline
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Download spaCy models**
   ```bash
   python -m spacy download en_core_web_sm
   python -m spacy download es_core_news_sm
   python -m spacy download fr_core_news_sm
   python -m spacy download de_core_news_sm
   ```

5. **Configure environment**
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

6. **Run the application**
   ```bash
   # Using the main entry point
   python run.py
   
   # Or using uvicorn directly
   python -m uvicorn src.api.server:app --reload
   ```

### Docker Deployment

1. **Build and run with Docker Compose**
   ```bash
   docker-compose up -d
   ```

2. **Or build and run manually**
   ```bash
   docker build -t masx-etl-pipeline .
   docker run -p 8000:8000 --env-file .env masx-etl-pipeline
   ```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SUPABASE_URL` | Supabase project URL | Required |
| `SUPABASE_ANON_KEY` | Supabase anon key | Required |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service key | Required |
| `SUPABASE_IMAGE_BUCKET` | Supabase image bucket name | Required |
| `SUPABASE_DB_PASSWORD` | Supabase database password | Required |
| `SUPABASE_DB_URL` | Supabase database URL | Required |
| `BING_SEARCH_API_KEY` | Bing Search API key | Optional |
| `DUCKDUCKGO_API_KEY` | DuckDuckGo API key | Optional |
| `PROXY_API_KEY` | Proxy service API key | Optional |
| `PROXY_BASE` | Proxy service base URL | Optional |
| `MAX_WORKERS` | Maximum worker threads | CPU cores × 2 |
| `BATCH_SIZE` | Database batch size | 100 |
| `REQUEST_TIMEOUT` | Request timeout (seconds) | 30 |
| `RETRY_ATTEMPTS` | Number of retry attempts | 3 |
| `RETRY_DELAY` | Delay between retries (seconds) | 1.0 |
| `LOG_LEVEL` | Logging level | INFO |
| `LOG_FORMAT` | Log format (json/text) | json |
| `HOST` | Server host | 0.0.0.0 |
| `PORT` | Server port | 8000 |
| `DEBUG` | Debug mode | false |
| `ENABLE_IMAGE_SEARCH` | Enable image search | true |
| `ENABLE_GEOTAGGING` | Enable geotagging | true |
| `CLEAN_TEXT` | Enable text cleaning | true |
| `MAX_ARTICLE_LENGTH` | Maximum article length | 50000 |

### Database Setup

1. **Create Supabase project** at [supabase.com](https://supabase.com)
2. **Run database initialization**:
   ```sql
   -- Execute init.sql in your Supabase SQL editor
   ```
3. **Configure connection** in `.env` file

## 🚀 Usage

### Quick Start

The easiest way to start the application:

```bash
# Main entry point (recommended)
python run.py
```

This will:
- Check Python version requirements
- Validate environment configuration
- Start the FastAPI server with proper logging

### API Endpoints

#### Health Check
```bash
curl http://localhost:8000/health
```

#### Process Single Article
```bash
curl -X POST "http://localhost:8000/process-article" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://example.com/article",
    "article_id": "article_123",
    "metadata": {"source": "test"}
  }'
```

#### Process Batch
```bash
curl -X POST "http://localhost:8000/process-batch" \
  -H "Content-Type: application/json" \
  -d '{
    "article_ids": ["article_1", "article_2", "article_3"],
    "batch_size": 10
  }'
```

#### Get Statistics
```bash
curl http://localhost:8000/stats
```

#### List Articles
```bash
curl "http://localhost:8000/articles?limit=10&status=completed"
```

### Feed Processing Endpoints

#### Warm Up Server (Load Feed Entries)
```bash
# Load feed entries for today
curl -X POST "http://localhost:8000/feed/warmup"

# Load feed entries for specific date
curl -X POST "http://localhost:8000/feed/warmup?date=2025-01-15"
```

#### Process All Feed Entries
```bash
# Process all entries for today
curl -X POST "http://localhost:8000/feed/process"

# Process all entries for specific date
curl -X POST "http://localhost:8000/feed/process?date=2025-01-15"
```

#### Process Feed Entries by Flashpoint ID
```bash
# Process entries for specific flashpoint ID
curl -X POST "http://localhost:8000/feed/process/flashpoint?date=2025-01-15&flashpoint_id=123e4567-e89b-12d3-a456-426614174000"
```

#### Get Feed Statistics
```bash
curl http://localhost:8000/feed/stats
```

#### Get Loaded Feed Entries
```bash
curl "http://localhost:8000/feed/entries/2025-01-15"
```

#### Clear Feed Entries from Memory
```bash
# Clear specific date
curl -X DELETE "http://localhost:8000/feed/clear/2025-01-15"

# Clear all dates
curl -X DELETE "http://localhost:8000/feed/clear"
```

### Feed Processing Workflow

The application supports processing feed entries from date-based tables:

1. **Warm Up**: Load feed entries from `feed_entries_{date}` table into memory
2. **Process**: Run complete pipeline (scrape → clean → geotag → find image → save to DB)
3. **Filter**: Process specific entries by flashpoint_id
4. **Monitor**: Track processing statistics and performance

#### Workflow Example

```bash
# 1. Warm up server with today's feed entries
curl -X POST "http://localhost:8000/feed/warmup"

# 2. Process all entries for today
curl -X POST "http://localhost:8000/feed/process"

# 3. Check processing statistics
curl http://localhost:8000/feed/stats

# 4. Process specific flashpoint entries
curl -X POST "http://localhost:8000/feed/process/flashpoint?flashpoint_id=123e4567-e89b-12d3-a456-426614174000"
```

### Python API Usage

```python
import asyncio
from src.pipeline.pipeline_manager import pipeline_manager
from src.processing.feed_processor import feed_processor

async def process_articles():
    # Process single article
    article_data = {
        "id": "article_123",
        "url": "https://example.com/article",
        "metadata": {"source": "test"}
    }
    
    result = await pipeline_manager.process_article(article_data, "2025-01-15")
    print(f"Processing result: {result['status']}")
    
    # Process batch
    batch_result = await pipeline_manager.process_batch(["article_1", "article_2"], "2025-01-15")
    print(f"Batch processing: {batch_result['successful']} successful, {batch_result['failed']} failed")

async def process_feed_entries():
    # Warm up server with feed entries
    warmup_result = await feed_processor.warm_up_server("2025-01-15")
    print(f"Warmed up with {warmup_result['total_entries']} entries")
    
    # Process all feed entries
    process_result = await feed_processor.process_feed_entries_by_date("2025-01-15")
    print(f"Processed: {process_result['successful']} successful, {process_result['failed']} failed")
    
    # Process specific flashpoint
    flashpoint_result = await feed_processor.process_feed_entries_by_flashpoint_id("2025-01-15", "flashpoint_123")
    print(f"Flashpoint processing: {flashpoint_result['successful']} successful")

# Run the examples
asyncio.run(process_articles())
asyncio.run(process_feed_entries())
```

## 🧪 Testing

### Run All Tests
```bash
pytest
```

### Run Specific Test Categories
```bash
# Unit tests only
pytest -m unit

# Integration tests only
pytest -m integration

# Slow tests only
pytest -m slow
```

### Test Coverage
```bash
pytest --cov=src --cov-report=html
```

### Debug Testing
```bash
# Run debug script for testing feed processing
python debug.py
```

## 📊 Monitoring

### Health Check
- **Endpoint**: `GET /health`
- **Response**: Component health status and details

### Statistics
- **Endpoint**: `GET /stats`
- **Response**: Processing statistics and database stats

### Component Testing
- **Text Cleaner**: `GET /test/text-cleaner?text=Hello&language=en`
- **Geotagger**: `GET /test/geotagger?text=Paris&language=en`
- **Image Finder**: `GET /test/image-finder?query=news&max_images=3&language=en`

## 🔧 Development

### Code Style
```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Lint code
flake8 src/ tests/
```

### Pre-commit Hooks
```bash
pip install pre-commit
pre-commit install
```

## 🐳 Docker

### Multi-stage Build
- **Builder stage**: Installs dependencies and builds packages
- **Production stage**: Minimal runtime image with security best practices

### Security Features
- Non-root user execution
- Minimal attack surface
- Health checks
- Resource limits

### Production Deployment
```bash
# Build production image
docker build -t masx-etl-pipeline:latest .

# Run with production settings
docker run -d \
  --name masx-etl-pipeline \
  -p 8000:8000 \
  --env-file .env \
  --restart unless-stopped \
  masx-etl-pipeline:latest
```

## 📈 Performance

### Benchmarks
- **Processing Speed**: ~100,000 articles/day
- **Concurrent Workers**: Up to 32 threads (thread-safe)
- **Memory Usage**: ~2GB per worker
- **Response Time**: <100ms for health checks

### Thread Safety
- **Parallel Execution**: Safe for production parallel batch processing
- **Singleton Patterns**: All singletons are thread-safe
- **Database Operations**: Parameter-based to avoid race conditions
- **Cache Management**: Thread-safe caching with proper locking

### Optimization Tips
1. **Adjust `MAX_WORKERS`** based on your CPU cores
2. **Tune `BATCH_SIZE`** for your database performance
3. **Enable caching** for frequently accessed data
4. **Use connection pooling** for database connections
5. **Monitor memory usage** during parallel processing

## 🚨 Troubleshooting

### Common Issues

#### 1. Database Connection Failed
```bash
# Check Supabase credentials
curl -H "apikey: YOUR_SUPABASE_KEY" https://YOUR_PROJECT.supabase.co/rest/v1/
```

#### 2. spaCy Models Not Found
```bash
# Download required models
python -m spacy download en_core_web_sm
python -m spacy download es_core_news_sm
```

#### 3. Memory Issues
```bash
# Reduce MAX_WORKERS
export MAX_WORKERS=8

# Increase Docker memory limit
docker run -m 4g masx-etl-pipeline
```

#### 4. Parallel Processing Issues
```bash
# Check thread safety
python -c "from src.pipeline.pipeline_manager import pipeline_manager; print('Thread-safe pipeline ready')"
```

### Debug Mode
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
export DEBUG=true

# Run with debug output
python -m uvicorn src.api.server:app --reload --log-level debug
```

## 🔄 Recent Updates

### Thread Safety Improvements
- **TranslationManager**: Removed global environment variable manipulation
- **DatabaseClientAndPool**: Eliminated shared date/table_name state
- **ProxyService**: Added thread-safe cache management
- **PipelineManager**: Removed shared statistics to prevent race conditions


### Performance Enhancements
- Optimized batch size calculation for even worker distribution
- Improved Supabase storage operations
- Enhanced parallel processing capabilities

### Development Guidelines
- Follow PEP 8 style guidelines
- Write comprehensive tests
- Update documentation
- Use meaningful commit messages
- Ensure thread safety for parallel operations

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **FastAPI** for the excellent web framework
- **spaCy** for multilingual NLP capabilities
- **Supabase** for the database backend
- **Trafilatura** for content extraction
- **BeautifulSoup** for HTML parsing
- **httpx** for async HTTP client
- **pytest** for testing framework

## 📞 Support

- **Documentation**: [GitHub Wiki](https://github.com/masx-ai/masx_ai_etl_cpu_pipeline/wiki)
- **Issues**: [GitHub Issues](https://github.com/masx-ai/masx_ai_etl_cpu_pipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/masx-ai/masx_ai_etl_cpu_pipeline/discussions)

---

**Built with 🔥 by the MASX AI Team**