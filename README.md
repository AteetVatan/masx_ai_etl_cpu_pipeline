# MASX AI ETL CPU Pipeline

A high-performance CPU-only news enrichment pipeline with FastAPI service layer. Processes ~100,000 articles/day with parallel processing, multilingual support, and comprehensive error handling.

## 🚀 Features

- **High-Performance Processing**: Parallel thread processing with dynamic CPU scaling
- **Multilingual Support**: Handles articles in multiple languages with language-specific processing
- **Comprehensive Scraping**: Primary scraper with BeautifulSoup + httpx, fallback with Crawl4AI
- **Advanced Text Processing**: Intelligent text cleaning and normalization
- **Geographic Entity Recognition**: Multilingual NER with spaCy and pycountry
- **Image Search**: Multiple API support (Bing, DuckDuckGo) with quality filtering
- **Production-Ready API**: FastAPI with comprehensive endpoints and monitoring
- **Database Integration**: Supabase support with batch operations
- **Docker Support**: Multi-stage Dockerfile with optimized production image
- **Comprehensive Testing**: Full test suite with unit and integration tests

## 📁 Project Structure

```
masx_cpu_pipeline/
├── src/
│   ├── api/                  # FastAPI endpoints
│   │   └── server.py
│   ├── config/               # Configuration & settings
│   │   └── settings.py
│   ├── db/                   # Supabase DB client & batch helpers
│   │   └── db_client.py
│   ├── scraping/             # Web scraping
│   │   ├── scraper.py        # BeautifulSoup + httpx
│   │   └── fallback_crawl4ai.py
│   ├── processing/           # Text cleaning & enrichment
│   │   ├── cleaner.py
│   │   ├── geotagger.py      # Multilingual NER + pycountry
│   │   └── image_finder.py   # Bing API / DuckDuckGo / CLIP
│   ├── pipeline/             # Orchestrator
│   │   └── pipeline_manager.py
│   └── utils/                # Helpers (logging, threadpool, etc.)
│       └── threadpool.py
├── tests/                    # Pytest unit/integration tests
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── nginx.conf
├── prometheus.yml
├── init.sql
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
   git clone https://github.com/masx-ai/etl-cpu-pipeline.git
   cd etl-cpu-pipeline
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
   # Option 1: Using the convenient run script
   python run.py
   
   # Option 2: Using uvicorn directly
   python -m uvicorn src.api.server:app --reload
   
   # Option 3: Using the shell script (Unix/Linux/macOS)
   ./run.sh
   
   # Option 4: Using the batch file (Windows)
   run.bat
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
| `SUPABASE_KEY` | Supabase anon key | Required |
| `SUPABASE_SERVICE_KEY` | Supabase service key | Required |
| `BING_SEARCH_API_KEY` | Bing Search API key | Optional |
| `DUCKDUCKGO_API_KEY` | DuckDuckGo API key | Optional |
| `MAX_WORKERS` | Maximum worker threads | CPU cores × 2 |
| `BATCH_SIZE` | Database batch size | 100 |
| `REQUEST_TIMEOUT` | Request timeout (seconds) | 30 |
| `LOG_LEVEL` | Logging level | INFO |
| `ENABLE_IMAGE_SEARCH` | Enable image search | true |
| `ENABLE_GEOTAGGING` | Enable geotagging | true |

### Database Setup

1. **Create Supabase project** at [supabase.com](https://supabase.com)
2. **Run database initialization**:
   ```sql
   -- Execute init.sql in your Supabase SQL editor
   ```
3. **Configure connection** in `.env` file

## 🚀 Usage

### Quick Start

The easiest way to start the application is using the provided run scripts:

```bash
# Python script (cross-platform)
python run.py

# Shell script (Unix/Linux/macOS)
./run.sh

# Batch file (Windows)
run.bat
```

These scripts will:
- Check Python version requirements
- Create and activate virtual environment
- Install dependencies automatically
- Download required spaCy models
- Validate environment configuration
- Start the FastAPI server

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
curl -X POST "http://localhost:8000/feed/warmup?date=2025-07-02"
```

#### Process All Feed Entries
```bash
# Process all entries for today
curl -X POST "http://localhost:8000/feed/process"

# Process all entries for specific date
curl -X POST "http://localhost:8000/feed/process?date=2025-07-02"
```

#### Process Feed Entries by Flashpoint ID
```bash
# Process entries for specific flashpoint ID
curl -X POST "http://localhost:8000/feed/process/flashpoint?date=2025-07-02&flashpoint_id=123e4567-e89b-12d3-a456-426614174000"
```

#### Get Feed Statistics
```bash
curl http://localhost:8000/feed/stats
```

#### Get Loaded Feed Entries
```bash
curl "http://localhost:8000/feed/entries/2025-07-02"
```

#### Clear Feed Entries from Memory
```bash
# Clear specific date
curl -X DELETE "http://localhost:8000/feed/clear/2025-07-02"

# Clear all dates
curl -X DELETE "http://localhost:8000/feed/clear"
```

### Feed Processing Workflow

The application now supports processing feed entries from date-based tables:

1. **Warm Up**: Load feed entries from `feed_entries_{date}` table into memory (date in YYYY-MM-DD format)
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
    
    result = await pipeline_manager.process_article(article_data)
    print(f"Processing result: {result['status']}")
    
    # Process batch
    batch_result = await pipeline_manager.process_batch(["article_1", "article_2"])
    print(f"Batch processing: {batch_result['successful']} successful, {batch_result['failed']} failed")

async def process_feed_entries():
    # Warm up server with feed entries
    warmup_result = await feed_processor.warm_up_server("2025-07-02")
    print(f"Warmed up with {warmup_result['total_entries']} entries")
    
    # Process all feed entries
    process_result = await feed_processor.process_feed_entries_by_date("2025-07-02")
    print(f"Processed: {process_result['successful']} successful, {process_result['failed']} failed")
    
    # Process specific flashpoint
    flashpoint_result = await feed_processor.process_feed_entries_by_flashpoint_id("2025-07-02", "flashpoint_123")
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

## 📊 Monitoring

### Health Check
- **Endpoint**: `GET /health`
- **Response**: Component health status and details

### Statistics
- **Endpoint**: `GET /stats`
- **Response**: Processing statistics, thread pool metrics, database stats

### Prometheus Metrics
- **Endpoint**: `GET /metrics`
- **Integration**: Built-in Prometheus metrics collection

### Grafana Dashboard
- **URL**: `http://localhost:3000`
- **Default credentials**: admin/admin

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
- **Concurrent Workers**: Up to 32 threads
- **Memory Usage**: ~2GB per worker
- **Response Time**: <100ms for health checks

### Optimization Tips
1. **Adjust `MAX_WORKERS`** based on your CPU cores
2. **Tune `BATCH_SIZE`** for your database performance
3. **Enable caching** for frequently accessed data
4. **Use connection pooling** for database connections

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

#### 4. API Rate Limits
```bash
# Check rate limiting in nginx.conf
# Adjust limit_req_zone settings
```

### Debug Mode
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
export DEBUG=true

# Run with debug output
python -m uvicorn src.api.server:app --reload --log-level debug
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Write comprehensive tests
- Update documentation
- Use meaningful commit messages

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **FastAPI** for the excellent web framework
- **spaCy** for multilingual NLP capabilities
- **Supabase** for the database backend
- **BeautifulSoup** for HTML parsing
- **httpx** for async HTTP client
- **pytest** for testing framework

## 📞 Support

- **Documentation**: [GitHub Wiki](https://github.com/masx-ai/etl-cpu-pipeline/wiki)
- **Issues**: [GitHub Issues](https://github.com/masx-ai/etl-cpu-pipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/masx-ai/etl-cpu-pipeline/discussions)

---

**Built with ❤️ by the MASX AI Team**