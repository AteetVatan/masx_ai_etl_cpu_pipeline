-- Database initialization script for MASX AI ETL CPU Pipeline
-- Creates necessary tables and indexes for article processing

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create articles table
CREATE TABLE IF NOT EXISTS articles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    url TEXT NOT NULL UNIQUE,
    title TEXT,
    content TEXT,
    author TEXT,
    published_date TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    enriched_data JSONB,
    metadata JSONB,
    error_message TEXT,
    processing_time FLOAT,
    processing_steps TEXT[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(status);
CREATE INDEX IF NOT EXISTS idx_articles_created_at ON articles(created_at);
CREATE INDEX IF NOT EXISTS idx_articles_updated_at ON articles(updated_at);
CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url);
CREATE INDEX IF NOT EXISTS idx_articles_published_date ON articles(published_date);

-- Create GIN index for JSONB enriched_data
CREATE INDEX IF NOT EXISTS idx_articles_enriched_data ON articles USING GIN(enriched_data);

-- Create GIN index for JSONB metadata
CREATE INDEX IF NOT EXISTS idx_articles_metadata ON articles USING GIN(metadata);

-- Create trigram index for full-text search on title and content
CREATE INDEX IF NOT EXISTS idx_articles_title_trgm ON articles USING GIN(title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_articles_content_trgm ON articles USING GIN(content gin_trgm_ops);

-- Create processing statistics table
CREATE TABLE IF NOT EXISTS processing_stats (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    total_processed INTEGER DEFAULT 0,
    successful INTEGER DEFAULT 0,
    failed INTEGER DEFAULT 0,
    average_processing_time FLOAT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(date)
);

-- Create index on date for efficient queries
CREATE INDEX IF NOT EXISTS idx_processing_stats_date ON processing_stats(date);

-- Create processing logs table for debugging
CREATE TABLE IF NOT EXISTS processing_logs (
    id SERIAL PRIMARY KEY,
    article_id UUID REFERENCES articles(id) ON DELETE CASCADE,
    level VARCHAR(10) NOT NULL CHECK (level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    message TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index on article_id and created_at for efficient queries
CREATE INDEX IF NOT EXISTS idx_processing_logs_article_id ON processing_logs(article_id);
CREATE INDEX IF NOT EXISTS idx_processing_logs_created_at ON processing_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_processing_logs_level ON processing_logs(level);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_articles_updated_at 
    BEFORE UPDATE ON articles 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Create function to clean up old processing logs
CREATE OR REPLACE FUNCTION cleanup_old_logs()
RETURNS void AS $$
BEGIN
    DELETE FROM processing_logs 
    WHERE created_at < NOW() - INTERVAL '30 days';
END;
$$ language 'plpgsql';

-- Create function to get processing statistics
CREATE OR REPLACE FUNCTION get_processing_stats()
RETURNS TABLE (
    status VARCHAR(20),
    count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        a.status,
        COUNT(*) as count
    FROM articles a
    GROUP BY a.status
    ORDER BY a.status;
END;
$$ language 'plpgsql';

-- Create function to get daily processing statistics
CREATE OR REPLACE FUNCTION get_daily_stats(days INTEGER DEFAULT 7)
RETURNS TABLE (
    date DATE,
    total_processed BIGINT,
    successful BIGINT,
    failed BIGINT,
    average_processing_time NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        DATE(a.created_at) as date,
        COUNT(*) as total_processed,
        COUNT(*) FILTER (WHERE a.status = 'completed') as successful,
        COUNT(*) FILTER (WHERE a.status = 'failed') as failed,
        ROUND(AVG(a.processing_time), 2) as average_processing_time
    FROM articles a
    WHERE a.created_at >= NOW() - INTERVAL '1 day' * days
    GROUP BY DATE(a.created_at)
    ORDER BY date DESC;
END;
$$ language 'plpgsql';

-- Create function to search articles
CREATE OR REPLACE FUNCTION search_articles(
    search_query TEXT,
    limit_count INTEGER DEFAULT 100,
    offset_count INTEGER DEFAULT 0
)
RETURNS TABLE (
    id UUID,
    url TEXT,
    title TEXT,
    content TEXT,
    author TEXT,
    published_date TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    similarity REAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        a.id,
        a.url,
        a.title,
        a.content,
        a.author,
        a.published_date,
        a.status,
        a.created_at,
        a.updated_at,
        GREATEST(
            similarity(a.title, search_query),
            similarity(a.content, search_query)
        ) as similarity
    FROM articles a
    WHERE 
        a.title ILIKE '%' || search_query || '%' OR
        a.content ILIKE '%' || search_query || '%' OR
        a.author ILIKE '%' || search_query || '%'
    ORDER BY similarity DESC, a.created_at DESC
    LIMIT limit_count
    OFFSET offset_count;
END;
$$ language 'plpgsql';

-- Insert initial data
INSERT INTO processing_stats (date, total_processed, successful, failed, average_processing_time)
VALUES (CURRENT_DATE, 0, 0, 0, 0)
ON CONFLICT (date) DO NOTHING;

-- Create views for common queries
CREATE OR REPLACE VIEW article_summary AS
SELECT 
    id,
    url,
    title,
    author,
    published_date,
    status,
    created_at,
    updated_at,
    CASE 
        WHEN content IS NOT NULL THEN LENGTH(content)
        ELSE 0
    END as content_length,
    CASE 
        WHEN enriched_data IS NOT NULL THEN jsonb_array_length(enriched_data->'geographic_entities'->'countries')
        ELSE 0
    END as country_count,
    CASE 
        WHEN enriched_data IS NOT NULL THEN jsonb_array_length(enriched_data->'geographic_entities'->'cities')
        ELSE 0
    END as city_count,
    CASE 
        WHEN enriched_data IS NOT NULL THEN jsonb_array_length(enriched_data->'images')
        ELSE 0
    END as image_count
FROM articles;

-- Create view for processing performance
CREATE OR REPLACE VIEW processing_performance AS
SELECT 
    DATE(created_at) as date,
    COUNT(*) as total_articles,
    COUNT(*) FILTER (WHERE status = 'completed') as successful,
    COUNT(*) FILTER (WHERE status = 'failed') as failed,
    ROUND(AVG(processing_time), 2) as avg_processing_time,
    ROUND(MAX(processing_time), 2) as max_processing_time,
    ROUND(MIN(processing_time), 2) as min_processing_time
FROM articles
WHERE created_at >= NOW() - INTERVAL '30 days'
GROUP BY DATE(created_at)
ORDER BY date DESC;

-- Grant permissions (adjust as needed for your setup)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO masx_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO masx_user;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO masx_user;
