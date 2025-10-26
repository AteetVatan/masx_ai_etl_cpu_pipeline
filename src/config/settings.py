"""
Configuration settings for MASX AI ETL CPU Pipeline.

Uses pydantic-settings for robust configuration management with environment
variable support and validation.
"""

import os
from typing import Optional
from pydantic import Field, validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # Supabase Configuration
    # supabase_url: str = Field(..., description="Supabase project URL")
    # supabase_anon_key: str = Field(..., description="Supabase anon key")
    # supabase_service_role_key: str = Field(..., description="Supabase service key")

    process_articles_limit: int = Field(
        default=0, description="Number of articles to process"
    )
    # Pipeline Configuration
    max_scrapers: int = Field(
        default=1,
        description="Maximum number of scrapers to use",
    )

    db_batch_size: int = Field(default=100, description="Batch size for DB operation")

    # Server Configuration
    api_key: str = Field(default="", description="API key")
    require_api_key: bool = Field(
        default=False, description="Require API key for requests"
    )
    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8000, description="Server port")
    debug: bool = Field(default=False, description="Debug mode")

    # Database Configuration (Supabase)
    supabase_url: str = Field(default="", description="Supabase project URL")
    supabase_anon_key: str = Field(default="", description="Supabase anonymous key")
    supabase_service_role_key: str = Field(
        default="", description="Supabase service role key"
    )
    supabase_image_bucket: str = Field(default="", description="Supabase image bucket")
    supabase_db_password: str = Field(
        default="", description="Supabase database password"
    )
    supabase_db_url: str = Field(default="", description="Supabase database URL")
    database_max_connections: int = Field(
        default=10, description="Maximum number of database connections"
    )
    database_min_connections: int = Field(
        default=1, description="Minimum number of database connections"
    )

    request_timeout: int = Field(default=30, description="Request timeout in seconds")
    retry_attempts: int = Field(default=3, description="Number of retry attempts")
    retry_delay: float = Field(
        default=1.0, description="Delay between retries in seconds"
    )

    # Logging Configuration
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Log format (json or text)")

    # Processing Configuration
    enable_image_search: bool = Field(default=True, description="Enable image search")
    enable_geotagging: bool = Field(default=True, description="Enable geotagging")
    clean_text: bool = Field(default=True, description="Enable text cleaning")
    max_article_length: int = Field(default=50000, description="Maximum article length")

    # Image processing settings
    enable_image_download: bool = Field(
        default=True, description="Enable image download"
    )
    image_download_max_concurrency: int = Field(
        default=4, description="Maximum image download concurrency"
    )
    image_download_max_bytes: int = Field(
        default=5242880, description="Maximum image download bytes"
    )
    supabase_use_signed_urls: bool = Field(
        default=False, description="Use signed URLs for Supabase"
    )
    supabase_signed_url_expiry: int = Field(
        default=3600, description="Supabase signed URL expiry"
    )

    # Proxy Configuration
    proxy_api_key: Optional[str] = Field(default=None, description="Proxy API key")
    proxy_base: Optional[str] = Field(default=None, description="Proxy base URL")
    proxy_post_start_service: Optional[str] = Field(
        default=None, description="Proxy post start service"
    )
    proxy_get_proxies: Optional[str] = Field(
        default=None, description="Proxy get proxies"
    )

    # Test Database Configuration
    db_host: Optional[str] = Field(default="localhost", description="Database host")
    db_port: Optional[int] = Field(default=5432, description="Database port")
    db_name: Optional[str] = Field(default="", description="Database name")
    db_user: Optional[str] = Field(default="", description="Database user")
    db_password: Optional[str] = Field(default="", description="Database password")

    class Config:
        """Pydantic configuration."""

        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get the global settings instance."""
    return settings
