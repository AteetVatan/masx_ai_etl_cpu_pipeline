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
    supabase_url: str = Field(..., description="Supabase project URL")
    supabase_key: str = Field(..., description="Supabase anon key")
    supabase_service_key: str = Field(..., description="Supabase service key")
    
    # Database Configuration
    db_host: str = Field(default="localhost", description="Database host")
    db_port: int = Field(default=5432, description="Database port")
    db_name: str = Field(..., description="Database name")
    db_user: str = Field(..., description="Database user")
    db_password: str = Field(..., description="Database password")
    
    # API Keys
    bing_search_api_key: Optional[str] = Field(default=None, description="Bing Search API key")
    duckduckgo_api_key: Optional[str] = Field(default=None, description="DuckDuckGo API key")
    
    # Pipeline Configuration
    max_workers: int = Field(
        default_factory=lambda: min(32, (os.cpu_count() or 1) * 2),
        description="Maximum number of worker threads"
    )
    batch_size: int = Field(default=100, description="Batch size for database operations")
    request_timeout: int = Field(default=30, description="Request timeout in seconds")
    retry_attempts: int = Field(default=3, description="Number of retry attempts")
    retry_delay: float = Field(default=1.0, description="Delay between retries in seconds")
    
    # Logging Configuration
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Log format (json or text)")
    
    # Server Configuration
    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8000, description="Server port")
    debug: bool = Field(default=False, description="Debug mode")
    
    # Processing Configuration
    enable_image_search: bool = Field(default=True, description="Enable image search")
    enable_geotagging: bool = Field(default=True, description="Enable geotagging")
    clean_text: bool = Field(default=True, description="Enable text cleaning")
    max_article_length: int = Field(default=50000, description="Maximum article length")
    
    @validator("log_level")
    def validate_log_level(cls, v):
        """Validate log level is one of the supported levels."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v.upper()
    
    @validator("log_format")
    def validate_log_format(cls, v):
        """Validate log format is supported."""
        valid_formats = ["json", "text"]
        if v.lower() not in valid_formats:
            raise ValueError(f"log_format must be one of {valid_formats}")
        return v.lower()
    
    @validator("max_workers")
    def validate_max_workers(cls, v):
        """Ensure max_workers is reasonable."""
        if v < 1:
            raise ValueError("max_workers must be at least 1")
        if v > 64:
            raise ValueError("max_workers should not exceed 64 for stability")
        return v
    
    @validator("batch_size")
    def validate_batch_size(cls, v):
        """Ensure batch_size is reasonable."""
        if v < 1:
            raise ValueError("batch_size must be at least 1")
        if v > 1000:
            raise ValueError("batch_size should not exceed 1000 for performance")
        return v
    
    class Config:
        """Pydantic configuration."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()
