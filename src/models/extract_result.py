from pydantic import BaseModel
from typing import Dict, Any, Optional
from .entity_model import EntityModel
from .geo_entity import GeoEntity


class ExtractResult(BaseModel):
    """Normalized result for downstream pipeline."""

    id: str = ""
    parent_id: Optional[str] = ""
    url: str = ""
    title: Optional[str] = ""
    title_en: Optional[str] = ""
    language: Optional[str] = ""
    author: Optional[str] = ""
    published_date: Optional[str] = ""
    content: Optional[str] = ""
    images: Optional[list[str]] = []
    article_source_country: Optional[str] = ""
    hostname: Optional[str] = ""
    metadata: Dict[str, Any] = {}
    entities: Optional[EntityModel] = None
    article_source_country: Optional[str] = ""
    geo_entities: Optional[list[GeoEntity]] = []
    scraped_at: str = ""
    word_count: int = 0
