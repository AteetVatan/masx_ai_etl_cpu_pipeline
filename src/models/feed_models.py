from typing import Optional, Dict, Any, List
from pydantic import BaseModel
from .entity_model import EntityModel
from .geo_entity import GeoEntity


class FeedModel(BaseModel):
    id: str
    url: str
    title: str
    title_en: Optional[str] = None  # new
    content: Optional[str] = ""
    author: Optional[str] = ""
    published_date: Optional[str] = None
    flashpoint_id: Optional[str] = None
    domain: Optional[str] = None
    language: Optional[str] = None
    source_country: Optional[str] = None
    original_image: Optional[str] = None
    images: Optional[List[str]] = None  # new
    hostname: Optional[str] = None  # new
    entities: Optional[EntityModel] = None  # new #jsonb
    geo_entities: Optional[List[GeoEntity]] = None  # new #jsonb

    @classmethod
    def from_feed_entry(cls, feed_entry: Dict[str, Any]) -> "FeedModel":
        """Factory method to safely create FeedEntry from a raw dict"""
        return cls(
            id=feed_entry.get("id"),
            url=feed_entry.get("url"),
            title=feed_entry.get("title"),
            title_en=feed_entry.get("title_en"),
            content=feed_entry.get("content") or "",
            author=feed_entry.get("author") or "",
            published_date=feed_entry.get("published_date"),
            flashpoint_id=feed_entry.get("flashpoint_id"),
            domain=feed_entry.get("domain"),
            language=feed_entry.get("language"),
            source_country=feed_entry.get("source_country"),
            original_image=feed_entry.get("image"),
            images=feed_entry.get("images"),
            hostname=feed_entry.get("hostname"),
            compressed_content=feed_entry.get("compressed_content"),
            summary=feed_entry.get("summary"),
            entities=feed_entry.get("entities"),
            geo_entities=feed_entry.get("geo_entities"),
        )
