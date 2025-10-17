from typing import List
from pydantic import BaseModel

class EntityAttributes(BaseModel):
    """
    Represents a single extracted entity with its confidence score.

    Attributes:
        text (str): The actual text span recognized as an entity.
        score (float): Model-assigned confidence score (0.0–1.0) for this entity.
    """
    text: str
    score: float


class MetaAttributes(BaseModel):
    """
    Metadata about the NER (Named Entity Recognition) extraction process.

    Attributes:
        chars (int): Total number of characters processed.
        score (float): Average model confidence score across all entities.
        model (str): Identifier of the NER model used for extraction.
        chunks (int): Number of text chunks the document was split into before processing.
    """
    chars: int
    score: float
    model: str
    chunks: int


class EntityModel(BaseModel):
    """
    Structured container for all recognized named entities and extraction metadata.

    Entity categories follow the OntoNotes-style NER schema:

        PERSON   → Named people (e.g., "Elon Musk")
        ORG      → Organizations (e.g., "United Nations", "Google")
        GPE      → Geo-political entities (countries, cities, states) (e.g., "Germany")
        LOC      → Non-GPE locations (mountains, rivers) (e.g., "Himalayas")
        NORP     → Nationalities, religious or political groups (e.g., "French", "Buddhists")
        EVENT    → Named events (e.g., "World War II", "Olympics")
        LAW      → Laws, treaties, legal documents (e.g., "GDPR", "First Amendment")
        DATE     → Dates and relative time (e.g., "June 21, 2023", "yesterday")
        MONEY    → Monetary values (e.g., "$5 million", "€200")
        QUANTITY → Numeric quantities and units (e.g., "5 kg", "10 miles")

    Attributes:
        PERSON (List[EntityAttributes]): List of detected persons.
        ORG (List[EntityAttributes]): List of detected organizations.
        GPE (List[EntityAttributes]): List of detected geopolitical entities.
        LOC (List[EntityAttributes]): List of detected non-GPE locations.
        NORP (List[EntityAttributes]): List of detected nationality/religion/political groups.
        EVENT (List[EntityAttributes]): List of detected events.
        LAW (List[EntityAttributes]): List of detected laws and treaties.
        DATE (List[EntityAttributes]): List of detected dates and times.
        MONEY (List[EntityAttributes]): List of detected monetary values.
        QUANTITY (List[EntityAttributes]): List of detected quantities and measurements.
        meta (MetaAttributes): Metadata about the extraction process.
    """
    PERSON: List[EntityAttributes]
    ORG: List[EntityAttributes]
    GPE: List[EntityAttributes]
    LOC: List[EntityAttributes]
    NORP: List[EntityAttributes]
    EVENT: List[EntityAttributes]
    LAW: List[EntityAttributes]
    DATE: List[EntityAttributes]
    MONEY: List[EntityAttributes]
    QUANTITY: List[EntityAttributes]
    meta: MetaAttributes
