"""
Geotagging module for MASX AI ETL CPU Pipeline.

Uses multilingual NER and pycountry for accurate geographic entity recognition
and normalization across multiple languages.
"""

import re
from typing import List, Dict, Any, Optional, Tuple
import logging

try:
    import spacy
    from spacy import displacy
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False

try:
    import pycountry
    PYCOUNTRY_AVAILABLE = True
except ImportError:
    PYCOUNTRY_AVAILABLE = False

from ..config.settings import settings


logger = logging.getLogger(__name__)


class Geotagger:
    """
    Multilingual geotagging system using spaCy NER and pycountry.
    
    Identifies and normalizes geographic entities in text across multiple
    languages with high accuracy and confidence scoring.
    """
    
    def __init__(self):
        """Initialize the geotagger with language models."""
        self.enabled = settings.enable_geotagging
        self.models = {}
        self.country_mappings = {}
        self.city_mappings = {}
        
        if not SPACY_AVAILABLE:
            logger.warning("spaCy not available - geotagging disabled")
            self.enabled = False
            return
        
        if not PYCOUNTRY_AVAILABLE:
            logger.warning("pycountry not available - geotagging disabled")
            self.enabled = False
            return
        
        # Initialize country and city mappings
        self._initialize_mappings()
        
        # Load language models
        self._load_models()
        
        logger.info("Geotagger initialized with multilingual support")
    
    def _initialize_mappings(self):
        """Initialize country and city mappings for normalization."""
        # Country mappings (common variations to ISO codes)
        self.country_mappings = {
            # English variations
            "united states": "US", "usa": "US", "america": "US",
            "united kingdom": "GB", "uk": "GB", "britain": "GB",
            "russia": "RU", "russian federation": "RU",
            "china": "CN", "people's republic of china": "CN",
            "germany": "DE", "deutschland": "DE",
            "france": "FR", "french republic": "FR",
            "spain": "ES", "spain": "ES", "españa": "ES",
            "italy": "IT", "italia": "IT",
            "japan": "JP", "nippon": "JP",
            "south korea": "KR", "korea": "KR",
            "north korea": "KP", "dprk": "KP",
            "ukraine": "UA", "ukraina": "UA",
            "poland": "PL", "polska": "PL",
            "canada": "CA", "canada": "CA",
            "australia": "AU", "australia": "AU",
            "brazil": "BR", "brasil": "BR",
            "india": "IN", "bharat": "IN",
            "mexico": "MX", "méxico": "MX",
            "argentina": "AR", "argentina": "AR",
            "south africa": "ZA", "zuid-afrika": "ZA",
            "egypt": "EG", "misr": "EG",
            "turkey": "TR", "türkiye": "TR",
            "iran": "IR", "islamic republic of iran": "IR",
            "iraq": "IQ", "iraq": "IQ",
            "syria": "SY", "syrian arab republic": "SY",
            "israel": "IL", "israel": "IL",
            "palestine": "PS", "palestinian territories": "PS",
            "saudi arabia": "SA", "kingdom of saudi arabia": "SA",
            "uae": "AE", "united arab emirates": "AE",
            "qatar": "QA", "qatar": "QA",
            "kuwait": "KW", "kuwait": "KW",
            "bahrain": "BH", "bahrain": "BH",
            "oman": "OM", "sultanate of oman": "OM",
            "yemen": "YE", "yemen republic": "YE",
            "lebanon": "LB", "lebanese republic": "LB",
            "jordan": "JO", "hashemite kingdom of jordan": "JO",
            "cyprus": "CY", "cyprus republic": "CY",
            "greece": "GR", "hellenic republic": "GR",
            "bulgaria": "BG", "republic of bulgaria": "BG",
            "romania": "RO", "românia": "RO",
            "hungary": "HU", "magyarország": "HU",
            "czech republic": "CZ", "czechia": "CZ",
            "slovakia": "SK", "slovak republic": "SK",
            "slovenia": "SI", "republic of slovenia": "SI",
            "croatia": "HR", "republic of croatia": "HR",
            "bosnia and herzegovina": "BA", "bosnia": "BA",
            "serbia": "RS", "republic of serbia": "RS",
            "montenegro": "ME", "montenegro": "ME",
            "albania": "AL", "republic of albania": "AL",
            "north macedonia": "MK", "macedonia": "MK",
            "kosovo": "XK", "republic of kosovo": "XK",
            "moldova": "MD", "republic of moldova": "MD",
            "belarus": "BY", "republic of belarus": "BY",
            "lithuania": "LT", "republic of lithuania": "LT",
            "latvia": "LV", "republic of latvia": "LV",
            "estonia": "EE", "republic of estonia": "EE",
            "finland": "FI", "finnish republic": "FI",
            "sweden": "SE", "kingdom of sweden": "SE",
            "norway": "NO", "kingdom of norway": "NO",
            "denmark": "DK", "kingdom of denmark": "DK",
            "iceland": "IS", "republic of iceland": "IS",
            "ireland": "IE", "republic of ireland": "IE",
            "portugal": "PT", "portuguese republic": "PT",
            "switzerland": "CH", "swiss confederation": "CH",
            "austria": "AT", "republic of austria": "AT",
            "belgium": "BE", "kingdom of belgium": "BE",
            "netherlands": "NL", "kingdom of the netherlands": "NL",
            "luxembourg": "LU", "grand duchy of luxembourg": "LU",
            "monaco": "MC", "principality of monaco": "MC",
            "liechtenstein": "LI", "principality of liechtenstein": "LI",
            "san marino": "SM", "republic of san marino": "SM",
            "vatican": "VA", "vatican city": "VA",
            "malta": "MT", "republic of malta": "MT",
            "andorra": "AD", "principality of andorra": "AD",
        }
        
        # City mappings (common variations to standardized names)
        self.city_mappings = {
            # Major cities
            "new york": "New York",
            "new york city": "New York",
            "nyc": "New York",
            "london": "London",
            "paris": "Paris",
            "berlin": "Berlin",
            "madrid": "Madrid",
            "rome": "Rome",
            "moscow": "Moscow",
            "tokyo": "Tokyo",
            "beijing": "Beijing",
            "peking": "Beijing",
            "shanghai": "Shanghai",
            "hong kong": "Hong Kong",
            "singapore": "Singapore",
            "sydney": "Sydney",
            "melbourne": "Melbourne",
            "toronto": "Toronto",
            "vancouver": "Vancouver",
            "mexico city": "Mexico City",
            "são paulo": "São Paulo",
            "sao paulo": "São Paulo",
            "buenos aires": "Buenos Aires",
            "cairo": "Cairo",
            "istanbul": "Istanbul",
            "constantinople": "Istanbul",
            "tehran": "Tehran",
            "baghdad": "Baghdad",
            "damascus": "Damascus",
            "jerusalem": "Jerusalem",
            "tel aviv": "Tel Aviv",
            "riyadh": "Riyadh",
            "dubai": "Dubai",
            "doha": "Doha",
            "kuwait city": "Kuwait City",
            "manama": "Manama",
            "muscat": "Muscat",
            "sanaa": "Sana'a",
            "beirut": "Beirut",
            "amman": "Amman",
            "nicosia": "Nicosia",
            "athens": "Athens",
            "sofia": "Sofia",
            "bucharest": "Bucharest",
            "budapest": "Budapest",
            "prague": "Prague",
            "bratislava": "Bratislava",
            "ljubljana": "Ljubljana",
            "zagreb": "Zagreb",
            "sarajevo": "Sarajevo",
            "belgrade": "Belgrade",
            "podgorica": "Podgorica",
            "tirana": "Tirana",
            "skopje": "Skopje",
            "pristina": "Pristina",
            "chisinau": "Chișinău",
            "minsk": "Minsk",
            "vilnius": "Vilnius",
            "riga": "Riga",
            "tallinn": "Tallinn",
            "helsinki": "Helsinki",
            "stockholm": "Stockholm",
            "oslo": "Oslo",
            "copenhagen": "Copenhagen",
            "reykjavik": "Reykjavik",
            "dublin": "Dublin",
            "lisbon": "Lisbon",
            "bern": "Bern",
            "vienna": "Vienna",
            "brussels": "Brussels",
            "amsterdam": "Amsterdam",
            "luxembourg city": "Luxembourg",
            "monaco": "Monaco",
            "vaduz": "Vaduz",
            "san marino": "San Marino",
            "vatican city": "Vatican City",
            "valletta": "Valletta",
            "andorra la vella": "Andorra la Vella",
        }
    
    def _load_models(self):
        """Load spaCy language models."""
        # Language models to load
        language_models = {
            "en": "en_core_web_sm",
            "es": "es_core_news_sm",
            "fr": "fr_core_news_sm",
            "de": "de_core_news_sm",
            "it": "it_core_news_sm",
            "pt": "pt_core_news_sm",
            "ru": "ru_core_news_sm",
            "zh": "zh_core_web_sm",
            "ja": "ja_core_news_sm",
            "ar": "ar_core_news_sm",
        }
        
        for lang, model_name in language_models.items():
            try:
                self.models[lang] = spacy.load(model_name)
                logger.info(f"Loaded {model_name} model for {lang}")
            except OSError:
                logger.warning(f"Could not load {model_name} model for {lang}")
                # Try to download the model
                try:
                    spacy.cli.download(model_name)
                    self.models[lang] = spacy.load(model_name)
                    logger.info(f"Downloaded and loaded {model_name} model for {lang}")
                except Exception as e:
                    logger.error(f"Failed to download {model_name} model for {lang}: {e}")
    
    def extract_geographic_entities(self, text: str, language: str = "en") -> Dict[str, Any]:
        """
        Extract geographic entities from text.
        
        Args:
            text: Text to analyze
            language: Language code for model selection
            
        Returns:
            Dictionary containing extracted geographic entities
        """
        if not self.enabled or not text:
            return {
                "countries": [],
                "cities": [],
                "regions": [],
                "other_locations": [],
                "confidence_scores": {},
                "language": language,
                "extraction_method": "disabled"
            }
        
        # Select appropriate model
        model = self.models.get(language, self.models.get("en"))
        if not model:
            logger.warning(f"No model available for language {language}")
            return {
                "countries": [],
                "cities": [],
                "regions": [],
                "other_locations": [],
                "confidence_scores": {},
                "language": language,
                "extraction_method": "no_model"
            }
        
        logger.debug(f"Extracting geographic entities from {len(text)} characters in {language}")
        
        # Process text with spaCy
        doc = model(text)
        
        # Extract entities
        countries = []
        cities = []
        regions = []
        other_locations = []
        confidence_scores = {}
        
        for ent in doc.ents:
            if ent.label_ in ["GPE", "LOC", "FAC"]:  # Geopolitical entities, locations, facilities
                entity_text = ent.text.strip()
                entity_type = self._classify_geographic_entity(entity_text, ent.label_)
                
                # Normalize entity
                normalized = self._normalize_geographic_entity(entity_text, entity_type)
                
                if normalized:
                    if entity_type == "country":
                        countries.append(normalized)
                    elif entity_type == "city":
                        cities.append(normalized)
                    elif entity_type == "region":
                        regions.append(normalized)
                    else:
                        other_locations.append(normalized)
                    
                    # Store confidence score
                    confidence_scores[entity_text] = {
                        "normalized": normalized,
                        "type": entity_type,
                        "spacy_label": ent.label_,
                        "confidence": getattr(ent, 'confidence', 0.5)
                    }
        
        # Remove duplicates while preserving order
        countries = list(dict.fromkeys(countries))
        cities = list(dict.fromkeys(cities))
        regions = list(dict.fromkeys(regions))
        other_locations = list(dict.fromkeys(other_locations))
        
        return {
            "countries": countries,
            "cities": cities,
            "regions": regions,
            "other_locations": other_locations,
            "confidence_scores": confidence_scores,
            "language": language,
            "extraction_method": "spacy_ner"
        }
    
    def _classify_geographic_entity(self, entity_text: str, spacy_label: str) -> str:
        """Classify a geographic entity as country, city, region, or other."""
        entity_lower = entity_text.lower()
        
        # Check if it's a known country
        if entity_lower in self.country_mappings:
            return "country"
        
        # Check if it's a known city
        if entity_lower in self.city_mappings:
            return "city"
        
        # Use spaCy label as hint
        if spacy_label == "GPE":
            # Geopolitical entities are usually countries or cities
            if len(entity_text.split()) == 1:
                return "city"  # Single word is likely a city
            else:
                return "country"  # Multiple words likely a country
        elif spacy_label == "LOC":
            return "region"  # Locations are usually regions
        elif spacy_label == "FAC":
            return "other"  # Facilities are other locations
        
        return "other"
    
    def _normalize_geographic_entity(self, entity_text: str, entity_type: str) -> Optional[str]:
        """Normalize a geographic entity to a standard form."""
        entity_lower = entity_text.lower()
        
        if entity_type == "country":
            # Check country mappings
            if entity_lower in self.country_mappings:
                iso_code = self.country_mappings[entity_lower]
                try:
                    country = pycountry.countries.get(alpha_2=iso_code)
                    return country.name if country else entity_text
                except Exception:
                    return entity_text
            
            # Try to find by name
            try:
                country = pycountry.countries.get(name=entity_text)
                if country:
                    return country.name
            except Exception:
                pass
            
            # Try to find by common name
            try:
                country = pycountry.countries.get(common_name=entity_text)
                if country:
                    return country.name
            except Exception:
                pass
        
        elif entity_type == "city":
            # Check city mappings
            if entity_lower in self.city_mappings:
                return self.city_mappings[entity_lower]
            
            # Return as-is for cities (no comprehensive database available)
            return entity_text.title()
        
        elif entity_type == "region":
            # Return as-is for regions
            return entity_text.title()
        
        else:
            # Return as-is for other locations
            return entity_text.title()
    
    def get_country_info(self, country_name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a country.
        
        Args:
            country_name: Country name or ISO code
            
        Returns:
            Dictionary with country information or None
        """
        if not PYCOUNTRY_AVAILABLE:
            return None
        
        try:
            # Try to find by name
            country = pycountry.countries.get(name=country_name)
            if not country:
                # Try by common name
                country = pycountry.countries.get(common_name=country_name)
            if not country:
                # Try by alpha_2 code
                country = pycountry.countries.get(alpha_2=country_name.upper())
            if not country:
                # Try by alpha_3 code
                country = pycountry.countries.get(alpha_3=country_name.upper())
            
            if country:
                return {
                    "name": country.name,
                    "common_name": getattr(country, 'common_name', country.name),
                    "alpha_2": country.alpha_2,
                    "alpha_3": country.alpha_3,
                    "numeric": country.numeric,
                    "official_name": getattr(country, 'official_name', country.name)
                }
        except Exception as e:
            logger.error(f"Error getting country info for {country_name}: {e}")
        
        return None
    
    def validate_geographic_entities(self, entities: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and enrich geographic entities with additional information.
        
        Args:
            entities: Dictionary of extracted entities
            
        Returns:
            Enriched entities dictionary
        """
        if not entities:
            return entities
        
        enriched_entities = entities.copy()
        
        # Enrich countries with additional info
        enriched_countries = []
        for country in entities.get("countries", []):
            country_info = self.get_country_info(country)
            if country_info:
                enriched_countries.append({
                    "name": country,
                    "info": country_info
                })
            else:
                enriched_countries.append({
                    "name": country,
                    "info": None
                })
        
        enriched_entities["countries"] = enriched_countries
        
        return enriched_entities


# Global geotagger instance
geotagger = Geotagger()
