"""
Unit tests for the geotagger module.

Tests geographic entity extraction, normalization, and country information retrieval.
"""

import pytest
from unittest.mock import patch, MagicMock

from src.processing.geotagger import Geotagger


class TestGeotagger:
    """Test cases for Geotagger class."""
    
    @pytest.fixture
    def geotagger(self):
        """Create a geotagger instance for testing."""
        return Geotagger()
    
    def test_init(self, geotagger):
        """Test geotagger initialization."""
        assert geotagger.enabled == True  # Should be enabled by default
        assert len(geotagger.country_mappings) > 0  # Should have country mappings
        assert len(geotagger.city_mappings) > 0  # Should have city mappings
    
    @patch('src.processing.geotagger.SPACY_AVAILABLE', False)
    def test_init_spacy_unavailable(self):
        """Test initialization when spaCy is not available."""
        geotagger = Geotagger()
        assert geotagger.enabled == False
    
    @patch('src.processing.geotagger.PYCOUNTRY_AVAILABLE', False)
    def test_init_pycountry_unavailable(self):
        """Test initialization when pycountry is not available."""
        geotagger = Geotagger()
        assert geotagger.enabled == False
    
    def test_classify_geographic_entity_country(self, geotagger):
        """Test country classification."""
        # Test known country
        assert geotagger._classify_geographic_entity("United States", "GPE") == "country"
        assert geotagger._classify_geographic_entity("USA", "GPE") == "country"
        assert geotagger._classify_geographic_entity("Germany", "GPE") == "country"
        
        # Test known city
        assert geotagger._classify_geographic_entity("New York", "GPE") == "city"
        assert geotagger._classify_geographic_entity("London", "GPE") == "city"
    
    def test_classify_geographic_entity_by_label(self, geotagger):
        """Test classification by spaCy label."""
        # GPE with single word (likely city)
        assert geotagger._classify_geographic_entity("Paris", "GPE") == "city"
        
        # GPE with multiple words (likely country)
        assert geotagger._classify_geographic_entity("United States", "GPE") == "country"
        
        # LOC (location/region)
        assert geotagger._classify_geographic_entity("North America", "LOC") == "region"
        
        # FAC (facility)
        assert geotagger._classify_geographic_entity("Central Park", "FAC") == "other"
    
    def test_normalize_geographic_entity_country(self, geotagger):
        """Test country normalization."""
        # Test known country mapping
        result = geotagger._normalize_geographic_entity("united states", "country")
        assert result == "United States"  # Should be normalized to proper name
        
        result = geotagger._normalize_geographic_entity("usa", "country")
        assert result == "United States"
        
        result = geotagger._normalize_geographic_entity("germany", "country")
        assert result == "Germany"
    
    def test_normalize_geographic_entity_city(self, geotagger):
        """Test city normalization."""
        # Test known city mapping
        result = geotagger._normalize_geographic_entity("new york", "city")
        assert result == "New York"
        
        result = geotagger._normalize_geographic_entity("london", "city")
        assert result == "London"
        
        # Test unknown city
        result = geotagger._normalize_geographic_entity("unknown city", "city")
        assert result == "Unknown City"  # Should be title-cased
    
    def test_normalize_geographic_entity_region(self, geotagger):
        """Test region normalization."""
        result = geotagger._normalize_geographic_entity("north america", "region")
        assert result == "North America"  # Should be title-cased
    
    def test_normalize_geographic_entity_other(self, geotagger):
        """Test other location normalization."""
        result = geotagger._normalize_geographic_entity("central park", "other")
        assert result == "Central Park"  # Should be title-cased
    
    @patch('src.processing.geotagger.pycountry.countries.get')
    def test_get_country_info_by_name(self, mock_get, geotagger):
        """Test getting country info by name."""
        mock_country = MagicMock()
        mock_country.name = "United States"
        mock_country.common_name = "United States"
        mock_country.alpha_2 = "US"
        mock_country.alpha_3 = "USA"
        mock_country.numeric = "840"
        mock_country.official_name = "United States of America"
        mock_get.return_value = mock_country
        
        result = geotagger.get_country_info("United States")
        
        assert result is not None
        assert result["name"] == "United States"
        assert result["common_name"] == "United States"
        assert result["alpha_2"] == "US"
        assert result["alpha_3"] == "USA"
        assert result["numeric"] == "840"
        assert result["official_name"] == "United States of America"
    
    @patch('src.processing.geotagger.pycountry.countries.get')
    def test_get_country_info_by_alpha_2(self, mock_get, geotagger):
        """Test getting country info by alpha-2 code."""
        mock_country = MagicMock()
        mock_country.name = "Germany"
        mock_country.alpha_2 = "DE"
        mock_get.return_value = mock_country
        
        result = geotagger.get_country_info("DE")
        
        assert result is not None
        assert result["name"] == "Germany"
        assert result["alpha_2"] == "DE"
    
    def test_get_country_info_not_found(self, geotagger):
        """Test getting country info for non-existent country."""
        result = geotagger.get_country_info("NonExistentCountry")
        assert result is None
    
    def test_get_country_info_pycountry_unavailable(self, geotagger):
        """Test getting country info when pycountry is unavailable."""
        with patch('src.processing.geotagger.PYCOUNTRY_AVAILABLE', False):
            result = geotagger.get_country_info("United States")
            assert result is None
    
    def test_validate_geographic_entities(self, geotagger):
        """Test geographic entity validation and enrichment."""
        entities = {
            "countries": ["United States", "Canada"],
            "cities": ["New York", "Toronto"],
            "regions": ["North America"],
            "other_locations": ["Central Park"]
        }
        
        with patch.object(geotagger, 'get_country_info') as mock_get_info:
            mock_get_info.side_effect = [
                {"name": "United States", "alpha_2": "US"},
                {"name": "Canada", "alpha_2": "CA"}
            ]
            
            result = geotagger.validate_geographic_entities(entities)
            
            assert "countries" in result
            assert len(result["countries"]) == 2
            
            # Check first country
            assert result["countries"][0]["name"] == "United States"
            assert result["countries"][0]["info"]["alpha_2"] == "US"
            
            # Check second country
            assert result["countries"][1]["name"] == "Canada"
            assert result["countries"][1]["info"]["alpha_2"] == "CA"
    
    def test_validate_geographic_entities_empty(self, geotagger):
        """Test validation with empty entities."""
        result = geotagger.validate_geographic_entities({})
        assert result == {}
    
    @patch('src.processing.geotagger.spacy.load')
    def test_load_models_success(self, mock_load, geotagger):
        """Test successful model loading."""
        mock_model = MagicMock()
        mock_load.return_value = mock_model
        
        # Mock the models dictionary
        geotagger.models = {}
        
        # Test loading a specific model
        geotagger._load_models()
        
        # Should have attempted to load models
        assert mock_load.called
    
    @patch('src.processing.geotagger.spacy.load')
    @patch('src.processing.geotagger.spacy.cli.download')
    def test_load_models_with_download(self, mock_download, mock_load, geotagger):
        """Test model loading with download fallback."""
        # First call raises OSError, second call succeeds
        mock_load.side_effect = [OSError("Model not found"), MagicMock()]
        mock_download.return_value = None
        
        geotagger.models = {}
        geotagger._load_models()
        
        # Should have attempted download
        assert mock_download.called
        assert mock_load.call_count >= 2
    
    def test_extract_geographic_entities_disabled(self, geotagger):
        """Test extraction when geotagger is disabled."""
        geotagger.enabled = False
        
        result = geotagger.extract_geographic_entities("Test text", "en")
        
        assert result["countries"] == []
        assert result["cities"] == []
        assert result["regions"] == []
        assert result["other_locations"] == []
        assert result["extraction_method"] == "disabled"
    
    def test_extract_geographic_entities_no_text(self, geotagger):
        """Test extraction with empty text."""
        result = geotagger.extract_geographic_entities("", "en")
        
        assert result["countries"] == []
        assert result["cities"] == []
        assert result["regions"] == []
        assert result["other_locations"] == []
    
    def test_extract_geographic_entities_no_model(self, geotagger):
        """Test extraction when no model is available."""
        geotagger.models = {}
        
        result = geotagger.extract_geographic_entities("Test text", "en")
        
        assert result["countries"] == []
        assert result["cities"] == []
        assert result["regions"] == []
        assert result["other_locations"] == []
        assert result["extraction_method"] == "no_model"
    
    @patch('src.processing.geotagger.spacy.load')
    def test_extract_geographic_entities_with_model(self, mock_load, geotagger):
        """Test extraction with a mock spaCy model."""
        # Create mock model and document
        mock_model = MagicMock()
        mock_doc = MagicMock()
        
        # Mock entity
        mock_entity = MagicMock()
        mock_entity.text = "New York"
        mock_entity.label_ = "GPE"
        mock_entity.confidence = 0.9
        
        mock_doc.ents = [mock_entity]
        mock_model.return_value = mock_doc
        mock_load.return_value = mock_model
        
        # Set up geotagger
        geotagger.models = {"en": mock_model}
        
        result = geotagger.extract_geographic_entities("I live in New York", "en")
        
        assert len(result["cities"]) > 0
        assert "New York" in result["cities"]
        assert result["extraction_method"] == "spacy_ner"
    
    def test_initialize_mappings(self, geotagger):
        """Test initialization of country and city mappings."""
        # Test country mappings
        assert "united states" in geotagger.country_mappings
        assert geotagger.country_mappings["united states"] == "US"
        assert "usa" in geotagger.country_mappings
        assert geotagger.country_mappings["usa"] == "US"
        
        # Test city mappings
        assert "new york" in geotagger.city_mappings
        assert geotagger.city_mappings["new york"] == "New York"
        assert "london" in geotagger.city_mappings
        assert geotagger.city_mappings["london"] == "London"
    
    def test_country_mappings_comprehensive(self, geotagger):
        """Test that country mappings are comprehensive."""
        # Test major countries
        major_countries = [
            "united states", "usa", "america",
            "united kingdom", "uk", "britain",
            "germany", "deutschland",
            "france", "spain", "italy",
            "china", "japan", "russia",
            "canada", "australia", "brazil",
            "india", "mexico", "argentina"
        ]
        
        for country in major_countries:
            assert country in geotagger.country_mappings, f"Missing mapping for {country}"
    
    def test_city_mappings_comprehensive(self, geotagger):
        """Test that city mappings are comprehensive."""
        # Test major cities
        major_cities = [
            "new york", "london", "paris", "berlin",
            "madrid", "rome", "moscow", "tokyo",
            "beijing", "shanghai", "sydney", "toronto"
        ]
        
        for city in major_cities:
            assert city in geotagger.city_mappings, f"Missing mapping for {city}"
