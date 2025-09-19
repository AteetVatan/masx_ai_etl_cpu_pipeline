"""
Unit tests for the text cleaner module.

Tests text cleaning, normalization, and language-specific processing.
"""

import pytest
from unittest.mock import patch

from src.processing.cleaner import TextCleaner


class TestTextCleaner:
    """Test cases for TextCleaner class."""
    
    @pytest.fixture
    def cleaner(self):
        """Create a text cleaner instance for testing."""
        return TextCleaner()
    
    def test_init(self, cleaner):
        """Test cleaner initialization."""
        assert cleaner.max_length == 50000  # Default max length
        assert cleaner.enable_cleaning == True  # Default enabled
        assert len(cleaner.compiled_unwanted) > 0  # Should have compiled patterns
    
    def test_clean_text_basic(self, cleaner):
        """Test basic text cleaning."""
        text = "This is a test article with   multiple   spaces and\n\nnewlines."
        
        result = cleaner.clean_text(text, "en")
        
        assert result["cleaned_text"] == "This is a test article with multiple spaces and newlines."
        assert result["original_length"] == len(text)
        assert result["cleaned_length"] < len(text)
        assert result["language"] == "en"
        assert result["cleaning_applied"] == True
        assert "whitespace_normalization" in result["removed_elements"]
    
    def test_clean_text_with_urls_and_emails(self, cleaner):
        """Test cleaning URLs and email addresses."""
        text = "Visit https://example.com for more info. Contact us at test@example.com or call 555-123-4567."
        
        result = cleaner.clean_text(text, "en")
        
        assert "https://example.com" not in result["cleaned_text"]
        assert "test@example.com" not in result["cleaned_text"]
        assert "555-123-4567" not in result["cleaned_text"]
        assert "urls_emails" in result["removed_elements"]
    
    def test_clean_text_with_html_entities(self, cleaner):
        """Test cleaning HTML entities."""
        text = "This is a test with &amp; HTML entities &lt; and &gt; symbols."
        
        result = cleaner.clean_text(text, "en")
        
        assert "&amp;" not in result["cleaned_text"]
        assert "&lt;" not in result["cleaned_text"]
        assert "&gt;" not in result["cleaned_text"]
        assert "&" in result["cleaned_text"]
        assert "<" in result["cleaned_text"]
        assert ">" in result["cleaned_text"]
        assert "html_entities" in result["removed_elements"]
    
    def test_clean_text_remove_unwanted_patterns(self, cleaner):
        """Test removal of unwanted patterns."""
        text = "This is an article. Advertisement: Buy now! Subscribe to our newsletter. The content continues here."
        
        result = cleaner.clean_text(text, "en")
        
        assert "Advertisement" not in result["cleaned_text"]
        assert "Subscribe" not in result["cleaned_text"]
        assert "This is an article" in result["cleaned_text"]
        assert "The content continues here" in result["cleaned_text"]
        assert "unwanted_patterns" in result["removed_elements"]
    
    def test_clean_text_unicode_normalization(self, cleaner):
        """Test Unicode normalization."""
        text = "Café résumé naïve"
        
        result = cleaner.clean_text(text, "en")
        
        assert result["cleaned_text"] == "Café résumé naïve"  # Should be normalized
        assert "unicode_normalization" in result["removed_elements"]
    
    def test_clean_text_language_specific_english(self, cleaner):
        """Test English-specific cleaning."""
        text = "So, well, this is a test article. You know, it's basically about technology. Um, let me explain."
        
        result = cleaner.clean_text(text, "en")
        
        # Should remove common English filler words
        assert "So," not in result["cleaned_text"]
        assert "well," not in result["cleaned_text"]
        assert "You know," not in result["cleaned_text"]
        assert "basically" not in result["cleaned_text"]
        assert "Um," not in result["cleaned_text"]
        assert "this is a test article" in result["cleaned_text"]
        assert "let me explain" in result["cleaned_text"]
    
    def test_clean_text_language_specific_spanish(self, cleaner):
        """Test Spanish-specific cleaning."""
        text = "Bueno, este es un artículo de prueba. Pues, es sobre tecnología. ¿Sabes? Es muy interesante."
        
        result = cleaner.clean_text(text, "es")
        
        # Should remove common Spanish filler words
        assert "Bueno," not in result["cleaned_text"]
        assert "Pues," not in result["cleaned_text"]
        assert "¿Sabes?" not in result["cleaned_text"]
        assert "este es un artículo de prueba" in result["cleaned_text"]
        assert "Es muy interesante" in result["cleaned_text"]
    
    def test_clean_text_language_specific_french(self, cleaner):
        """Test French-specific cleaning."""
        text = "Bon, c'est un article de test. Alors, c'est sur la technologie. Euh, c'est très intéressant."
        
        result = cleaner.clean_text(text, "fr")
        
        # Should remove common French filler words
        assert "Bon," not in result["cleaned_text"]
        assert "Alors," not in result["cleaned_text"]
        assert "Euh," not in result["cleaned_text"]
        assert "c'est un article de test" in result["cleaned_text"]
        assert "c'est très intéressant" in result["cleaned_text"]
    
    def test_clean_text_language_specific_german(self, cleaner):
        """Test German-specific cleaning."""
        text = "Also, das ist ein Testartikel. Dann, es geht um Technologie. Äh, es ist sehr interessant."
        
        result = cleaner.clean_text(text, "de")
        
        # Should remove common German filler words
        assert "Also," not in result["cleaned_text"]
        assert "Dann," not in result["cleaned_text"]
        assert "Äh," not in result["cleaned_text"]
        assert "das ist ein Testartikel" in result["cleaned_text"]
        assert "es ist sehr interessant" in result["cleaned_text"]
    
    def test_clean_text_truncation(self, cleaner):
        """Test text truncation when too long."""
        # Create a very long text
        long_text = "This is a test sentence. " * 2000  # Much longer than max_length
        
        result = cleaner.clean_text(long_text, "en")
        
        assert len(result["cleaned_text"]) <= cleaner.max_length
        assert result["cleaned_text"].endswith("...")
        assert "truncation" in result["removed_elements"]
    
    def test_clean_text_disabled(self, cleaner):
        """Test cleaning when disabled."""
        cleaner.enable_cleaning = False
        text = "This is a test with   multiple   spaces."
        
        result = cleaner.clean_text(text, "en")
        
        assert result["cleaned_text"] == text
        assert result["cleaning_applied"] == False
        assert result["removed_elements"] == []
    
    def test_clean_text_empty_input(self, cleaner):
        """Test cleaning with empty input."""
        result = cleaner.clean_text("", "en")
        
        assert result["cleaned_text"] == ""
        assert result["original_length"] == 0
        assert result["cleaned_length"] == 0
        assert result["cleaning_applied"] == False
    
    def test_clean_text_none_input(self, cleaner):
        """Test cleaning with None input."""
        result = cleaner.clean_text(None, "en")
        
        assert result["cleaned_text"] == ""
        assert result["original_length"] == 0
        assert result["cleaned_length"] == 0
        assert result["cleaning_applied"] == False
    
    def test_extract_sentences(self, cleaner):
        """Test sentence extraction."""
        text = "This is the first sentence. This is the second sentence! This is the third sentence? This is too short."
        
        sentences = cleaner.extract_sentences(text, min_length=10)
        
        assert len(sentences) == 3  # Should exclude the short sentence
        assert "This is the first sentence" in sentences[0]
        assert "This is the second sentence" in sentences[1]
        assert "This is the third sentence" in sentences[2]
    
    def test_extract_keywords(self, cleaner):
        """Test keyword extraction."""
        text = "This is a test article about technology and innovation. Technology is important for the future. Innovation drives progress."
        
        keywords = cleaner.extract_keywords(text, max_keywords=5)
        
        assert len(keywords) <= 5
        assert "technology" in keywords
        assert "innovation" in keywords
        assert "article" in keywords
        # Should not include common stop words
        assert "this" not in keywords
        assert "is" not in keywords
        assert "a" not in keywords
    
    def test_normalize_unicode(self, cleaner):
        """Test Unicode normalization."""
        # Test with combining characters
        text = "Café"  # é is U+00E9
        result = cleaner._normalize_unicode(text)
        assert result == "Café"
        
        # Test with control characters
        text = "Test\x00\x01\x02string"
        result = cleaner._normalize_unicode(text)
        assert "\x00" not in result
        assert "\x01" not in result
        assert "\x02" not in result
        assert "Test" in result
        assert "string" in result
    
    def test_decode_html_entities(self, cleaner):
        """Test HTML entity decoding."""
        text = "This &amp; that &lt; 5 &gt; 3 &quot;quoted&quot;"
        result = cleaner._decode_html_entities(text)
        
        assert "&amp;" not in result
        assert "&lt;" not in result
        assert "&gt;" not in result
        assert "&quot;" not in result
        assert "&" in result
        assert "<" in result
        assert ">" in result
        assert '"' in result
    
    def test_remove_urls_and_emails(self, cleaner):
        """Test URL and email removal."""
        text = "Visit https://example.com and http://test.org. Contact test@example.com or admin@test.org. Call 555-123-4567 or +1-555-987-6543."
        
        result, count = cleaner._remove_urls_and_emails(text)
        
        assert "https://example.com" not in result
        assert "http://test.org" not in result
        assert "test@example.com" not in result
        assert "admin@test.org" not in result
        assert "555-123-4567" not in result
        assert "+1-555-987-6543" not in result
        assert count > 0
    
    def test_normalize_whitespace(self, cleaner):
        """Test whitespace normalization."""
        text = "This   has    multiple     spaces\n\nand\n\n\nnewlines."
        result = cleaner._normalize_whitespace(text)
        
        assert "   " not in result  # Multiple spaces should be single
        assert "\n\n" not in result  # Multiple newlines should be single
        assert " " in result  # Single spaces should remain
        assert result.strip() == result  # Should be trimmed
    
    def test_truncate_text(self, cleaner):
        """Test text truncation."""
        text = "This is a very long text that needs to be truncated because it exceeds the maximum length allowed for processing."
        
        # Test truncation at word boundary
        result = cleaner._truncate_text(text, 50)
        assert len(result) <= 53  # 50 + "..."
        assert result.endswith("...")
        
        # Test truncation when no good word boundary
        short_text = "Thisisaverylongwordwithoutspaces"
        result = cleaner._truncate_text(short_text, 10)
        assert len(result) <= 13  # 10 + "..."
        assert result.endswith("...")
    
    def test_final_validation(self, cleaner):
        """Test final validation and cleanup."""
        text = "  This   has   extra   spaces  \n\n  And empty lines  \n\n  "
        result = cleaner._final_validation(text)
        
        assert result.strip() == result  # Should be trimmed
        assert "   " not in result  # Multiple spaces should be single
        assert "\n\n" not in result  # Multiple newlines should be single
        assert "This has extra spaces" in result
        assert "And empty lines" in result
