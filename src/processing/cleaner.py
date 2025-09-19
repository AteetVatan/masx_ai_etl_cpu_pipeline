"""
Text cleaner for MASX AI ETL CPU Pipeline.

Provides comprehensive text cleaning and normalization for multilingual content
with support for various languages and text formats.
"""

import re
import unicodedata
from typing import str, Optional, Dict, Any
import logging

from ..config.settings import settings


logger = logging.getLogger(__name__)


class TextCleaner:
    """
    Advanced text cleaner for multilingual content.
    
    Handles normalization, cleaning, and formatting of text content
    from various sources and languages.
    """
    
    def __init__(self):
        """Initialize the text cleaner with configuration."""
        self.max_length = settings.max_article_length
        self.enable_cleaning = settings.clean_text
        
        # Common patterns for cleaning
        self.whitespace_pattern = re.compile(r'\s+')
        self.url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        self.phone_pattern = re.compile(r'(\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}')
        
        # HTML entity patterns
        self.html_entity_pattern = re.compile(r'&[a-zA-Z0-9#]+;')
        
        # Common unwanted patterns
        self.unwanted_patterns = [
            r'Advertisement',
            r'Advertise',
            r'Subscribe',
            r'Newsletter',
            r'Follow us',
            r'Share this',
            r'Like this',
            r'Comment',
            r'Comments',
            r'Related articles',
            r'You may also like',
            r'Read more',
            r'Continue reading',
            r'Show more',
            r'Load more',
            r'Click here',
            r'More info',
            r'Learn more',
            r'Find out more'
        ]
        
        # Compile unwanted patterns for efficiency
        self.compiled_unwanted = [re.compile(pattern, re.IGNORECASE) for pattern in self.unwanted_patterns]
        
        logger.info("Text cleaner initialized")
    
    def clean_text(self, text: str, language: str = "en") -> Dict[str, Any]:
        """
        Clean and normalize text content.
        
        Args:
            text: Raw text to clean
            language: Language code for language-specific cleaning
            
        Returns:
            Dictionary containing cleaned text and metadata
        """
        if not text or not isinstance(text, str):
            return {
                "cleaned_text": "",
                "original_length": 0,
                "cleaned_length": 0,
                "removed_elements": [],
                "language": language,
                "cleaning_applied": False
            }
        
        if not self.enable_cleaning:
            return {
                "cleaned_text": text,
                "original_length": len(text),
                "cleaned_length": len(text),
                "removed_elements": [],
                "language": language,
                "cleaning_applied": False
            }
        
        logger.debug(f"Cleaning text of length {len(text)} in language {language}")
        
        original_text = text
        removed_elements = []
        
        # Step 1: Basic normalization
        text = self._normalize_unicode(text)
        removed_elements.append("unicode_normalization")
        
        # Step 2: Remove HTML entities
        text = self._decode_html_entities(text)
        removed_elements.append("html_entities")
        
        # Step 3: Remove unwanted patterns
        text, removed_count = self._remove_unwanted_patterns(text)
        if removed_count > 0:
            removed_elements.append(f"unwanted_patterns({removed_count})")
        
        # Step 4: Clean URLs and emails
        text, url_count = self._remove_urls_and_emails(text)
        if url_count > 0:
            removed_elements.append(f"urls_emails({url_count})")
        
        # Step 5: Normalize whitespace
        text = self._normalize_whitespace(text)
        removed_elements.append("whitespace_normalization")
        
        # Step 6: Language-specific cleaning
        text = self._apply_language_specific_cleaning(text, language)
        removed_elements.append(f"language_specific_{language}")
        
        # Step 7: Truncate if too long
        if len(text) > self.max_length:
            text = self._truncate_text(text, self.max_length)
            removed_elements.append("truncation")
        
        # Step 8: Final validation
        text = self._final_validation(text)
        
        return {
            "cleaned_text": text,
            "original_length": len(original_text),
            "cleaned_length": len(text),
            "removed_elements": removed_elements,
            "language": language,
            "cleaning_applied": True,
            "compression_ratio": len(text) / len(original_text) if original_text else 0
        }
    
    def _normalize_unicode(self, text: str) -> str:
        """Normalize Unicode characters."""
        # Normalize to NFC form
        text = unicodedata.normalize('NFC', text)
        
        # Remove control characters except newlines and tabs
        text = ''.join(char for char in text if unicodedata.category(char) != 'Cc' or char in '\n\t')
        
        return text
    
    def _decode_html_entities(self, text: str) -> str:
        """Decode HTML entities."""
        import html
        
        # Decode common HTML entities
        text = html.unescape(text)
        
        # Remove any remaining HTML entity patterns
        text = self.html_entity_pattern.sub('', text)
        
        return text
    
    def _remove_unwanted_patterns(self, text: str) -> tuple[str, int]:
        """Remove common unwanted patterns."""
        removed_count = 0
        
        for pattern in self.compiled_unwanted:
            matches = pattern.findall(text)
            if matches:
                text = pattern.sub('', text)
                removed_count += len(matches)
        
        return text, removed_count
    
    def _remove_urls_and_emails(self, text: str) -> tuple[str, int]:
        """Remove URLs and email addresses."""
        removed_count = 0
        
        # Remove URLs
        urls = self.url_pattern.findall(text)
        if urls:
            text = self.url_pattern.sub('', text)
            removed_count += len(urls)
        
        # Remove email addresses
        emails = self.email_pattern.findall(text)
        if emails:
            text = self.email_pattern.sub('', text)
            removed_count += len(emails)
        
        # Remove phone numbers
        phones = self.phone_pattern.findall(text)
        if phones:
            text = self.phone_pattern.sub('', text)
            removed_count += len(phones)
        
        return text, removed_count
    
    def _normalize_whitespace(self, text: str) -> str:
        """Normalize whitespace characters."""
        # Replace multiple whitespace with single space
        text = self.whitespace_pattern.sub(' ', text)
        
        # Remove leading and trailing whitespace
        text = text.strip()
        
        return text
    
    def _apply_language_specific_cleaning(self, text: str, language: str) -> str:
        """Apply language-specific cleaning rules."""
        if language == "en":
            return self._clean_english_text(text)
        elif language == "es":
            return self._clean_spanish_text(text)
        elif language == "fr":
            return self._clean_french_text(text)
        elif language == "de":
            return self._clean_german_text(text)
        else:
            return self._clean_generic_text(text)
    
    def _clean_english_text(self, text: str) -> str:
        """Clean English text."""
        # Remove common English filler words at sentence boundaries
        patterns = [
            r'\b(So|Well|Now|Actually|Basically|Literally|Obviously|Clearly)\s*,?\s*',
            r'\b(You know|I mean|Like|Right|Okay|OK)\s*,?\s*',
            r'\b(Um|Uh|Ah|Oh)\s*,?\s*'
        ]
        
        for pattern in patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        return text
    
    def _clean_spanish_text(self, text: str) -> str:
        """Clean Spanish text."""
        # Remove common Spanish filler words
        patterns = [
            r'\b(Bueno|Pues|Entonces|O sea|Es decir|O sea que)\s*,?\s*',
            r'\b(¿Sabes?|¿Verdad?|¿No?)\s*,?\s*'
        ]
        
        for pattern in patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        return text
    
    def _clean_french_text(self, text: str) -> str:
        """Clean French text."""
        # Remove common French filler words
        patterns = [
            r'\b(Bon|Alors|Donc|En fait|En gros|Genre|Comme)\s*,?\s*',
            r'\b(Hein|Euh|Ah|Oh)\s*,?\s*'
        ]
        
        for pattern in patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        return text
    
    def _clean_german_text(self, text: str) -> str:
        """Clean German text."""
        # Remove common German filler words
        patterns = [
            r'\b(Also|Dann|Doch|Eigentlich|Einfach|Halt|Ja|Nein)\s*,?\s*',
            r'\b(Äh|Ähm|Hmm)\s*,?\s*'
        ]
        
        for pattern in patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        return text
    
    def _clean_generic_text(self, text: str) -> str:
        """Clean text for unknown languages."""
        # Remove common punctuation issues
        text = re.sub(r'\.{2,}', '.', text)  # Multiple periods
        text = re.sub(r'!{2,}', '!', text)  # Multiple exclamations
        text = re.sub(r'\?{2,}', '?', text)  # Multiple questions
        
        return text
    
    def _truncate_text(self, text: str, max_length: int) -> str:
        """Truncate text to maximum length while preserving word boundaries."""
        if len(text) <= max_length:
            return text
        
        # Find the last complete word within the limit
        truncated = text[:max_length]
        last_space = truncated.rfind(' ')
        
        if last_space > max_length * 0.8:  # Only truncate at word boundary if it's not too short
            return truncated[:last_space] + "..."
        else:
            return truncated + "..."
    
    def _final_validation(self, text: str) -> str:
        """Final validation and cleanup."""
        # Remove any remaining excessive whitespace
        text = self.whitespace_pattern.sub(' ', text)
        
        # Ensure text ends properly
        text = text.strip()
        
        # Remove any remaining empty lines
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        text = '\n'.join(lines)
        
        return text
    
    def extract_sentences(self, text: str, min_length: int = 10) -> list[str]:
        """
        Extract sentences from cleaned text.
        
        Args:
            text: Cleaned text
            min_length: Minimum sentence length
            
        Returns:
            List of sentences
        """
        if not text:
            return []
        
        # Simple sentence splitting (can be enhanced with NLTK/spaCy)
        sentences = re.split(r'[.!?]+', text)
        
        # Clean and filter sentences
        cleaned_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) >= min_length:
                cleaned_sentences.append(sentence)
        
        return cleaned_sentences
    
    def extract_keywords(self, text: str, max_keywords: int = 10) -> list[str]:
        """
        Extract potential keywords from text.
        
        Args:
            text: Cleaned text
            max_keywords: Maximum number of keywords to return
            
        Returns:
            List of potential keywords
        """
        if not text:
            return []
        
        # Simple keyword extraction (can be enhanced with NLP libraries)
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
        
        # Count word frequencies
        word_counts = {}
        for word in words:
            word_counts[word] = word_counts.get(word, 0) + 1
        
        # Sort by frequency and return top keywords
        sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        keywords = [word for word, count in sorted_words[:max_keywords]]
        
        return keywords


# Global text cleaner instance
text_cleaner = TextCleaner()
