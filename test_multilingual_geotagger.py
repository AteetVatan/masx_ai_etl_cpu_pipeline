#!/usr/bin/env python3
"""
Test script for the updated multilingual geotagger.
Demonstrates support for 60+ languages using en_core_web_sm and xx_ent_wiki_sm.
"""

import sys
import os
import asyncio
import logging

# Add src to path
sys.path.insert(0, 'src')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set minimal environment variables
os.environ.setdefault('ENABLE_GEOTAGGING', 'True')
os.environ.setdefault('LOG_LEVEL', 'INFO')

from src.processing.geotagger import geotagger

def test_multilingual_support():
    """Test the multilingual geotagger with various languages."""
    
    print("🌍 Testing Multilingual Geotagger")
    print("=" * 50)
    
    # Test cases in different languages
    test_cases = [
        {
            "text": "The United States and China are discussing trade relations in Washington D.C.",
            "language": "en",
            "description": "English text"
        },
        {
            "text": "La France et l'Allemagne collaborent à Paris et Berlin.",
            "language": "fr", 
            "description": "French text"
        },
        {
            "text": "Deutschland und Österreich arbeiten in Wien und München zusammen.",
            "language": "de",
            "description": "German text"
        },
        {
            "text": "España y Portugal están en la península ibérica.",
            "language": "es",
            "description": "Spanish text"
        },
        {
            "text": "Россия и Украина ведут переговоры в Москве и Киеве.",
            "language": "ru",
            "description": "Russian text"
        },
        {
            "text": "中国和日本在北京和东京进行会谈。",
            "language": "zh",
            "description": "Chinese text"
        },
        {
            "text": "العربية والمملكة العربية السعودية في الرياض والدمام.",
            "language": "ar",
            "description": "Arabic text"
        },
        {
            "text": "日本と韓国は東京とソウルで会談している。",
            "language": "ja",
            "description": "Japanese text"
        }
    ]
    
    print(f"✅ Geotagger enabled: {geotagger.enabled}")
    print(f"✅ Models loaded: {list(geotagger.models.keys())}")
    print(f"✅ Supported languages: {len(geotagger.get_supported_languages())}")
    print()
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"Test {i}: {test_case['description']}")
        print(f"Language: {test_case['language']}")
        print(f"Text: {test_case['text']}")
        
        # Check if language is supported
        is_supported = geotagger.is_language_supported(test_case['language'])
        print(f"Language supported: {'✅' if is_supported else '❌'}")
        
        if is_supported:
            # Extract entities
            result = geotagger.extract_geographic_entities(
                test_case['text'], 
                test_case['language']
            )
            
            print(f"Model used: {result.get('model_used', 'unknown')}")
            print(f"Countries found: {result.get('countries', [])}")
            print(f"Cities found: {result.get('cities', [])}")
            print(f"Regions found: {result.get('regions', [])}")
            print(f"Other locations: {result.get('other_locations', [])}")
        else:
            print("❌ Language not supported")
        
        print("-" * 50)
        print()

def test_supported_languages():
    """Test the supported languages functionality."""
    print("🌐 Supported Languages Test")
    print("=" * 50)
    
    supported_languages = geotagger.get_supported_languages()
    print(f"Total supported languages: {len(supported_languages)}")
    print("Languages:", ", ".join(supported_languages))
    print()
    
    # Test some specific languages
    test_languages = ["en", "fr", "de", "es", "ru", "zh", "ar", "ja", "ko", "hi", "th", "vi"]
    
    print("Language support check:")
    for lang in test_languages:
        is_supported = geotagger.is_language_supported(lang)
        print(f"  {lang}: {'✅' if is_supported else '❌'}")

def main():
    """Main test function."""
    try:
        test_multilingual_support()
        test_supported_languages()
        
        print("🎉 All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    main()
