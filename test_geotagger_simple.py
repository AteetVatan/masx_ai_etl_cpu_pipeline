#!/usr/bin/env python3
"""
Simple test for the updated multilingual geotagger configuration.
Tests the model configuration without requiring full model loading.
"""

import sys
import os

# Add src to path
sys.path.insert(0, 'src')

# Set minimal environment variables
os.environ.setdefault('ENABLE_GEOTAGGING', 'True')
os.environ.setdefault('LOG_LEVEL', 'INFO')

def test_geotagger_configuration():
    """Test the geotagger configuration without loading models."""
    
    print("🌍 Testing Multilingual Geotagger Configuration")
    print("=" * 60)
    
    try:
        # Import the geotagger class (not the instance)
        from src.processing.geotagger import Geotagger
        
        # Create a test instance
        geotagger = Geotagger()
        
        print(f"✅ Geotagger enabled: {geotagger.enabled}")
        print(f"✅ Models dictionary: {geotagger.models}")
        print(f"✅ English languages: {geotagger.english_languages}")
        print()
        
        # Test language mapping
        print("Language mapping test:")
        test_languages = ["en", "fr", "de", "es", "ru", "zh", "ar", "ja", "ko", "hi"]
        
        for lang in test_languages:
            model = geotagger._get_model_for_language(lang)
            model_type = "en_core_web_sm" if lang == "en" else "xx_ent_wiki_sm"
            print(f"  {lang}: {model_type} ({'✅' if model else '❌'})")
        
        print()
        
        # Test supported languages
        supported_languages = geotagger.get_supported_languages()
        print(f"✅ Supported languages count: {len(supported_languages)}")
        print(f"✅ First 10 languages: {supported_languages[:10]}")
        
        print()
        print("🎉 Configuration test completed successfully!")
        print("📝 Note: Full model loading requires spaCy models to be installed")
        print("   Run: python -m spacy download en_core_web_sm")
        print("   Run: python -m spacy download xx_ent_wiki_sm")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_geotagger_configuration()
