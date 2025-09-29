#!/usr/bin/env python3
"""
Quick test to verify the refactored EntityTagger works with the new model structure.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.processing.entity_tragger import extract_entities
from src.models.entity_model import EntityModel, EntityAttributes, MetaAttributes

def test_entity_tagger_refactor():
    """Test that the refactored EntityTagger returns proper model structure."""
    
    # Test text
    test_text = """
    O presidente Lula da Silva anunciou hoje na COP30 que o Brasil investirá 
    R$ 80 bilhões na proteção da Amazônia. A Lei Geral do Meio Ambiente 
    foi aprovada pelo Congresso Nacional em 2024. O IBAMA monitora 20 mil km² 
    de floresta amazônica. Os indígenas brasileiros participaram das discussões.
    """
    
    print("Testing EntityTagger refactor...")
    print(f"Input text: {test_text.strip()}")
    print()
    
    try:
        # Extract entities
        result = extract_entities(test_text)
        
        # Verify it's an EntityModel instance
        assert isinstance(result, EntityModel), f"Expected EntityModel, got {type(result)}"
        
        # Verify meta structure
        assert isinstance(result.meta, MetaAttributes), f"Expected MetaAttributes, got {type(result.meta)}"
        assert hasattr(result.meta, 'chunks'), "Meta missing 'chunks' attribute"
        assert hasattr(result.meta, 'chars'), "Meta missing 'chars' attribute"
        assert hasattr(result.meta, 'model'), "Meta missing 'model' attribute"
        assert hasattr(result.meta, 'score'), "Meta missing 'score' attribute"
        
        # Verify entity lists are lists of EntityAttributes
        for entity_type in ['PERSON', 'ORG', 'GPE', 'LOC', 'NORP', 'EVENT', 'LAW', 'DATE', 'MONEY', 'QUANTITY']:
            entity_list = getattr(result, entity_type)
            assert isinstance(entity_list, list), f"{entity_type} should be a list"
            
            for entity in entity_list:
                assert isinstance(entity, EntityAttributes), f"Entity in {entity_type} should be EntityAttributes"
                assert hasattr(entity, 'text'), "Entity missing 'text' attribute"
                assert hasattr(entity, 'score'), "Entity missing 'score' attribute"
                assert isinstance(entity.text, str), "Entity text should be string"
                assert isinstance(entity.score, float), "Entity score should be float"
        
        print("✅ All tests passed!")
        print(f"Found {len(result.PERSON)} persons, {len(result.ORG)} organizations, {len(result.GPE)} geopolitical entities")
        print(f"Meta: {result.meta.chunks} chunks, {result.meta.chars} chars, model: {result.meta.model}")
        
        # Show some examples
        if result.PERSON:
            print(f"Example PERSON: {result.PERSON[0].text} (score: {result.PERSON[0].score})")
        if result.MONEY:
            print(f"Example MONEY: {result.MONEY[0].text} (score: {result.MONEY[0].score})")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = test_entity_tagger_refactor()
    sys.exit(0 if success else 1)
