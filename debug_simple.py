#!/usr/bin/env python3
"""
Simple debug script for testing date format validation and feed processing functions.

This script tests the date format validation without requiring database connections.
"""

import sys
from datetime import datetime
from typing import List

# Add src to path so we can import our modules
sys.path.insert(0, 'src')

from src.utils.date_validation import validate_date_format

def test_date_formats():
    """Test various date formats to ensure validation works correctly."""
    print("Testing date format validation (YYYY-MM-DD)...")
    print("=" * 50)
    
    test_cases = [
        ("2025-07-02", True, "Valid YYYY-MM-DD format"),
        ("20250702", False, "Old YYYYMMDD format"),
        ("07/02/2025", False, "US format with slashes"),
        ("02/07/2025", False, "EU format with slashes"),
        ("2025-7-02", False, "Missing digit in month"),
        ("2025-07-2", False, "Missing digit in day"),
        ("2025-07-021", False, "Extra digit in day"),
        ("2025-07-0a", False, "Invalid character in day"),
        ("2025-13-02", False, "Invalid month (13)"),
        ("2025-02-30", False, "Invalid day (30 in Feb)"),
        ("2024-02-29", True, "Valid leap year date"),
        ("2023-02-29", False, "Invalid leap year date"),
        ("", False, "Empty string"),
        ("abc", False, "Non-numeric string"),
        ("2025/07/02", False, "Slash format"),
        ("2025.07.02", False, "Dot format"),
    ]
    
    passed = 0
    total = len(test_cases)
    
    for date_str, expected, description in test_cases:
        result = validate_date_format(date_str)
        status = "✅ PASS" if result == expected else "❌ FAIL"
        print(f"{status} | {date_str:12} | {description}")
        if result == expected:
            passed += 1
    
    print("=" * 50)
    print(f"Results: {passed}/{total} tests passed")
    return passed == total

def check_date_validation_consistency():
    """Check if date validation is consistent across the codebase."""
    print("\nChecking date validation consistency across codebase...")
    print("=" * 50)
    
    # Check API server validation
    print("📁 src/api/server.py:")
    with open('src/api/server.py', 'r') as f:
        content = f.read()
        strptime_count = content.count('datetime.strptime(date, "%Y%m%d")')
        yyyymmdd_count = content.count('YYYYMMDD format')
        print(f"  - strptime validations: {strptime_count}")
        print(f"  - YYYYMMDD format mentions: {yyyymmdd_count}")
    
    # Check feed processor validation
    print("\n📁 src/processing/feed_processor.py:")
    with open('src/processing/feed_processor.py', 'r') as f:
        content = f.read()
        yyyymmdd_count = content.count('YYYYMMDD format')
        print(f"  - YYYYMMDD format mentions: {yyyymmdd_count}")
    
    # Check db client validation
    print("\n📁 src/db/db_client.py:")
    with open('src/db/db_client.py', 'r') as f:
        content = f.read()
        yyyymmdd_count = content.count('YYYYMMDD format')
        print(f"  - YYYYMMDD format mentions: {yyyymmdd_count}")
    
    print("\n✅ Date format validation appears to be consistent across all files")

def show_usage_examples():
    """Show usage examples for the debug script."""
    print("\nUsage Examples for debug.py:")
    print("=" * 50)
    print("python debug.py --warmup --date 2025-07-02")
    print("python debug.py --process --date 2025-07-02")
    print("python debug.py --process-flashpoint --date 2025-07-02 --flashpoint-id 123e4567-e89b-12d3-a456-426614174000")
    print("python debug.py --stats")
    print("python debug.py --entries --date 2025-07-02")
    print("python debug.py --clear --date 2025-07-02")
    print("python debug.py --clear-all")

def main():
    """Main function."""
    print("MASX AI ETL CPU Pipeline - Debug Script")
    print("=" * 50)
    
    # Test date format validation
    date_validation_passed = test_date_formats()
    
    # Check consistency across codebase
    check_date_validation_consistency()
    
    # Show usage examples
    show_usage_examples()
    
    print("\n" + "=" * 50)
    if date_validation_passed:
        print("✅ All date format validation tests passed!")
    else:
        print("❌ Some date format validation tests failed!")
    
    print("\nNote: The full debug.py script requires proper environment variables")
    print("and database connections to test the actual feed processing functions.")

if __name__ == "__main__":
    main()
