#!/usr/bin/env python3
"""
Comprehensive analysis of date format validation across the MASX AI ETL CPU Pipeline.

This script analyzes all date-related functions and parameters to ensure
consistent YYYYMMDD format validation throughout the codebase.
"""

import os
import re
from typing import List, Dict, Any

def analyze_file(file_path: str) -> Dict[str, Any]:
    """Analyze a single file for date validation patterns."""
    if not os.path.exists(file_path):
        return {"error": f"File not found: {file_path}"}
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    analysis = {
        "file": file_path,
        "strptime_validations": [],
        "date_parameters": [],
        "date_usage": [],
        "validation_coverage": "unknown"
    }
    
    # Find strptime validations and shared validation utility usage
    strptime_pattern = r'datetime\.strptime\([^,]+,\s*["\']%Y%m%d["\']\)'
    strptime_matches = re.finditer(strptime_pattern, content)
    for match in strptime_matches:
        line_num = content[:match.start()].count('\n') + 1
        analysis["strptime_validations"].append({
            "line": line_num,
            "code": match.group().strip()
        })
    
    # Find shared validation utility usage
    validation_patterns = [
        r'validate_and_raise\([^)]+\)',
        r'validate_date_format\([^)]+\)',
        r'from.*date_validation.*import'
    ]
    
    for pattern in validation_patterns:
        matches = re.finditer(pattern, content)
        for match in matches:
            line_num = content[:match.start()].count('\n') + 1
            analysis["strptime_validations"].append({
                "line": line_num,
                "code": match.group().strip(),
                "type": "shared_validation"
            })
    
    # Find date parameters in function signatures
    date_param_pattern = r'def\s+\w+\([^)]*date:\s*str[^)]*\)'
    date_param_matches = re.finditer(date_param_pattern, content)
    for match in date_param_matches:
        line_num = content[:match.start()].count('\n') + 1
        analysis["date_parameters"].append({
            "line": line_num,
            "signature": match.group().strip()
        })
    
    # Find date usage patterns
    date_usage_patterns = [
        r'date:\s*str\s*=',
        r'date:\s*str[^=]',
        r'date\s*=\s*datetime\.now\(\)\.strftime\(',
        r'feed_entries_\{date\}',
        r'table_name\s*=\s*f["\']feed_entries_\{date\}["\']'
    ]
    
    for pattern in date_usage_patterns:
        matches = re.finditer(pattern, content)
        for match in matches:
            line_num = content[:match.start()].count('\n') + 1
            analysis["date_usage"].append({
                "line": line_num,
                "pattern": pattern,
                "code": match.group().strip()
            })
    
    # Determine validation coverage
    has_strptime = len(analysis["strptime_validations"]) > 0
    has_date_params = len(analysis["date_parameters"]) > 0
    
    if has_strptime and has_date_params:
        analysis["validation_coverage"] = "validated"
    elif has_date_params and not has_strptime:
        analysis["validation_coverage"] = "missing_validation"
    elif not has_date_params:
        analysis["validation_coverage"] = "no_date_params"
    else:
        analysis["validation_coverage"] = "partial"
    
    return analysis

def main():
    """Main analysis function."""
    print("🔍 MASX AI ETL CPU Pipeline - Date Validation Analysis")
    print("=" * 60)
    
    # Files to analyze
    files_to_analyze = [
        "src/api/server.py",
        "src/db/db_client.py", 
        "src/processing/feed_processor.py",
        "debug.py",
        "debug_simple.py"
    ]
    
    all_analyses = []
    total_strptime = 0
    total_date_params = 0
    files_with_validation = 0
    files_missing_validation = 0
    
    for file_path in files_to_analyze:
        print(f"\n📁 Analyzing {file_path}...")
        analysis = analyze_file(file_path)
        all_analyses.append(analysis)
        
        if analysis.get("error"):
            print(f"   ❌ {analysis['error']}")
            continue
        
        strptime_count = len(analysis["strptime_validations"])
        date_param_count = len(analysis["date_parameters"])
        coverage = analysis["validation_coverage"]
        
        print(f"   📊 strptime validations: {strptime_count}")
        print(f"   📊 date parameters: {date_param_count}")
        print(f"   📊 coverage: {coverage}")
        
        total_strptime += strptime_count
        total_date_params += date_param_count
        
        if coverage == "validated":
            files_with_validation += 1
        elif coverage == "missing_validation":
            files_missing_validation += 1
        
        # Show specific validations
        if analysis["strptime_validations"]:
            print("   ✅ Validation points:")
            for val in analysis["strptime_validations"]:
                print(f"      Line {val['line']}: {val['code']}")
        
        # Show date parameters
        if analysis["date_parameters"]:
            print("   📝 Date parameters:")
            for param in analysis["date_parameters"]:
                print(f"      Line {param['line']}: {param['signature']}")
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 SUMMARY")
    print("=" * 60)
    print(f"Total strptime validations: {total_strptime}")
    print(f"Total date parameters: {total_date_params}")
    print(f"Files with validation: {files_with_validation}")
    print(f"Files missing validation: {files_missing_validation}")
    
    # Detailed analysis
    print("\n🔍 DETAILED ANALYSIS")
    print("=" * 60)
    
    for analysis in all_analyses:
        if analysis.get("error"):
            continue
            
        file_name = analysis["file"].split("/")[-1]
        coverage = analysis["validation_coverage"]
        
        if coverage == "validated":
            print(f"✅ {file_name}: Properly validated")
        elif coverage == "missing_validation":
            print(f"⚠️  {file_name}: Has date parameters but missing validation")
        elif coverage == "no_date_params":
            print(f"ℹ️  {file_name}: No date parameters")
        else:
            print(f"❓ {file_name}: {coverage}")
    
    # Recommendations
    print("\n💡 RECOMMENDATIONS")
    print("=" * 60)
    
    if files_missing_validation > 0:
        print("⚠️  Some files have date parameters without validation:")
        for analysis in all_analyses:
            if analysis.get("validation_coverage") == "missing_validation":
                print(f"   - {analysis['file']}")
        print("\n   Consider adding date format validation to these functions.")
    else:
        print("✅ All files with date parameters have proper validation!")
    
    print(f"\n📈 Overall validation coverage: {files_with_validation}/{len([a for a in all_analyses if not a.get('error')])} files")
    
    # Check for consistency
    print("\n🔍 CONSISTENCY CHECK")
    print("=" * 60)
    
    validation_patterns = set()
    for analysis in all_analyses:
        for val in analysis.get("strptime_validations", []):
            validation_patterns.add(val["code"])
    
    if len(validation_patterns) == 1:
        print("✅ All validations use the same pattern - consistent!")
    elif len(validation_patterns) > 1:
        print("⚠️  Multiple validation patterns found:")
        for pattern in validation_patterns:
            print(f"   - {pattern}")
        print("   Consider standardizing validation patterns.")
    else:
        print("ℹ️  No validation patterns found.")

if __name__ == "__main__":
    main()
