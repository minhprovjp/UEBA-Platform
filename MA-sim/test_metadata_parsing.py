#!/usr/bin/env python3
"""
Test metadata parsing with obfuscated SIM_META comments
"""

from perf_log_dataset_creator import extract_extended_metadata

def test_obfuscated_metadata_parsing():
    """Test parsing of obfuscated SIM_META comments"""
    print("🧪 Testing obfuscated SIM_META parsing...")
    
    # Test cases with different obfuscation patterns
    test_cases = [
        # Normal case
        "SELECT * FROM test /* SIM_META:thai_thanh_an|10.0.0.1|ID:test|BEH:NORMAL|ANO:0 */",
        
        # Obfuscated case from the dataset
        "/**//*/**/SIM_META:ta_minh_khang|10.0.0.78|ID:HACKER|BEH:MALICIOUS|ANO:1|TS:2025-12-19T20:20:44.217546Z|CPLX:1|PTN:FALLBACK|VNC:0|ASC:1.0/**/*/",
        
        # Another obfuscation pattern
        "/***SIM_META:test_user|192.168.1.1|ID:123|BEH:LOGIN|ANO:0***/",
        
        # Complex obfuscation
        "SELECT/**/*/**/test/**/FROM/**/users/**/SIM_META:complex_user|10.0.0.5|ID:complex|BEH:SEARCH|ANO:0|TS:2025-12-20T10:00:00Z/**/"
    ]
    
    for i, test_sql in enumerate(test_cases, 1):
        print(f"\n📝 Test case {i}:")
        print(f"   SQL: {test_sql[:80]}...")
        
        meta, clean_sql = extract_extended_metadata(test_sql, "fallback_user", "fallback_host")
        
        print(f"   Extracted user: {meta['sim_user']}")
        print(f"   Extracted behavior: {meta['beh_type']}")
        print(f"   Extracted anomaly: {meta['is_anomaly']}")
        
        if meta['sim_user'] != 'fallback_user':
            print(f"   ✅ Successfully parsed obfuscated SIM_META")
        else:
            print(f"   ❌ Failed to parse obfuscated SIM_META")

if __name__ == "__main__":
    test_obfuscated_metadata_parsing()