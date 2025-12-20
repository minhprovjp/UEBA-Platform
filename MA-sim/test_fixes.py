#!/usr/bin/env python3
"""
Test script to verify the fixes for dataset issues
"""

from agents_enhanced import EnhancedEmployeeAgent, EnhancedMaliciousAgent
from executor import SQLExecutor
from obfuscator import SQLObfuscator
from perf_log_dataset_creator import extract_extended_metadata
import json

def test_username_normalization():
    """Test that usernames are properly normalized"""
    print("🔍 Testing username normalization...")
    
    # Test normal agent
    agent = EnhancedEmployeeAgent(1, "THAI_thaNH_An", "HR")
    print(f"   Original: THAI_thaNH_An -> Normalized: {agent.username}")
    assert agent.username == "thai_thanh_an", f"Expected 'thai_thanh_an', got '{agent.username}'"
    
    # Test malicious agent
    mal_agent = EnhancedMaliciousAgent(2, "Test_User", "SALES", is_insider=True)
    print(f"   Original: Test_User -> Normalized: {mal_agent.username}")
    assert mal_agent.username == "test_user", f"Expected 'test_user', got '{mal_agent.username}'"
    
    print("   ✅ Username normalization working correctly")

def test_metadata_extraction():
    """Test that metadata extraction normalizes behavior types and usernames"""
    print("\n🔍 Testing metadata extraction normalization...")
    
    # Test SQL with mixed case in SIM_META
    test_sql = "SELECT * FROM test /* SIM_META:THAI_thaNH_An|10.0.0.1|ID:test|BEH:MAliciOuS|ANO:1|TS:2025-12-20T10:00:00Z */"
    
    meta, clean_sql = extract_extended_metadata(test_sql, "db_user", "db_host")
    
    print(f"   Original username: THAI_thaNH_An -> Extracted: {meta['sim_user']}")
    print(f"   Original behavior: MAliciOuS -> Extracted: {meta['beh_type']}")
    
    # Check normalization
    if meta['sim_user'] == 'thai_thanh_an' and meta['beh_type'] == 'MALICIOUS':
        print("   ✅ Metadata extraction normalization working correctly")
    else:
        print(f"   ❌ Normalization failed - User: {meta['sim_user']}, Behavior: {meta['beh_type']}")
    
    return meta

def test_obfuscator_sim_meta_preservation():
    """Test that obfuscator preserves SIM_META comments"""
    print("\n🔍 Testing obfuscator SIM_META preservation...")
    
    sql_with_meta = "SELECT * FROM users /* SIM_META:thai_thanh_an|10.0.0.1|ID:test|BEH:NORMAL|ANO:0 */"
    obfuscated = SQLObfuscator.random_case(sql_with_meta)
    
    # Check that username in SIM_META is preserved
    if 'thai_thanh_an' in obfuscated and 'SIM_META:' in obfuscated:
        print("   ✅ SIM_META preservation working correctly")
        print(f"   Original: {sql_with_meta}")
        print(f"   Obfuscated: {obfuscated}")
    else:
        print(f"   ❌ SIM_META not preserved: {obfuscated}")

if __name__ == "__main__":
    print("🧪 TESTING DATASET FIXES")
    print("=" * 50)
    
    test_username_normalization()
    test_metadata_extraction()
    test_obfuscator_sim_meta_preservation()
    
    print("\n" + "=" * 50)
    print("🎯 TESTS COMPLETE")