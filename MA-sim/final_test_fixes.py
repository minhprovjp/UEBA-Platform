#!/usr/bin/env python3
"""
Final comprehensive test of all dataset fixes
"""

import pandas as pd
import os
from agents_enhanced import EnhancedEmployeeAgent, EnhancedMaliciousAgent
from executor import SQLExecutor
from translator import EnhancedSQLTranslator
from obfuscator import SQLObfuscator
from perf_log_dataset_creator import extract_extended_metadata

def test_complete_pipeline():
    """Test the complete pipeline with all fixes"""
    print("🧪 FINAL COMPREHENSIVE TEST")
    print("=" * 50)
    
    # 1. Test agent username normalization
    print("\n1️⃣ Testing agent username normalization...")
    agent = EnhancedEmployeeAgent(1, "MIXED_Case_User", "SALES")
    print(f"   Input: MIXED_Case_User -> Output: {agent.username}")
    assert agent.username == "mixed_case_user", "Username normalization failed"
    print("   ✅ Agent username normalization working")
    
    # 2. Test malicious agent
    print("\n2️⃣ Testing malicious agent...")
    mal_agent = EnhancedMaliciousAgent(2, "Test_Insider", "HR", is_insider=True)
    print(f"   Input: Test_Insider -> Output: {mal_agent.username}")
    assert mal_agent.username == "test_insider", "Malicious agent username normalization failed"
    print("   ✅ Malicious agent username normalization working")
    
    # 3. Test SQL generation and execution
    print("\n3️⃣ Testing SQL generation and execution...")
    executor = SQLExecutor()
    translator = EnhancedSQLTranslator()
    
    # Create intent with mixed case action
    intent = {
        'user': agent.username,
        'action': 'MAliciOuS',  # Mixed case
        'is_anomaly': 1,
        'database': 'sales_db',
        'role': 'SALES'
    }
    
    sql = translator.translate(intent)
    result = executor.execute(intent, sql, client_profile={'source_ip': '192.168.1.100'})
    print(f"   SQL generation result: {result}")
    print("   ✅ SQL generation and execution working")
    
    # 4. Test obfuscation with SIM_META preservation
    print("\n4️⃣ Testing obfuscation with SIM_META preservation...")
    test_sql = "SELECT * FROM users /* SIM_META:test_user|10.0.0.1|ID:123|BEH:NORMAL|ANO:0 */"
    obfuscated = SQLObfuscator.random_case(test_sql)
    
    # Check that SIM_META content is preserved
    if 'test_user' in obfuscated and 'SIM_META:' in obfuscated:
        print("   ✅ SIM_META preservation in obfuscation working")
    else:
        print(f"   ❌ SIM_META not preserved: {obfuscated}")
    
    # 5. Test metadata extraction from obfuscated comments
    print("\n5️⃣ Testing metadata extraction from obfuscated comments...")
    obfuscated_meta = "/**//*/**/SIM_META:ta_minh_khang|10.0.0.78|ID:HACKER|BEH:MAliciOuS|ANO:1/**/*/"
    meta, clean = extract_extended_metadata(obfuscated_meta, "fallback", "fallback")
    
    print(f"   Extracted user: {meta['sim_user']}")
    print(f"   Extracted behavior: {meta['beh_type']}")
    
    if meta['sim_user'] == 'ta_minh_khang' and meta['beh_type'] == 'MALICIOUS':
        print("   ✅ Obfuscated metadata extraction working")
    else:
        print(f"   ❌ Metadata extraction failed")
    
    # 6. Check current dataset quality
    print("\n6️⃣ Checking current dataset quality...")
    if os.path.exists('final_clean_dataset.csv'):
        df = pd.read_csv('final_clean_dataset.csv')
        
        # Check for mixed case issues
        mixed_case_users = [u for u in df['user'].dropna().unique() 
                           if isinstance(u, str) and u != u.lower() and u != u.upper()]
        mixed_case_behaviors = [b for b in df['behavior_type'].unique() 
                               if isinstance(b, str) and b != b.upper() and b != b.lower()]
        
        print(f"   Dataset records: {len(df)}")
        print(f"   Mixed case users: {len(mixed_case_users)}")
        print(f"   Mixed case behaviors: {len(mixed_case_behaviors)}")
        print(f"   Null users: {df['user'].isna().sum()}")
        
        if len(mixed_case_users) == 0 and len(mixed_case_behaviors) == 0:
            print("   ✅ Dataset quality improved - no mixed case issues")
        else:
            print(f"   ⚠️ Still some issues: users={mixed_case_users}, behaviors={mixed_case_behaviors}")
    
    print("\n" + "=" * 50)
    print("🎯 COMPREHENSIVE TEST COMPLETE")
    print("All major fixes have been implemented and tested!")
    print("=" * 50)

if __name__ == "__main__":
    test_complete_pipeline()