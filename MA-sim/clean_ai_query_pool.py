#!/usr/bin/env python3
"""
Clean AI Query Pool - Remove queries that trigger security rules
"""
import json
import sys
import os
import re

# Add parent dir to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from engine.utils import check_technical_attacks, check_insider_threats, check_access_anomalies, check_data_destruction
import pandas as pd

def test_query_against_rules(query):
    """Test a single query against all security rules"""
    # Create a minimal dataframe for testing
    test_df = pd.DataFrame([{
        'query': query,
        'normalized_query': query.upper(),
        'timestamp': pd.Timestamp.now(),
        'user': 'test_user',
        'client_ip': '192.168.1.1',
        'execution_time_ms': 10,
        'lock_time_ms': 1,
        'cpu_time_ms': 5,
        'rows_returned': 10,
        'rows_examined': 100,
        'rows_affected': 0,
        'error_code': 0,
        'warning_count': 0,
        'query_entropy': 4.5,
        'query_length': len(query),
        'is_system_table': 0,
        'scan_efficiency': 0.1,
        'is_admin_command': 0,
        'is_risky_command': 0,
        'has_comment': 0,
        'program_name': 'python',
        'connection_type': 'TCP/IP',
        'event_name': 'statement/sql/select',
        'created_tmp_disk_tables': 0,
        'created_tmp_tables': 0,
        'select_full_join': 0,
        'select_scan': 0,
        'sort_merge_passes': 0,
        'no_index_used': 0,
        'no_good_index_used': 0
    }])
    
    # Default rule config from engine_config.json template
    rule_config = {
        'thresholds': {
            'mass_deletion_rows': 500,
            'execution_time_limit_ms': 5000,
            'brute_force_attempts': 5,
            'concurrent_ips_limit': 1,
            'scan_efficiency_min': 0.01,
            'scan_efficiency_min_rows': 1000,
            'max_query_entropy': 6.0,
            'cpu_time_limit_ms': 1000,
            'lock_time_limit_ms': 500,
            'warning_count_threshold': 5,
            'index_evasion_min_rows': 1000
        },
        'signatures': {
            'sqli_keywords': [
                'UNION SELECT', 'UNION ALL SELECT', 'SLEEP(', 'BENCHMARK(',
                'OR 1=1', 'DROP TABLE', '--', '#', 'INFORMATION_SCHEMA',
                'UPDATEXML', 'EXTRACTVALUE', 'WHERE 1=1', 'GTID_SUBSET'
            ],
            'admin_keywords': ['GRANT ALL', 'CREATE USER', 'DROP USER', 'UPDATE mysql.user', 'SET GLOBAL'],
            'sensitive_tables': ['inventory_db', 'finance_db', 'hr_db', 'admin_db'],
            'large_dump_tables': ['customers', 'employees', 'salaries'],
            'disallowed_programs': ['sqlmap', 'nmap', 'python-requests', 'curl', 'perl'],
            'restricted_connection_users': ['root', 'admin']
        },
        'settings': {}
    }
    
    # Test against all rule groups
    triggered_rules = []
    
    try:
        technical = check_technical_attacks(test_df, rule_config)
        if technical:
            triggered_rules.extend(technical.keys())
    except:
        pass
    
    try:
        insider = check_insider_threats(test_df, rule_config)
        if insider:
            triggered_rules.extend(insider.keys())
    except:
        pass
    
    try:
        access = check_access_anomalies(test_df, rule_config)
        if access:
            triggered_rules.extend(access.keys())
    except:
        pass
    
    try:
        destruction = check_data_destruction(test_df, rule_config)
        if destruction:
            triggered_rules.extend(destruction.keys())
    except:
        pass
    
    return triggered_rules

def clean_query_pool(input_file, output_file):
    """Remove queries that trigger security rules"""
    print(f"🔍 Loading query pool from {input_file}...")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Handle both formats (with/without metadata)
    if 'queries' in data:
        pool = data['queries']
        has_metadata = True
    else:
        pool = data
        has_metadata = False
    
    total_before = 0
    total_after = 0
    removed_counts = {}
    
    cleaned_pool = {}
    
    for database, intents in pool.items():
        cleaned_pool[database] = {}
        
        for intent, queries in intents.items():
            total_before += len(queries)
            clean_queries = []
            removed_queries = []
            
            print(f"\n📊 Testing {database}.{intent} ({len(queries)} queries)...")
            
            for i, query in enumerate(queries, 1):
                triggered = test_query_against_rules(query)
                
                if triggered:
                    removed_queries.append({
                        'query': query,
                        'rules': triggered
                    })
                    print(f"  ❌ Query {i}: Triggers {', '.join(triggered)}")
                else:
                    clean_queries.append(query)
            
            cleaned_pool[database][intent] = clean_queries
            total_after += len(clean_queries)
            
            if removed_queries:
                removed_counts[f"{database}.{intent}"] = len(removed_queries)
                print(f"  ✅ Kept {len(clean_queries)}/{len(queries)} queries")
    
    # Save cleaned pool
    if has_metadata:
        output_data = {
            'metadata': data['metadata'],
            'queries': cleaned_pool
        }
    else:
        output_data = cleaned_pool
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"✅ CLEANUP COMPLETE")
    print(f"{'='*60}")
    print(f"Total queries before: {total_before}")
    print(f"Total queries after:  {total_after}")
    print(f"Queries removed:       {total_before - total_after}")
    print(f"Removal rate:          {((total_before - total_after) / total_before * 100):.1f}%")
    
    if removed_counts:
        print(f"\n📋 Removed by intent:")
        for intent, count in sorted(removed_counts.items(), key=lambda x: -x[1]):
            print(f"  • {intent}: {count}")
    
    print(f"\n💾 Cleaned pool saved to: {output_file}")

if __name__ == '__main__':
    input_file = 'dynamic_sql_generation/ai_query_pool.json'
    output_file = 'dynamic_sql_generation/ai_query_pool.json'
    backup_file = 'dynamic_sql_generation/ai_query_pool.json.backup'
    
    # Create backup
    import shutil
    if os.path.exists(input_file):
        shutil.copy(input_file, backup_file)
        print(f"📦 Backup created: {backup_file}")
    
    clean_query_pool(input_file, output_file)
