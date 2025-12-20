#!/usr/bin/env python3
"""
Quick analysis of the current final_clean_dataset.csv to identify issues
"""

import pandas as pd
import numpy as np
from datetime import datetime

def analyze_current_dataset():
    """Analyze the current dataset and identify issues"""
    print("🔍 ANALYZING CURRENT DATASET: final_clean_dataset.csv")
    print("=" * 60)
    
    try:
        # Load dataset
        df = pd.read_csv("final_clean_dataset.csv")
        print(f"✅ Dataset loaded: {len(df):,} records")
        
        # Basic info
        print(f"\n📊 BASIC INFORMATION:")
        print(f"   Columns: {len(df.columns)}")
        print(f"   Records: {len(df):,}")
        
        # Check timestamp format issues
        print(f"\n⏰ TIMESTAMP ANALYSIS:")
        timestamp_samples = df['timestamp'].head(10).tolist()
        print("   Sample timestamps:")
        for i, ts in enumerate(timestamp_samples[:5]):
            print(f"     {i+1}. {ts}")
        
        # Look for inconsistent timestamp formats
        timestamp_formats = set()
        for ts in df['timestamp'].head(100):
            if 'T' in str(ts):
                if str(ts).endswith('Z'):
                    timestamp_formats.add("ISO_WITH_Z")
                elif 'z' in str(ts).lower():
                    timestamp_formats.add("ISO_WITH_LOWERCASE_Z")
                else:
                    timestamp_formats.add("ISO_NO_Z")
            else:
                timestamp_formats.add("NON_ISO")
        
        print(f"   Timestamp formats found: {timestamp_formats}")
        if len(timestamp_formats) > 1:
            print("   ❌ ISSUE: Inconsistent timestamp formats detected!")
        
        # Check for case inconsistencies
        print(f"\n🔤 CASE CONSISTENCY ANALYSIS:")
        
        # Check user names
        users_with_mixed_case = []
        for user in df['user'].unique()[:20]:
            if user and isinstance(user, str):
                if user != user.lower() and user != user.upper():
                    users_with_mixed_case.append(user)
        
        if users_with_mixed_case:
            print(f"   ❌ ISSUE: Mixed case in usernames:")
            for user in users_with_mixed_case[:5]:
                print(f"     - {user}")
        
        # Check behavior_type consistency
        if 'behavior_type' in df.columns:
            behavior_types = df['behavior_type'].unique()
            print(f"   Behavior types: {list(behavior_types)}")
            
            mixed_case_behaviors = []
            for bt in behavior_types:
                if bt and isinstance(bt, str):
                    if bt != bt.upper() and bt != bt.lower():
                        mixed_case_behaviors.append(bt)
            
            if mixed_case_behaviors:
                print(f"   ❌ ISSUE: Mixed case in behavior_type:")
                for bt in mixed_case_behaviors:
                    print(f"     - {bt}")
        
        # Check for empty queries with metadata comments
        print(f"\n📝 QUERY ANALYSIS:")
        empty_queries = df[df['query'] == ''].shape[0]
        queries_with_sim_meta = df[df['query'].str.contains('SIM_META', case=False, na=False)].shape[0]
        
        print(f"   Empty queries: {empty_queries:,}")
        print(f"   Queries with SIM_META: {queries_with_sim_meta:,}")
        
        if empty_queries > 0:
            print("   ❌ ISSUE: Empty queries found!")
        
        # Check for data type inconsistencies
        print(f"\n🔢 DATA TYPE ANALYSIS:")
        numeric_columns = ['query_length', 'execution_time_ms', 'is_anomaly', 'has_error']
        for col in numeric_columns:
            if col in df.columns:
                non_numeric = df[col].apply(lambda x: not str(x).replace('.', '').isdigit() if pd.notna(x) else False).sum()
                if non_numeric > 0:
                    print(f"   ❌ ISSUE: Non-numeric values in {col}: {non_numeric}")
        
        # Check for missing critical data
        print(f"\n❓ MISSING DATA ANALYSIS:")
        critical_columns = ['timestamp', 'user', 'database', 'is_anomaly']
        for col in critical_columns:
            if col in df.columns:
                missing = df[col].isna().sum()
                if missing > 0:
                    print(f"   ❌ ISSUE: Missing data in {col}: {missing:,} records")
        
        # Check for duplicate records
        print(f"\n🔄 DUPLICATE ANALYSIS:")
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            print(f"   ❌ ISSUE: Duplicate records found: {duplicates:,}")
        else:
            print("   ✅ No duplicate records")
        
        print(f"\n" + "=" * 60)
        print("🎯 ANALYSIS COMPLETE")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error analyzing dataset: {e}")

if __name__ == "__main__":
    analyze_current_dataset()