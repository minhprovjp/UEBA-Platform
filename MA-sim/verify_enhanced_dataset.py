#!/usr/bin/env python3
"""
Verify Enhanced Dataset - Check that all users are present
"""

import pandas as pd
import json

def verify_dataset():
    print("🔍 VERIFYING ENHANCED DATASET")
    print("=" * 50)
    
    # Load enhanced dataset
    df = pd.read_csv('final_enhanced_dataset_30d_all_users.csv')
    
    # Load config
    with open('simulation/users_config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Basic stats
    print(f"📊 Enhanced Dataset Summary:")
    print(f"   Total records: {len(df):,}")
    print(f"   Total users: {df['user'].nunique()}")
    print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    
    # Check role distribution
    users_in_dataset = set(df['user'].unique())
    all_users = config['users']
    included_roles = ['SALES', 'MARKETING', 'CUSTOMER_SERVICE', 'HR', 'FINANCE', 'DEV', 'MANAGEMENT', 'ADMIN']
    expected_users = set(user for user, role in all_users.items() if role in included_roles)
    
    role_counts = {}
    for user in users_in_dataset:
        role = all_users.get(user, 'UNKNOWN')
        role_counts[role] = role_counts.get(role, 0) + 1
    
    print(f"\n📈 Role Distribution in Enhanced Dataset:")
    for role, count in sorted(role_counts.items()):
        print(f"   {role}: {count} users")
    
    # Verify completeness
    missing_users = expected_users - users_in_dataset
    extra_users = users_in_dataset - expected_users
    
    print(f"\n✅ VERIFICATION RESULTS:")
    print(f"   Expected users: {len(expected_users)}")
    print(f"   Users in dataset: {len(users_in_dataset)}")
    print(f"   Missing users: {len(missing_users)}")
    print(f"   Extra users: {len(extra_users)}")
    
    if missing_users:
        print(f"   Missing: {list(missing_users)[:5]}...")
    
    if extra_users:
        print(f"   Extra: {list(extra_users)[:5]}...")
    
    if len(missing_users) == 0 and len(users_in_dataset) == len(expected_users):
        print(f"\n🎉 SUCCESS: All {len(expected_users)} expected users are present!")
        return True
    else:
        print(f"\n⚠️ ISSUE: Dataset is incomplete")
        return False

if __name__ == "__main__":
    verify_dataset()