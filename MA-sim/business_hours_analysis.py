#!/usr/bin/env python3
"""
Detailed Business Hours Analysis
"""

import pandas as pd
import matplotlib.pyplot as plt

def analyze_business_hours():
    df = pd.read_csv("final_clean_dataset_30d.csv")
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    
    print("🏢 DETAILED BUSINESS HOURS ANALYSIS")
    print("=" * 50)
    
    # Hourly distribution
    hourly_counts = df['hour'].value_counts().sort_index()
    total_queries = len(df)
    
    print("📊 Hourly Activity Breakdown:")
    for hour in range(24):
        count = hourly_counts.get(hour, 0)
        percentage = (count / total_queries) * 100
        
        if hour == 12:
            status = "🍽️ LUNCH BREAK"
        elif 8 <= hour <= 17:
            status = "✅ BUSINESS HOURS"
        elif hour == 7 or hour in [18, 19]:
            status = "⚠️ EXTENDED HOURS"
        elif 20 <= hour <= 23 or 0 <= hour <= 6:
            status = "❌ OFF HOURS"
        else:
            status = "❓ OTHER"
        
        if count > 0:
            print(f"   {hour:02d}:00 - {count:3d} queries ({percentage:4.1f}%) {status}")
    
    # Business hours categories
    core_business = hourly_counts[8:12].sum() + hourly_counts[13:18].sum()  # Excluding lunch
    lunch_break = hourly_counts.get(12, 0)
    extended_hours = hourly_counts.get(7, 0) + hourly_counts[18:20].sum()
    
    # Calculate off-hours properly (only count hours that actually exist in data)
    off_hours_list = list(range(20, 24)) + list(range(0, 7))  # 20-23 and 0-6
    off_hours = sum(hourly_counts.get(hour, 0) for hour in off_hours_list)
    
    print(f"\n📈 Business Hours Summary:")
    print(f"   Core Business Hours (8-11, 13-17): {core_business:,} queries ({core_business/total_queries*100:.1f}%)")
    print(f"   Lunch Break (12:00): {lunch_break:,} queries ({lunch_break/total_queries*100:.1f}%)")
    print(f"   Extended Hours (7, 18-19): {extended_hours:,} queries ({extended_hours/total_queries*100:.1f}%)")
    print(f"   Off Hours (20-6): {off_hours:,} queries ({off_hours/total_queries*100:.1f}%)")
    
    # Weekend analysis
    weekday_counts = df['day_of_week'].value_counts().sort_index()
    weekdays = weekday_counts[0:5].sum()  # Monday-Friday
    weekends = weekday_counts[5:7].sum()  # Saturday-Sunday
    
    print(f"\n📅 Weekly Distribution:")
    print(f"   Weekdays (Mon-Fri): {weekdays:,} queries ({weekdays/total_queries*100:.1f}%)")
    print(f"   Weekends (Sat-Sun): {weekends:,} queries ({weekends/total_queries*100:.1f}%)")
    
    # Assessment
    print(f"\n🎯 BUSINESS HOURS COMPLIANCE ASSESSMENT:")
    
    if off_hours == 0:
        print("   ✅ EXCELLENT: No off-hours activity detected")
    elif off_hours < total_queries * 0.05:
        print("   ✅ VERY GOOD: Minimal off-hours activity (<5%)")
    else:
        print("   ⚠️ NEEDS REVIEW: Significant off-hours activity detected")
    
    if lunch_break == 0:
        print("   ✅ EXCELLENT: Proper lunch break observed (no activity at 12:00)")
    else:
        print("   ⚠️ MINOR: Some lunch break activity detected")
    
    if weekends < total_queries * 0.10:
        print("   ✅ GOOD: Minimal weekend activity (<10%)")
    else:
        print("   ⚠️ HIGH: Significant weekend activity detected")
    
    # Role-based analysis
    if 'user' in df.columns:
        print(f"\n👥 TOP ACTIVE USERS (Extended Hours Analysis):")
        extended_hours_df = df[df['hour'].isin([7, 18, 19])]
        if len(extended_hours_df) > 0:
            extended_users = extended_hours_df['user'].value_counts().head(5)
            for user, count in extended_users.items():
                print(f"   {user}: {count} extended-hour queries")
        else:
            print("   No extended hours activity detected")

if __name__ == "__main__":
    analyze_business_hours()