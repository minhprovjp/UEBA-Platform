#!/usr/bin/env python3
"""
Simple Dataset Analysis - Business Hours Check
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

DATASET_FILE = "final_clean_dataset_30d.csv"

def analyze():
    if not os.path.exists(DATASET_FILE):
        print("❌ Dataset not found!")
        return

    print("📊 Loading dataset...")
    df = pd.read_csv(DATASET_FILE)
    
    # Convert timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    print(f"✅ Loaded {len(df)} rows.")
    print("-" * 30)

    # 1. Anomaly ratio
    anomaly_counts = df['is_anomaly'].value_counts(normalize=True)
    print("1️⃣ ANOMALY RATIO:")
    print(anomaly_counts)
    print("-" * 30)

    # 2. Top Users
    print("2️⃣ TOP 10 USERS (Activity Count):")
    top_users = df['user'].value_counts().head(10)
    print(top_users)
    
    # User distribution chart
    plt.figure(figsize=(10, 6))
    user_counts = df['user'].value_counts().values
    plt.loglog(range(1, len(user_counts) + 1), user_counts, marker='o', linestyle='none', alpha=0.7)
    plt.title("User Activity Distribution (Zipf's Law Check)")
    plt.xlabel("User Rank (Log Scale)")
    plt.ylabel("Activity Count (Log Scale)")
    plt.grid(True, alpha=0.3)
    plt.savefig("user_dist_zipf.png", dpi=300, bbox_inches='tight')
    print("   -> Saved chart: user_dist_zipf.png")
    print("-" * 30)

    # 3. Inter-arrival times
    print("3️⃣ INTER-ARRIVAL TIMES (IAT) STATS:")
    df['prev_time'] = df.groupby('user')['timestamp'].shift(1)
    df['iat_sec'] = (df['timestamp'] - df['prev_time']).dt.total_seconds()
    iat_clean = df['iat_sec'].dropna()
    
    print(iat_clean.describe())
    
    # IAT distribution chart
    plt.figure(figsize=(12, 5))
    plt.subplot(1, 2, 1)
    iat_filtered = iat_clean[iat_clean <= 3600]  # <= 1 hour
    plt.hist(iat_filtered, bins=50, alpha=0.7, color='#3498db')
    plt.title('Inter-Arrival Time Distribution (≤ 1 hour)')
    plt.xlabel('Seconds')
    plt.ylabel('Frequency')
    
    plt.subplot(1, 2, 2)
    plt.boxplot(iat_filtered)
    plt.title('Inter-Arrival Time Box Plot')
    plt.ylabel('Seconds')
    
    plt.tight_layout()
    plt.savefig("iat_dist.png", dpi=300, bbox_inches='tight')
    print("   -> Saved chart: iat_dist.png")
    print("-" * 30)

    # 4. Hourly Activity (Business Hours Check)
    print("4️⃣ HOURLY ACTIVITY:")
    df['hour'] = df['timestamp'].dt.hour
    hourly_counts = df.groupby('hour').size()
    
    # Create hourly activity chart
    plt.figure(figsize=(12, 6))
    bars = plt.bar(hourly_counts.index, hourly_counts.values, color='#3498db', alpha=0.7)
    
    # Highlight business hours (8-17)
    for i, bar in enumerate(bars):
        if 8 <= hourly_counts.index[i] <= 17:
            bar.set_color('#2ecc71')  # Green for business hours
        elif 18 <= hourly_counts.index[i] <= 19:
            bar.set_color('#f39c12')  # Orange for extended hours
        else:
            bar.set_color('#e74c3c')  # Red for off-hours
    
    plt.title("Activity by Hour of Day (Business Hours Analysis)")
    plt.xlabel("Hour of Day")
    plt.ylabel("Query Count")
    plt.grid(True, alpha=0.3)
    
    # Add business hours annotation
    plt.axvspan(8, 17, alpha=0.2, color='green', label='Business Hours (8-17)')
    plt.axvspan(18, 19, alpha=0.2, color='orange', label='Extended Hours (18-19)')
    plt.legend()
    
    # Add statistics
    business_hours_count = hourly_counts[8:18].sum()  # 8-17 inclusive
    total_count = hourly_counts.sum()
    business_percentage = (business_hours_count / total_count) * 100
    
    plt.text(0.02, 0.98, f'Business Hours: {business_percentage:.1f}%\nOff Hours: {100-business_percentage:.1f}%', 
             transform=plt.gca().transAxes, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.savefig("hourly_activity.png", dpi=300, bbox_inches='tight')
    print("   -> Saved chart: hourly_activity.png")
    
    # Print hourly statistics
    print(f"\n📊 Business Hours Analysis:")
    print(f"   Business Hours (8-17): {business_hours_count:,} queries ({business_percentage:.1f}%)")
    print(f"   Off Hours: {total_count - business_hours_count:,} queries ({100-business_percentage:.1f}%)")
    
    # Show peak hours
    peak_hours = hourly_counts.nlargest(3)
    print(f"\n🕐 Peak Activity Hours:")
    for hour, count in peak_hours.items():
        percentage = (count / total_count) * 100
        print(f"   {hour:02d}:00 - {count:,} queries ({percentage:.1f}%)")

if __name__ == "__main__":
    analyze()