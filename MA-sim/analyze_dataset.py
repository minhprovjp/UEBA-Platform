#!/usr/bin/env python3
"""
Comprehensive Dataset Quality Analysis Tool
Advanced analysis to ensure Vietnamese Enterprise UBA dataset quality and production readiness
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Set style for better plots
plt.style.use('default')
sns.set_palette("husl")

DATASET_FILE = "final_clean_dataset_30d.csv"

class DatasetQualityAnalyzer:
    """Comprehensive dataset quality analyzer for Vietnamese Enterprise UBA simulation"""
    
    def __init__(self, dataset_file=DATASET_FILE):
        self.dataset_file = dataset_file
        self.df = None
        self.quality_score = 0
        self.issues = []
        self.recommendations = []
        
    def load_dataset(self):
        """Load and prepare dataset for analysis"""
        print("🔍 COMPREHENSIVE DATASET QUALITY ANALYSIS")
        print("=" * 60)
        print("Vietnamese Enterprise UBA Simulation Dataset")
        
        if not os.path.exists(self.dataset_file):
            print(f"❌ Dataset file not found: {self.dataset_file}")
            return False
        
        try:
            print(f"📂 Loading dataset: {self.dataset_file}")
            self.df = pd.read_csv(self.dataset_file)
            
            # Convert timestamp with proper handling
            self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
            self.df = self.df.sort_values('timestamp')
            
            print(f"✅ Dataset loaded successfully: {len(self.df):,} records")
            print(f"📅 Time range: {self.df['timestamp'].min()} to {self.df['timestamp'].max()}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error loading dataset: {e}")
            return False
    
    def analyze_data_integrity(self):
        """Analyze basic data integrity and structure"""
        print(f"\n🔍 DATA INTEGRITY ANALYSIS")
        print("=" * 50)
        
        total_records = len(self.df)
        print(f"📊 Total Records: {total_records:,}")
        
        # Check required columns
        required_columns = ['timestamp', 'user', 'database', 'query', 'has_error', 'is_anomaly']
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        
        if missing_columns:
            print(f"❌ Missing required columns: {missing_columns}")
            self.issues.append(f"Missing columns: {missing_columns}")
        else:
            print("✅ All required columns present")
            self.quality_score += 10
        
        # Check for missing values
        missing_data = self.df.isnull().sum()
        critical_missing = missing_data[missing_data > 0]
        
        if len(critical_missing) > 0:
            print(f"⚠️ Missing Data Found:")
            for col, count in critical_missing.items():
                percentage = (count / total_records) * 100
                print(f"   {col}: {count:,} ({percentage:.2f}%)")
                # Only flag as issue if it's a critical column (not error_message for clean datasets)
                if percentage > 5 and col not in ['error_message']:
                    self.issues.append(f"High missing data in {col}: {percentage:.1f}%")
                elif col == 'error_message' and percentage == 100:
                    print(f"   ℹ️ Note: 100% missing error_message is expected for clean datasets")
        else:
            print("✅ No missing data found")
            self.quality_score += 15
        
        # Check for duplicate records
        duplicates = self.df.duplicated().sum()
        if duplicates > 0:
            print(f"⚠️ Duplicate Records: {duplicates:,} ({duplicates/total_records*100:.2f}%)")
            self.issues.append(f"Duplicate records found: {duplicates}")
        else:
            print("✅ No duplicate records")
            self.quality_score += 10
        
        return True
    
    def analyze_vietnamese_authenticity(self):
        """Analyze Vietnamese business context authenticity"""
        print(f"\n🏢 VIETNAMESE BUSINESS AUTHENTICITY")
        print("=" * 50)
        
        # Check Vietnamese user names
        vietnamese_patterns = ['nguyen', 'tran', 'le', 'pham', 'hoang', 'phan', 'vu', 'dang', 'bui', 'do', 
                              'ly', 'duong', 'chu', 'quan', 'luu', 'ha', 'dinh', 'luong', 'quach', 'trieu']
        
        if 'user' in self.df.columns:
            unique_users = self.df['user'].unique()
            vietnamese_users = 0
            
            for user in unique_users:
                user_lower = user.lower()
                if any(pattern in user_lower for pattern in vietnamese_patterns):
                    vietnamese_users += 1
            
            vietnamese_percentage = (vietnamese_users / len(unique_users)) * 100
            print(f"📊 User Analysis:")
            print(f"   Total Users: {len(unique_users)}")
            print(f"   Vietnamese Names: {vietnamese_users} ({vietnamese_percentage:.1f}%)")
            
            if vietnamese_percentage >= 90:
                print(f"   ✅ EXCELLENT: High Vietnamese authenticity")
                self.quality_score += 15
            elif vietnamese_percentage >= 70:
                print(f"   ✅ GOOD: Adequate Vietnamese authenticity")
                self.quality_score += 10
            else:
                print(f"   ⚠️ LOW: Limited Vietnamese authenticity")
                self.issues.append(f"Low Vietnamese name authenticity: {vietnamese_percentage:.1f}%")
        
        # Check database names for Vietnamese business context
        if 'database' in self.df.columns:
            databases = self.df['database'].unique()
            expected_dbs = ['sales_db', 'inventory_db', 'finance_db', 'marketing_db', 'support_db', 'hr_db', 'admin_db']
            
            print(f"\n📊 Database Analysis:")
            print(f"   Databases Found: {', '.join([db for db in databases if db != 'unknown'])}")
            
            business_dbs = [db for db in databases if db in expected_dbs]
            if len(business_dbs) >= 5:
                print(f"   ✅ EXCELLENT: Comprehensive business database structure ({len(business_dbs)}/7)")
                self.quality_score += 15
            elif len(business_dbs) >= 3:
                print(f"   ✅ GOOD: Adequate business database structure ({len(business_dbs)}/7)")
                self.quality_score += 10
            else:
                print(f"   ⚠️ LIMITED: Basic database structure ({len(business_dbs)}/7)")
        
        return True
    
    def analyze_temporal_patterns(self):
        """Analyze temporal patterns and business hours compliance"""
        print(f"\n⏰ TEMPORAL PATTERN ANALYSIS")
        print("=" * 50)
        
        # Add time components
        self.df['hour'] = self.df['timestamp'].dt.hour
        self.df['day_of_week'] = self.df['timestamp'].dt.dayofweek
        self.df['date'] = self.df['timestamp'].dt.date
        
        # Hourly distribution analysis
        hourly_activity = self.df['hour'].value_counts().sort_index()
        total_activity = len(self.df)
        
        # Business hours analysis (8-17, excluding lunch 12-13)
        core_business_hours = list(range(8, 12)) + list(range(13, 18))
        business_hours_activity = sum(hourly_activity.get(hour, 0) for hour in core_business_hours)
        business_percentage = (business_hours_activity / total_activity) * 100
        
        # Extended hours (7, 18-19)
        extended_hours = [7, 18, 19]
        extended_activity = sum(hourly_activity.get(hour, 0) for hour in extended_hours)
        extended_percentage = (extended_activity / total_activity) * 100
        
        # Off hours (20-6)
        off_hours = list(range(20, 24)) + list(range(0, 7))
        off_hours_activity = sum(hourly_activity.get(hour, 0) for hour in off_hours)
        off_hours_percentage = (off_hours_activity / total_activity) * 100
        
        # Lunch break (12:00)
        lunch_activity = hourly_activity.get(12, 0)
        lunch_percentage = (lunch_activity / total_activity) * 100
        
        print(f"📊 Business Hours Distribution:")
        print(f"   Core Business Hours (8-11, 13-17): {business_hours_activity:,} queries ({business_percentage:.1f}%)")
        print(f"   Extended Hours (7, 18-19): {extended_activity:,} queries ({extended_percentage:.1f}%)")
        print(f"   Lunch Break (12:00): {lunch_activity:,} queries ({lunch_percentage:.1f}%)")
        print(f"   Off Hours (20-6): {off_hours_activity:,} queries ({off_hours_percentage:.1f}%)")
        
        # Quality assessment
        if off_hours_percentage == 0:
            print(f"   ✅ EXCELLENT: No off-hours activity")
            self.quality_score += 20
        elif off_hours_percentage < 5:
            print(f"   ✅ VERY GOOD: Minimal off-hours activity")
            self.quality_score += 15
        else:
            print(f"   ⚠️ POOR: Significant off-hours activity")
            self.issues.append(f"High off-hours activity: {off_hours_percentage:.1f}%")
        
        if lunch_percentage == 0:
            print(f"   ✅ EXCELLENT: Proper lunch break observed")
            self.quality_score += 10
        elif lunch_percentage < 2:
            print(f"   ✅ GOOD: Minimal lunch break activity")
            self.quality_score += 5
        
        # Weekend analysis
        weekday_activity = self.df[self.df['day_of_week'] < 5]  # Monday-Friday
        weekend_activity = self.df[self.df['day_of_week'] >= 5]  # Saturday-Sunday
        
        weekend_percentage = (len(weekend_activity) / total_activity) * 100
        
        print(f"\n📅 Weekly Distribution:")
        print(f"   Weekdays: {len(weekday_activity):,} queries ({100-weekend_percentage:.1f}%)")
        print(f"   Weekends: {len(weekend_activity):,} queries ({weekend_percentage:.1f}%)")
        
        if weekend_percentage == 0:
            print(f"   ✅ EXCELLENT: No weekend activity")
            self.quality_score += 15
        elif weekend_percentage < 10:
            print(f"   ✅ GOOD: Minimal weekend activity")
            self.quality_score += 10
        else:
            print(f"   ⚠️ HIGH: Significant weekend activity")
        
        return True
    
    def analyze_user_behavior(self):
        """Analyze user behavior patterns and distribution"""
        print(f"\n👥 USER BEHAVIOR ANALYSIS")
        print("=" * 50)
        
        if 'user' not in self.df.columns:
            print("⚠️ No user column found")
            return True
        
        # User activity distribution
        user_activity = self.df['user'].value_counts()
        
        print(f"📊 User Activity Statistics:")
        print(f"   Total Users: {len(user_activity)}")
        print(f"   Most Active User: {user_activity.iloc[0]} queries")
        print(f"   Least Active User: {user_activity.iloc[-1]} queries")
        print(f"   Average per User: {user_activity.mean():.1f} queries")
        print(f"   Median per User: {user_activity.median():.1f} queries")
        
        # Check for realistic distribution (Zipf's law approximation)
        activity_std = user_activity.std()
        activity_mean = user_activity.mean()
        coefficient_variation = activity_std / activity_mean
        
        print(f"\n📈 Distribution Analysis:")
        print(f"   Coefficient of Variation: {coefficient_variation:.2f}")
        
        if 0.5 <= coefficient_variation <= 2.0:
            print(f"   ✅ REALISTIC: Natural user activity variation (follows Zipf-like distribution)")
            self.quality_score += 15
        elif 0.3 <= coefficient_variation < 0.5:
            print(f"   ✅ ACCEPTABLE: Users have similar activity levels (good for balanced dataset)")
            self.quality_score += 10
        else:
            print(f"   ⚠️ EXTREME: Very uneven user activity distribution")
        
        # Top active users analysis
        print(f"\n🏆 Top 10 Most Active Users:")
        for i, (user, count) in enumerate(user_activity.head(10).items(), 1):
            percentage = (count / len(self.df)) * 100
            print(f"   {i:2d}. {user}: {count:,} queries ({percentage:.1f}%)")
        
        return True
    
    def analyze_query_patterns(self):
        """Analyze SQL query patterns and complexity"""
        print(f"\n🔍 QUERY PATTERN ANALYSIS")
        print("=" * 50)
        
        if 'query' not in self.df.columns:
            print("⚠️ No query column found")
            return True
        
        # Query type distribution
        query_types = {}
        for query in self.df['query'].dropna():
            query_upper = query.upper().strip()
            if query_upper.startswith('SELECT'):
                query_types['SELECT'] = query_types.get('SELECT', 0) + 1
            elif query_upper.startswith('INSERT'):
                query_types['INSERT'] = query_types.get('INSERT', 0) + 1
            elif query_upper.startswith('UPDATE'):
                query_types['UPDATE'] = query_types.get('UPDATE', 0) + 1
            elif query_upper.startswith('DELETE'):
                query_types['DELETE'] = query_types.get('DELETE', 0) + 1
            elif query_upper.startswith('CREATE'):
                query_types['CREATE'] = query_types.get('CREATE', 0) + 1
            elif query_upper.startswith('DROP'):
                query_types['DROP'] = query_types.get('DROP', 0) + 1
            else:
                query_types['OTHER'] = query_types.get('OTHER', 0) + 1
        
        total_queries = sum(query_types.values())
        
        print(f"📊 Query Type Distribution:")
        for qtype, count in sorted(query_types.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_queries) * 100
            print(f"   {qtype}: {count:,} ({percentage:.1f}%)")
        
        # Assess query distribution realism
        select_percentage = (query_types.get('SELECT', 0) / total_queries) * 100
        if select_percentage >= 60:
            print(f"   ✅ REALISTIC: High SELECT query ratio (read-heavy workload)")
            self.quality_score += 10
        elif select_percentage >= 40:
            print(f"   ✅ ACCEPTABLE: Moderate SELECT query ratio")
            self.quality_score += 5
        
        # Query complexity analysis
        query_lengths = self.df['query'].str.len().dropna()
        
        print(f"\n📏 Query Complexity:")
        print(f"   Average Length: {query_lengths.mean():.1f} characters")
        print(f"   Median Length: {query_lengths.median():.1f} characters")
        print(f"   Max Length: {query_lengths.max()} characters")
        print(f"   Min Length: {query_lengths.min()} characters")
        
        if 30 <= query_lengths.mean() <= 300:
            print(f"   ✅ REALISTIC: Appropriate query complexity")
            self.quality_score += 10
        
        return True
    
    def analyze_error_patterns(self):
        """Analyze error patterns and data quality"""
        print(f"\n❌ ERROR PATTERN ANALYSIS")
        print("=" * 50)
        
        if 'has_error' not in self.df.columns:
            print("⚠️ No error column found")
            return True
        
        # Overall error rate
        total_records = len(self.df)
        error_records = self.df['has_error'].sum()
        success_records = total_records - error_records
        
        error_rate = (error_records / total_records) * 100
        success_rate = (success_records / total_records) * 100
        
        print(f"📊 Overall Statistics:")
        print(f"   Total Records: {total_records:,}")
        print(f"   Success Records: {success_records:,} ({success_rate:.1f}%)")
        print(f"   Error Records: {error_records:,} ({error_rate:.1f}%)")
        
        # Quality assessment
        if error_rate <= 1:
            print(f"   ✅ EXCELLENT: Very low error rate")
            self.quality_score += 20
        elif error_rate <= 5:
            print(f"   ✅ GOOD: Acceptable error rate")
            self.quality_score += 15
        elif error_rate <= 15:
            print(f"   ⚠️ MODERATE: Elevated error rate")
            self.quality_score += 5
        else:
            print(f"   ❌ HIGH: Concerning error rate")
            self.issues.append(f"High error rate: {error_rate:.1f}%")
        
        return True
    
    def analyze_anomaly_distribution(self):
        """Analyze anomaly distribution and patterns"""
        print(f"\n🚨 ANOMALY ANALYSIS")
        print("=" * 50)
        
        if 'is_anomaly' not in self.df.columns:
            print("⚠️ No anomaly column found")
            return True
        
        # Anomaly statistics
        anomaly_counts = self.df['is_anomaly'].value_counts()
        total_records = len(self.df)
        
        normal_count = anomaly_counts.get(0, 0)
        anomaly_count = anomaly_counts.get(1, 0)
        
        anomaly_rate = (anomaly_count / total_records) * 100
        
        print(f"📊 Anomaly Distribution:")
        print(f"   Normal Records: {normal_count:,} ({100-anomaly_rate:.1f}%)")
        print(f"   Anomaly Records: {anomaly_count:,} ({anomaly_rate:.1f}%)")
        
        # Assess anomaly rate appropriateness
        if 0 <= anomaly_rate <= 2:
            print(f"   ✅ CLEAN: Very low anomaly rate (clean dataset)")
            self.quality_score += 15
        elif 2 < anomaly_rate <= 15:
            print(f"   ✅ NORMAL: Realistic anomaly rate for business environment")
            self.quality_score += 15
        elif 15 < anomaly_rate <= 40:
            print(f"   ⚠️ HIGH: Elevated anomaly rate (attack scenario)")
            self.quality_score += 10
        else:
            print(f"   ❌ EXTREME: Very high anomaly rate")
            self.issues.append(f"Extremely high anomaly rate: {anomaly_rate:.1f}%")
        
        return True
    
    def generate_comprehensive_visualizations(self):
        """Generate comprehensive visualizations"""
        print(f"\n📈 GENERATING COMPREHENSIVE VISUALIZATIONS")
        print("=" * 50)
        
        try:
            # Create comprehensive analysis figure
            fig, axes = plt.subplots(3, 3, figsize=(20, 15))
            fig.suptitle('Vietnamese Enterprise UBA Dataset - Comprehensive Quality Analysis', 
                        fontsize=16, fontweight='bold')
            
            # 1. Hourly Activity with Business Hours Highlighting
            hourly_activity = self.df['hour'].value_counts().sort_index()
            bars = axes[0, 0].bar(hourly_activity.index, hourly_activity.values, alpha=0.7)
            
            # Color code business hours
            for i, bar in enumerate(bars):
                hour = hourly_activity.index[i]
                if 8 <= hour <= 17 and hour != 12:
                    bar.set_color('#2ecc71')  # Green for business hours
                elif hour == 12:
                    bar.set_color('#f39c12')  # Orange for lunch
                elif hour in [7, 18, 19]:
                    bar.set_color('#3498db')  # Blue for extended hours
                else:
                    bar.set_color('#e74c3c')  # Red for off-hours
            
            axes[0, 0].set_title('Hourly Activity (Business Hours Analysis)')
            axes[0, 0].set_xlabel('Hour of Day')
            axes[0, 0].set_ylabel('Query Count')
            
            # 2. User Activity Distribution (Zipf's Law)
            user_counts = self.df['user'].value_counts().values
            axes[0, 1].loglog(range(1, len(user_counts) + 1), user_counts, 
                             marker='o', linestyle='none', alpha=0.7)
            axes[0, 1].set_title("User Activity Distribution (Zipf's Law)")
            axes[0, 1].set_xlabel('User Rank (Log Scale)')
            axes[0, 1].set_ylabel('Activity Count (Log Scale)')
            axes[0, 1].grid(True, alpha=0.3)
            
            # 3. Database Usage Distribution
            if 'database' in self.df.columns:
                db_usage = self.df['database'].value_counts()
                axes[0, 2].pie(db_usage.values, labels=db_usage.index, autopct='%1.1f%%')
                axes[0, 2].set_title('Database Usage Distribution')
            
            # 4. Query Type Distribution
            query_types = {}
            for query in self.df['query'].dropna():
                query_upper = query.upper().strip()
                if query_upper.startswith('SELECT'):
                    query_types['SELECT'] = query_types.get('SELECT', 0) + 1
                elif query_upper.startswith('INSERT'):
                    query_types['INSERT'] = query_types.get('INSERT', 0) + 1
                elif query_upper.startswith('UPDATE'):
                    query_types['UPDATE'] = query_types.get('UPDATE', 0) + 1
                elif query_upper.startswith('DELETE'):
                    query_types['DELETE'] = query_types.get('DELETE', 0) + 1
                else:
                    query_types['OTHER'] = query_types.get('OTHER', 0) + 1
            
            if query_types and len(query_types) > 0:
                # Ensure we have valid data for pie chart
                values = list(query_types.values())
                labels = list(query_types.keys())
                if len(values) == len(labels) and sum(values) > 0:
                    axes[1, 0].pie(values, labels=labels, autopct='%1.1f%%')
                    axes[1, 0].set_title('Query Type Distribution')
                else:
                    axes[1, 0].text(0.5, 0.5, 'No Query Data', ha='center', va='center')
                    axes[1, 0].set_title('Query Type Distribution')
            else:
                axes[1, 0].text(0.5, 0.5, 'No Query Data', ha='center', va='center')
                axes[1, 0].set_title('Query Type Distribution')
            
            # 5. Inter-Arrival Time Distribution
            self.df['prev_time'] = self.df.groupby('user')['timestamp'].shift(1)
            self.df['iat_seconds'] = (self.df['timestamp'] - self.df['prev_time']).dt.total_seconds()
            iat_clean = self.df['iat_seconds'].dropna()
            
            if len(iat_clean) > 0:
                iat_filtered = iat_clean[iat_clean <= 3600]  # <= 1 hour
                axes[1, 1].hist(iat_filtered, bins=50, alpha=0.7, color='#3498db')
                axes[1, 1].set_title('Inter-Arrival Time Distribution (≤ 1 hour)')
                axes[1, 1].set_xlabel('Seconds')
                axes[1, 1].set_ylabel('Frequency')
            
            # 6. Error Rate Analysis
            if 'has_error' in self.df.columns:
                error_counts = self.df['has_error'].value_counts()
                if len(error_counts) == 2:
                    # Both success and error present
                    axes[1, 2].pie(error_counts.values, labels=['Success', 'Error'], 
                                  autopct='%1.1f%%', colors=['#2ecc71', '#e74c3c'])
                elif len(error_counts) == 1:
                    # Only one type present (likely all success)
                    if error_counts.index[0] == 0:
                        axes[1, 2].pie([100], labels=['Success'], 
                                      autopct='%1.1f%%', colors=['#2ecc71'])
                    else:
                        axes[1, 2].pie([100], labels=['Error'], 
                                      autopct='%1.1f%%', colors=['#e74c3c'])
                axes[1, 2].set_title('Success vs Error Rate')
            
            # 7. Daily Activity Pattern
            daily_activity = self.df.groupby(self.df['timestamp'].dt.date).size()
            axes[2, 0].plot(daily_activity.index, daily_activity.values, marker='o', alpha=0.7)
            axes[2, 0].set_title('Daily Activity Pattern')
            axes[2, 0].set_xlabel('Date')
            axes[2, 0].set_ylabel('Query Count')
            axes[2, 0].tick_params(axis='x', rotation=45)
            
            # 8. Query Length Distribution
            query_lengths = self.df['query'].str.len().dropna()
            axes[2, 1].hist(query_lengths, bins=50, alpha=0.7, color='#9b59b6')
            axes[2, 1].set_title('Query Length Distribution')
            axes[2, 1].set_xlabel('Characters')
            axes[2, 1].set_ylabel('Frequency')
            
            # 9. Anomaly Distribution Over Time
            if 'is_anomaly' in self.df.columns:
                daily_anomalies = self.df.groupby(self.df['timestamp'].dt.date)['is_anomaly'].sum()
                axes[2, 2].plot(daily_anomalies.index, daily_anomalies.values, 
                               marker='o', color='#e67e22', alpha=0.7)
                axes[2, 2].set_title('Daily Anomaly Count')
                axes[2, 2].set_xlabel('Date')
                axes[2, 2].set_ylabel('Anomaly Count')
                axes[2, 2].tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            plt.savefig('comprehensive_dataset_analysis.png', dpi=300, bbox_inches='tight')
            print("✅ Comprehensive analysis chart saved: comprehensive_dataset_analysis.png")
            
            # Generate individual charts for detailed analysis
            self._generate_individual_charts()
            
        except Exception as e:
            print(f"⚠️ Error generating visualizations: {e}")
        
        return True
    
    def _generate_individual_charts(self):
        """Generate individual detailed charts"""
        
        # User activity Zipf distribution
        plt.figure(figsize=(12, 6))
        user_counts = self.df['user'].value_counts().values
        plt.loglog(range(1, len(user_counts) + 1), user_counts, marker='o', linestyle='none', alpha=0.7)
        plt.title("User Activity Distribution (Zipf's Law Check)")
        plt.xlabel("User Rank (Log Scale)")
        plt.ylabel("Activity Count (Log Scale)")
        plt.grid(True, alpha=0.3)
        plt.savefig("user_dist_zipf.png", dpi=300, bbox_inches='tight')
        print("✅ User distribution chart saved: user_dist_zipf.png")
        
        # Enhanced hourly activity with business context
        plt.figure(figsize=(14, 6))
        hourly_counts = self.df['hour'].value_counts().sort_index()
        bars = plt.bar(hourly_counts.index, hourly_counts.values, alpha=0.7)
        
        # Color code by business context
        for i, bar in enumerate(bars):
            hour = hourly_counts.index[i]
            if 8 <= hour <= 17 and hour != 12:
                bar.set_color('#2ecc71')  # Green for business hours
            elif hour == 12:
                bar.set_color('#f39c12')  # Orange for lunch
            elif hour in [7, 18, 19]:
                bar.set_color('#3498db')  # Blue for extended hours
            else:
                bar.set_color('#e74c3c')  # Red for off-hours
        
        plt.title("Hourly Activity Pattern (Vietnamese Business Hours)")
        plt.xlabel("Hour of Day")
        plt.ylabel("Query Count")
        plt.grid(True, alpha=0.3)
        
        # Add legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='#2ecc71', label='Business Hours (8-11, 13-17)'),
            Patch(facecolor='#f39c12', label='Lunch Break (12)'),
            Patch(facecolor='#3498db', label='Extended Hours (7, 18-19)'),
            Patch(facecolor='#e74c3c', label='Off Hours (20-6)')
        ]
        plt.legend(handles=legend_elements, loc='upper right')
        
        plt.savefig("hourly_activity.png", dpi=300, bbox_inches='tight')
        print("✅ Hourly activity chart saved: hourly_activity.png")
        
        # Inter-arrival time analysis
        if 'iat_seconds' in self.df.columns:
            iat_clean = self.df['iat_seconds'].dropna()
            
            if len(iat_clean) > 0:
                plt.figure(figsize=(15, 5))
                
                plt.subplot(1, 3, 1)
                iat_filtered = iat_clean[iat_clean <= 3600]  # <= 1 hour
                plt.hist(iat_filtered, bins=50, alpha=0.7, color='#3498db')
                plt.title('Inter-Arrival Time (≤ 1 hour)')
                plt.xlabel('Seconds')
                plt.ylabel('Frequency')
                
                plt.subplot(1, 3, 2)
                plt.boxplot(iat_filtered)
                plt.title('IAT Box Plot')
                plt.ylabel('Seconds')
                
                plt.subplot(1, 3, 3)
                # Log scale for full distribution
                plt.hist(iat_clean, bins=100, alpha=0.7, color='#e74c3c')
                plt.yscale('log')
                plt.title('Full IAT Distribution (Log Scale)')
                plt.xlabel('Seconds')
                plt.ylabel('Frequency (Log)')
                
                plt.tight_layout()
                plt.savefig("iat_dist.png", dpi=300, bbox_inches='tight')
                print("✅ Inter-arrival time chart saved: iat_dist.png")
    
    def calculate_overall_quality_score(self):
        """Calculate overall dataset quality score and provide recommendations"""
        print(f"\n🎯 OVERALL QUALITY ASSESSMENT")
        print("=" * 50)
        
        # Normalize score to 0-100 scale
        max_possible_score = 150  # Adjusted for comprehensive analysis
        normalized_score = min((self.quality_score / max_possible_score) * 100, 100)
        
        print(f"📊 Quality Metrics:")
        print(f"   Raw Score: {self.quality_score}/{max_possible_score}")
        print(f"   Normalized Score: {normalized_score:.1f}/100")
        
        # Quality grade and status
        if normalized_score >= 90:
            grade = "A+ (EXCELLENT)"
            status = "🏆 PRODUCTION READY"
            color = "🟢"
        elif normalized_score >= 80:
            grade = "A (VERY GOOD)"
            status = "✅ PRODUCTION READY"
            color = "🟢"
        elif normalized_score >= 70:
            grade = "B (GOOD)"
            status = "✅ ACCEPTABLE FOR USE"
            color = "🟡"
        elif normalized_score >= 60:
            grade = "C (FAIR)"
            status = "⚠️ NEEDS IMPROVEMENT"
            color = "🟡"
        else:
            grade = "D (POOR)"
            status = "❌ NOT READY FOR PRODUCTION"
            color = "🔴"
        
        print(f"\n{color} FINAL ASSESSMENT:")
        print(f"   Grade: {grade}")
        print(f"   Status: {status}")
        
        # Issues and recommendations
        if self.issues:
            print(f"\n⚠️ ISSUES IDENTIFIED ({len(self.issues)}):")
            for i, issue in enumerate(self.issues, 1):
                print(f"   {i}. {issue}")
        
        # Generate recommendations
        self._generate_recommendations(normalized_score)
        
        if self.recommendations:
            print(f"\n💡 RECOMMENDATIONS ({len(self.recommendations)}):")
            for i, rec in enumerate(self.recommendations, 1):
                print(f"   {i}. {rec}")
        
        return normalized_score
    
    def _generate_recommendations(self, score):
        """Generate specific recommendations based on analysis"""
        if score < 70:
            self.recommendations.append("Consider regenerating dataset with improved parameters")
        
        if any("off-hours" in issue.lower() for issue in self.issues):
            self.recommendations.append("Review business hours configuration in simulation")
        
        if any("error rate" in issue.lower() for issue in self.issues):
            self.recommendations.append("Investigate and reduce error rate in simulation")
        
        if any("vietnamese" in issue.lower() for issue in self.issues):
            self.recommendations.append("Enhance Vietnamese name generation for authenticity")
        
        if len(self.df) < 1000:
            self.recommendations.append("Consider generating larger dataset for better statistical validity")
        
        # Always provide positive recommendations
        if score >= 80:
            self.recommendations.append("Dataset shows excellent quality - ready for UBA model training")
        elif score >= 70:
            self.recommendations.append("Dataset quality is good - suitable for most UBA applications")

def analyze():
    """Main analysis function for backward compatibility"""
    analyzer = DatasetQualityAnalyzer()
    
    if not analyzer.load_dataset():
        return
    
    # Run comprehensive analysis
    analyzer.analyze_data_integrity()
    analyzer.analyze_vietnamese_authenticity()
    analyzer.analyze_temporal_patterns()
    analyzer.analyze_user_behavior()
    analyzer.analyze_query_patterns()
    analyzer.analyze_error_patterns()
    analyzer.analyze_anomaly_distribution()
    analyzer.generate_comprehensive_visualizations()
    
    # Calculate final score
    final_score = analyzer.calculate_overall_quality_score()
    
    print(f"\n" + "=" * 60)
    print(f"🎉 ANALYSIS COMPLETE - Quality Score: {final_score:.1f}/100")
    print(f"=" * 60)

if __name__ == "__main__":
    analyze()