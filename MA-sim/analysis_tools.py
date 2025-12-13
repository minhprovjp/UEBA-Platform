#!/usr/bin/env python3
"""
Analysis Tools - Consolidated Dataset Analysis and Quality Assessment
Combines dataset analysis, verification, and quality assessment functionality
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
import os
import sys
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Set style for better plots
plt.style.use('default')
sns.set_palette("husl")

class DatasetAnalyzer:
    """Comprehensive dataset analyzer for Vietnamese Enterprise UBA simulation"""
    
    def __init__(self, dataset_file=None):
        self.dataset_file = dataset_file or "final_test_dataset_30d.csv"
        self.df = None
        self.quality_score = 0
        self.issues = []
        self.recommendations = []
        
    def load_dataset(self, dataset_file=None):
        """Load and prepare dataset for analysis"""
        if dataset_file:
            self.dataset_file = dataset_file
            
        print("🔍 COMPREHENSIVE DATASET ANALYSIS")
        print("=" * 60)
        print("Vietnamese Enterprise UBA Simulation Dataset")
        
        if not os.path.exists(self.dataset_file):
            print(f"❌ Dataset file not found: {self.dataset_file}")
            return False
        
        try:
            print(f"📂 Loading dataset: {self.dataset_file}")
            self.df = pd.read_csv(self.dataset_file)
            
            # Convert timestamp with proper handling
            if 'timestamp' in self.df.columns:
                self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
                self.df = self.df.sort_values('timestamp')
            
            print(f"✅ Dataset loaded successfully")
            print(f"   📊 Total records: {len(self.df):,}")
            print(f"   📅 Columns: {len(self.df.columns)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error loading dataset: {e}")
            return False
    
    def basic_statistics(self):
        """Generate basic dataset statistics"""
        if self.df is None:
            print("❌ No dataset loaded")
            return
        
        print("\n📊 BASIC STATISTICS")
        print("=" * 50)
        
        # Dataset overview
        print(f"Dataset shape: {self.df.shape}")
        print(f"Memory usage: {self.df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Column information
        print(f"\nColumn types:")
        for dtype in self.df.dtypes.value_counts().items():
            print(f"  {dtype[0]}: {dtype[1]} columns")
        
        # Missing values
        missing = self.df.isnull().sum()
        if missing.sum() > 0:
            print(f"\nMissing values:")
            for col, count in missing[missing > 0].items():
                percentage = (count / len(self.df)) * 100
                print(f"  {col}: {count} ({percentage:.1f}%)")
        else:
            print("\n✅ No missing values found")
        
        # Unique values
        print(f"\nUnique values per column:")
        for col in self.df.columns:
            unique_count = self.df[col].nunique()
            print(f"  {col}: {unique_count:,}")
    
    def error_analysis(self):
        """Analyze errors in the dataset"""
        if self.df is None:
            print("❌ No dataset loaded")
            return
        
        print("\n🔍 ERROR ANALYSIS")
        print("=" * 50)
        
        # Check for error columns
        error_columns = [col for col in self.df.columns if 'error' in col.lower()]
        
        if not error_columns:
            print("✅ No error columns found - assuming clean dataset")
            return
        
        # Analyze errors
        for error_col in error_columns:
            if error_col in self.df.columns:
                if self.df[error_col].dtype == 'bool' or self.df[error_col].dtype == 'int':
                    # Boolean or integer error flag
                    error_count = self.df[error_col].sum()
                    success_count = len(self.df) - error_count
                    error_rate = (error_count / len(self.df)) * 100
                    
                    print(f"\n{error_col}:")
                    print(f"  ✅ Success: {success_count:,} ({100-error_rate:.1f}%)")
                    print(f"  ❌ Errors: {error_count:,} ({error_rate:.1f}%)")
                    
                else:
                    # Text error messages
                    error_records = self.df[self.df[error_col].notna()]
                    if len(error_records) > 0:
                        print(f"\n{error_col} - Top error types:")
                        error_types = error_records[error_col].value_counts().head(10)
                        for error_msg, count in error_types.items():
                            percentage = (count / len(self.df)) * 100
                            print(f"  {error_msg[:80]}... : {count} ({percentage:.1f}%)")
    
    def user_analysis(self):
        """Analyze user behavior patterns"""
        if self.df is None:
            print("❌ No dataset loaded")
            return
        
        print("\n👥 USER BEHAVIOR ANALYSIS")
        print("=" * 50)
        
        # User activity
        if 'username' in self.df.columns:
            user_activity = self.df['username'].value_counts()
            print(f"Total unique users: {len(user_activity)}")
            print(f"Most active user: {user_activity.index[0]} ({user_activity.iloc[0]} queries)")
            print(f"Least active user: {user_activity.index[-1]} ({user_activity.iloc[-1]} queries)")
            print(f"Average queries per user: {user_activity.mean():.1f}")
            
            # User distribution plot
            plt.figure(figsize=(12, 6))
            plt.subplot(1, 2, 1)
            user_activity.head(20).plot(kind='bar')
            plt.title('Top 20 Most Active Users')
            plt.xticks(rotation=45)
            
            plt.subplot(1, 2, 2)
            plt.hist(user_activity.values, bins=30, alpha=0.7)
            plt.title('User Activity Distribution')
            plt.xlabel('Number of Queries')
            plt.ylabel('Number of Users')
            
            plt.tight_layout()
            plt.savefig('user_activity_analysis.png', dpi=300, bbox_inches='tight')
            plt.close()
            print("📊 User activity plot saved: user_activity_analysis.png")
        
        # Role analysis
        if 'role' in self.df.columns:
            role_activity = self.df['role'].value_counts()
            print(f"\nRole distribution:")
            for role, count in role_activity.items():
                percentage = (count / len(self.df)) * 100
                print(f"  {role}: {count} ({percentage:.1f}%)")
    
    def temporal_analysis(self):
        """Analyze temporal patterns"""
        if self.df is None or 'timestamp' not in self.df.columns:
            print("❌ No timestamp data available")
            return
        
        print("\n⏰ TEMPORAL ANALYSIS")
        print("=" * 50)
        
        # Time range
        start_time = self.df['timestamp'].min()
        end_time = self.df['timestamp'].max()
        duration = end_time - start_time
        
        print(f"Time range: {start_time} to {end_time}")
        print(f"Duration: {duration}")
        print(f"Records per hour: {len(self.df) / (duration.total_seconds() / 3600):.1f}")
        
        # Hourly activity
        self.df['hour'] = self.df['timestamp'].dt.hour
        hourly_activity = self.df['hour'].value_counts().sort_index()
        
        plt.figure(figsize=(12, 6))
        plt.subplot(1, 2, 1)
        hourly_activity.plot(kind='bar')
        plt.title('Activity by Hour of Day')
        plt.xlabel('Hour')
        plt.ylabel('Number of Queries')
        
        # Daily activity
        self.df['date'] = self.df['timestamp'].dt.date
        daily_activity = self.df['date'].value_counts().sort_index()
        
        plt.subplot(1, 2, 2)
        daily_activity.plot()
        plt.title('Activity by Date')
        plt.xlabel('Date')
        plt.ylabel('Number of Queries')
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig('temporal_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("📊 Temporal analysis plot saved: temporal_analysis.png")
    
    def database_analysis(self):
        """Analyze database usage patterns"""
        if self.df is None:
            print("❌ No dataset loaded")
            return
        
        print("\n🗄️ DATABASE USAGE ANALYSIS")
        print("=" * 50)
        
        # Database usage
        if 'database' in self.df.columns:
            db_usage = self.df['database'].value_counts()
            print("Database usage:")
            for db, count in db_usage.items():
                percentage = (count / len(self.df)) * 100
                print(f"  {db}: {count} ({percentage:.1f}%)")
        
        # Query type analysis
        if 'query' in self.df.columns:
            # Extract query types (SELECT, INSERT, UPDATE, DELETE)
            query_types = []
            for query in self.df['query'].dropna():
                query_upper = str(query).upper().strip()
                if query_upper.startswith('SELECT'):
                    query_types.append('SELECT')
                elif query_upper.startswith('INSERT'):
                    query_types.append('INSERT')
                elif query_upper.startswith('UPDATE'):
                    query_types.append('UPDATE')
                elif query_upper.startswith('DELETE'):
                    query_types.append('DELETE')
                else:
                    query_types.append('OTHER')
            
            if query_types:
                query_type_counts = pd.Series(query_types).value_counts()
                print(f"\nQuery type distribution:")
                for qtype, count in query_type_counts.items():
                    percentage = (count / len(query_types)) * 100
                    print(f"  {qtype}: {count} ({percentage:.1f}%)")
    
    def quality_assessment(self):
        """Assess overall dataset quality"""
        if self.df is None:
            print("❌ No dataset loaded")
            return
        
        print("\n🎯 QUALITY ASSESSMENT")
        print("=" * 50)
        
        quality_factors = []
        
        # Completeness (no missing values)
        missing_percentage = (self.df.isnull().sum().sum() / (len(self.df) * len(self.df.columns))) * 100
        completeness_score = max(0, 100 - missing_percentage)
        quality_factors.append(("Completeness", completeness_score))
        
        # Consistency (error rate)
        if 'has_error' in self.df.columns:
            error_rate = (self.df['has_error'].sum() / len(self.df)) * 100
            consistency_score = max(0, 100 - error_rate)
        else:
            consistency_score = 100  # Assume no errors if no error column
        quality_factors.append(("Consistency", consistency_score))
        
        # Validity (reasonable data ranges)
        validity_score = 100  # Default to perfect unless issues found
        
        # Check timestamp validity
        if 'timestamp' in self.df.columns:
            # Handle timezone-aware comparison
            if self.df['timestamp'].dt.tz is not None:
                # Timestamps are timezone-aware, use UTC for comparison
                from datetime import timezone
                current_time = datetime.now(timezone.utc)
            else:
                # Timestamps are timezone-naive
                current_time = datetime.now()
            
            future_timestamps = self.df['timestamp'] > current_time
            if future_timestamps.any():
                validity_score -= 10
                self.issues.append("Future timestamps detected")
        
        quality_factors.append(("Validity", validity_score))
        
        # Uniqueness (duplicate records)
        duplicate_percentage = (self.df.duplicated().sum() / len(self.df)) * 100
        uniqueness_score = max(0, 100 - duplicate_percentage)
        quality_factors.append(("Uniqueness", uniqueness_score))
        
        # Calculate overall quality score
        self.quality_score = sum(score for _, score in quality_factors) / len(quality_factors)
        
        print("Quality factors:")
        for factor, score in quality_factors:
            status = "✅" if score >= 90 else "⚠️" if score >= 70 else "❌"
            print(f"  {status} {factor}: {score:.1f}%")
        
        print(f"\n🎯 Overall Quality Score: {self.quality_score:.1f}%")
        
        # Quality rating
        if self.quality_score >= 90:
            rating = "Excellent"
            emoji = "🟢"
        elif self.quality_score >= 80:
            rating = "Good"
            emoji = "🟡"
        elif self.quality_score >= 70:
            rating = "Fair"
            emoji = "🟠"
        else:
            rating = "Poor"
            emoji = "🔴"
        
        print(f"{emoji} Quality Rating: {rating}")
        
        # Recommendations
        if self.quality_score < 90:
            print(f"\n💡 RECOMMENDATIONS:")
            if missing_percentage > 5:
                print("  • Address missing values in the dataset")
            if 'has_error' in self.df.columns and self.df['has_error'].sum() > 0:
                print("  • Investigate and fix query errors")
            if duplicate_percentage > 1:
                print("  • Remove duplicate records")
            if self.issues:
                for issue in self.issues:
                    print(f"  • Fix: {issue}")
    
    def generate_report(self):
        """Generate comprehensive analysis report"""
        if not self.load_dataset():
            return False
        
        print("📋 GENERATING COMPREHENSIVE ANALYSIS REPORT")
        print("=" * 60)
        
        # Run all analyses
        self.basic_statistics()
        self.error_analysis()
        self.user_analysis()
        self.temporal_analysis()
        self.database_analysis()
        self.quality_assessment()
        
        # Generate summary
        print(f"\n📄 ANALYSIS SUMMARY")
        print("=" * 50)
        print(f"Dataset: {self.dataset_file}")
        print(f"Records: {len(self.df):,}")
        print(f"Quality Score: {self.quality_score:.1f}%")
        print(f"Analysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return True

class DatabaseAnalyzer:
    """Database schema and structure analyzer"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to MySQL"""
        try:
            self.conn = mysql.connector.connect(
                host='localhost',
                user='root',
                password='root'
            )
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            print(f"❌ Database connection error: {e}")
            return False
    
    def analyze_database_structure(self):
        """Analyze complete database structure"""
        print("🗄️ DATABASE STRUCTURE ANALYSIS")
        print("=" * 50)
        
        if not self.connect():
            return False
        
        try:
            # Get all databases
            self.cursor.execute("SHOW DATABASES")
            databases = [db[0] for db in self.cursor.fetchall() 
                        if db[0] not in ['information_schema', 'performance_schema', 'mysql', 'sys']]
            
            print(f"Found {len(databases)} databases:")
            
            total_tables = 0
            for db_name in databases:
                self.cursor.execute(f"USE {db_name}")
                self.cursor.execute("SHOW TABLES")
                tables = [table[0] for table in self.cursor.fetchall()]
                
                print(f"\n📁 {db_name}: {len(tables)} tables")
                for table in tables:
                    # Get table info
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    row_count = self.cursor.fetchone()[0]
                    print(f"   📋 {table}: {row_count:,} rows")
                
                total_tables += len(tables)
            
            print(f"\n📊 Total: {len(databases)} databases, {total_tables} tables")
            return True
            
        except Exception as e:
            print(f"❌ Database analysis error: {e}")
            return False

def main():
    """Main execution function"""
    if len(sys.argv) > 1:
        action = sys.argv[1]
        
        if action == "dataset":
            dataset_file = sys.argv[2] if len(sys.argv) > 2 else None
            analyzer = DatasetAnalyzer(dataset_file)
            analyzer.generate_report()
        elif action == "database":
            db_analyzer = DatabaseAnalyzer()
            db_analyzer.analyze_database_structure()
        elif action == "quality":
            dataset_file = sys.argv[2] if len(sys.argv) > 2 else None
            analyzer = DatasetAnalyzer(dataset_file)
            if analyzer.load_dataset():
                analyzer.quality_assessment()
        else:
            print("Usage: python analysis_tools.py [dataset|database|quality] [dataset_file]")
    else:
        # Default: analyze latest dataset
        analyzer = DatasetAnalyzer()
        analyzer.generate_report()

if __name__ == "__main__":
    main()