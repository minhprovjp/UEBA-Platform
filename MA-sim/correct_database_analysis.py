#!/usr/bin/env python3
"""
Correct Database Analysis - Proper understanding of Vietnamese enterprise schema
Fixes the misconception that customers table should exist in all databases
"""

import pandas as pd
import mysql.connector
import sys

def analyze_dataset_with_correct_expectations(csv_file):
    """Analyze dataset with correct database schema expectations"""
    print("🔍 CORRECT DATASET ANALYSIS")
    print("=" * 50)
    
    try:
        df = pd.read_csv(csv_file)
        print(f"📊 Dataset loaded: {len(df)} records")
        
        # Count error types
        error_counts = {}
        
        # Table doesn't exist errors
        table_errors = df[df['error_message'].str.contains("doesn't exist", na=False)]
        error_counts['table_not_exist'] = len(table_errors)
        
        # No database selected errors
        no_db_errors = df[df['error_message'].str.contains("No database selected", na=False)]
        error_counts['no_database'] = len(no_db_errors)
        
        # Permission denied errors
        perm_errors = df[df['error_message'].str.contains("SELECT command denied", na=False)]
        error_counts['permission_denied'] = len(perm_errors)
        
        # Column errors (minor schema issues)
        column_errors = df[df['error_message'].str.contains("Unknown column", na=False)]
        error_counts['column_errors'] = len(column_errors)
        
        # Success records
        success_records = df[df['has_error'] == 0]
        error_counts['success'] = len(success_records)
        
        print(f"\n📋 CORRECT ERROR ANALYSIS:")
        total_records = len(df)
        for error_type, count in error_counts.items():
            percentage = (count / total_records) * 100
            status = "✅" if error_type == 'success' else "❌" if count > 0 else "✅"
            print(f"   {status} {error_type}: {count} ({percentage:.1f}%)")
        
        # Calculate overall error rate
        total_errors = sum(count for key, count in error_counts.items() if key != 'success')
        error_rate = (total_errors / total_records) * 100
        
        print(f"\n📊 OVERALL METRICS:")
        print(f"   Success Rate: {(error_counts['success'] / total_records) * 100:.1f}%")
        print(f"   Error Rate: {error_rate:.1f}%")
        
        if error_rate <= 10:
            print(f"   🎯 TARGET ACHIEVED: Error rate {error_rate:.1f}% ≤ 10%")
        else:
            print(f"   ⚠️ TARGET MISSED: Error rate {error_rate:.1f}% > 10%")
        
        return df, error_counts, error_rate
        
    except Exception as e:
        print(f"❌ Error analyzing dataset: {e}")
        return None, {}, 100

def check_correct_database_structure():
    """Check database structure with correct expectations"""
    print(f"\n🗄️ CORRECT DATABASE STRUCTURE ANALYSIS")
    print("=" * 50)
    
    try:
        conn = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="root",
            password="root",
            autocommit=True
        )
        cursor = conn.cursor()
        
        # CORRECT schema expectations for Vietnamese enterprise
        correct_schema = {
            'sales_db': {
                'required_tables': ['customers', 'orders', 'products'],
                'description': 'Sales and customer management'
            },
            'inventory_db': {
                'required_tables': ['inventory_levels', 'warehouse_locations', 'stock_movements'],
                'description': 'Inventory and warehouse management'
            },
            'finance_db': {
                'required_tables': ['invoices', 'accounts', 'expense_reports'],
                'description': 'Financial records and accounting'
            },
            'marketing_db': {
                'required_tables': ['campaigns', 'leads'],
                'description': 'Marketing campaigns and lead management'
            },
            'support_db': {
                'required_tables': ['support_tickets', 'knowledge_base'],
                'description': 'Customer support and help desk'
            },
            'hr_db': {
                'required_tables': ['employees', 'departments', 'salaries'],
                'description': 'Human resources management'
            },
            'admin_db': {
                'required_tables': ['system_logs', 'user_sessions'],
                'description': 'System administration and logging'
            }
        }
        
        print(f"📊 CORRECT SCHEMA ANALYSIS:")
        print(f"   Note: 'customers' table should ONLY exist in sales_db")
        print(f"   Other databases have their own specialized tables")
        
        all_correct = True
        
        for db_name, schema_info in correct_schema.items():
            try:
                cursor.execute(f"USE {db_name}")
                cursor.execute("SHOW TABLES")
                actual_tables = [row[0] for row in cursor.fetchall()]
                
                print(f"\n   📊 {db_name} ({schema_info['description']}):")
                print(f"      Actual tables: {len(actual_tables)}")
                
                # Check required tables
                missing_tables = []
                for required_table in schema_info['required_tables']:
                    if required_table in actual_tables:
                        print(f"      ✅ {required_table}: EXISTS")
                    else:
                        print(f"      ❌ {required_table}: MISSING")
                        missing_tables.append(required_table)
                        all_correct = False
                
                # Check for customers table in wrong databases
                if db_name != 'sales_db' and 'customers' in actual_tables:
                    print(f"      ⚠️ customers: EXISTS (unexpected but not critical)")
                
                # Show additional tables
                extra_tables = set(actual_tables) - set(schema_info['required_tables'])
                if extra_tables:
                    print(f"      ➕ Additional: {', '.join(list(extra_tables)[:3])}{'...' if len(extra_tables) > 3 else ''}")
                
            except Exception as e:
                print(f"   ❌ {db_name}: Error - {e}")
                all_correct = False
        
        cursor.close()
        conn.close()
        
        return all_correct
        
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return False

def analyze_specific_errors(df):
    """Analyze specific error patterns to understand root causes"""
    print(f"\n🔍 SPECIFIC ERROR PATTERN ANALYSIS")
    print("=" * 50)
    
    # Group errors by type
    error_patterns = {}
    
    # Table existence errors
    table_errors = df[df['error_message'].str.contains("doesn't exist", na=False)]
    if len(table_errors) > 0:
        print(f"\n❌ TABLE EXISTENCE ERRORS ({len(table_errors)} records):")
        # Extract table names from error messages
        for _, row in table_errors.head(5).iterrows():
            error_msg = row['error_message']
            print(f"   • {error_msg}")
        
        # Count by database
        table_error_by_db = {}
        for _, row in table_errors.iterrows():
            query = row['query']
            if 'FROM ' in query:
                # Extract database.table from query
                from_part = query.split('FROM ')[1].split(' ')[0]
                if '.' in from_part:
                    db_table = from_part.strip('`')
                    db_name = db_table.split('.')[0]
                    table_name = db_table.split('.')[1]
                    
                    if db_name not in table_error_by_db:
                        table_error_by_db[db_name] = {}
                    if table_name not in table_error_by_db[db_name]:
                        table_error_by_db[db_name][table_name] = 0
                    table_error_by_db[db_name][table_name] += 1
        
        print(f"\n   📊 Table errors by database:")
        for db, tables in table_error_by_db.items():
            print(f"      {db}:")
            for table, count in tables.items():
                print(f"         {table}: {count} errors")
    
    # Column errors
    column_errors = df[df['error_message'].str.contains("Unknown column", na=False)]
    if len(column_errors) > 0:
        print(f"\n⚠️ COLUMN ERRORS ({len(column_errors)} records):")
        for _, row in column_errors.head(3).iterrows():
            error_msg = row['error_message']
            query = row['query'][:80] + "..." if len(row['query']) > 80 else row['query']
            print(f"   • {error_msg}")
            print(f"     Query: {query}")
    
    return error_patterns

def provide_correct_recommendations(error_rate, error_counts):
    """Provide correct recommendations based on proper analysis"""
    print(f"\n🔧 CORRECT RECOMMENDATIONS")
    print("=" * 50)
    
    if error_rate <= 5:
        print(f"🎯 EXCELLENT: Error rate {error_rate:.1f}% is very low")
        print(f"   System is performing excellently")
        
        if error_counts.get('column_errors', 0) > 0:
            print(f"   Minor fixes needed:")
            print(f"   • Fix column name mismatches in SQL library")
            print(f"   • Update table schemas to match expected columns")
    
    elif error_rate <= 10:
        print(f"✅ GOOD: Error rate {error_rate:.1f}% meets target (<10%)")
        print(f"   System is acceptable for production use")
        
        if error_counts.get('table_not_exist', 0) > 0:
            print(f"   Recommended fixes:")
            print(f"   • Create missing tables or update SQL library")
        
        if error_counts.get('column_errors', 0) > 0:
            print(f"   • Fix column name mismatches")
    
    else:
        print(f"❌ NEEDS IMPROVEMENT: Error rate {error_rate:.1f}% exceeds target (>10%)")
        print(f"   System needs fixes before production use")
        
        if error_counts.get('table_not_exist', 0) > 0:
            print(f"   Critical fixes needed:")
            print(f"   • Create missing tables")
            print(f"   • Update SQL library to use only existing tables")
        
        if error_counts.get('permission_denied', 0) > 0:
            print(f"   • Fix user permissions")
        
        if error_counts.get('no_database', 0) > 0:
            print(f"   • Fix database context in queries")

def main():
    """Main function for correct database analysis"""
    print("🔧 CORRECT DATABASE ANALYSIS TOOL")
    print("=" * 60)
    print("Understanding: customers table should ONLY exist in sales_db")
    
    csv_file = "final_clean_dataset_30d.csv"
    
    # Step 1: Analyze dataset with correct expectations
    df, error_counts, error_rate = analyze_dataset_with_correct_expectations(csv_file)
    if df is None:
        print("❌ Failed to analyze dataset")
        return
    
    # Step 2: Check database structure with correct expectations
    structure_correct = check_correct_database_structure()
    
    # Step 3: Analyze specific error patterns
    if len(df) > 0:
        analyze_specific_errors(df)
    
    # Step 4: Provide correct recommendations
    provide_correct_recommendations(error_rate, error_counts)
    
    print(f"\n🎯 FINAL ASSESSMENT")
    print("=" * 60)
    
    if error_rate <= 10:
        print(f"✅ SUCCESS: System meets quality standards")
        print(f"   Error Rate: {error_rate:.1f}% ≤ 10% target")
        print(f"   Success Rate: {(error_counts['success'] / len(df)) * 100:.1f}%")
        print(f"   Status: Ready for production use")
    else:
        print(f"⚠️ NEEDS WORK: System needs improvement")
        print(f"   Error Rate: {error_rate:.1f}% > 10% target")
        print(f"   Success Rate: {(error_counts['success'] / len(df)) * 100:.1f}%")
        print(f"   Status: Requires fixes before production")
    
    print(f"\n📊 Database Schema Status: {'✅ Correct' if structure_correct else '⚠️ Needs fixes'}")

if __name__ == "__main__":
    main()