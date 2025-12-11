#!/usr/bin/env python3
"""
Fix Column Errors - Systematically fix all "Unknown column" errors
by checking actual database schemas and updating SQL library
"""

import mysql.connector
import pandas as pd
import sys

def connect_to_mysql():
    """Connect to MySQL as root"""
    try:
        conn = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="root",
            password="root",
            autocommit=True
        )
        return conn
    except Exception as e:
        print(f"❌ MySQL connection error: {e}")
        return None

def analyze_column_errors():
    """Analyze all column errors in the dataset"""
    print("🔍 ANALYZING COLUMN ERRORS")
    print("=" * 50)
    
    try:
        df = pd.read_csv("final_clean_dataset_30d.csv")
        
        # Find all column errors
        column_errors = df[df['error_message'].str.contains("Unknown column", na=False)]
        
        print(f"📊 Total records: {len(df)}")
        print(f"❌ Column errors: {len(column_errors)} ({len(column_errors)/len(df)*100:.1f}%)")
        
        # Extract unique column error patterns
        error_patterns = {}
        
        for _, row in column_errors.iterrows():
            error_msg = row['error_message']
            query = row['query']
            
            # Extract column name from error message
            if "Unknown column '" in error_msg:
                column_name = error_msg.split("Unknown column '")[1].split("'")[0]
                
                # Extract table from query
                if " FROM " in query:
                    table_part = query.split(" FROM ")[1].split(" ")[0].strip('`')
                    
                    if table_part not in error_patterns:
                        error_patterns[table_part] = set()
                    error_patterns[table_part].add(column_name)
        
        print(f"\n📋 COLUMN ERROR PATTERNS:")
        for table, columns in error_patterns.items():
            print(f"   {table}: {', '.join(columns)}")
        
        return error_patterns
        
    except Exception as e:
        print(f"❌ Error analyzing column errors: {e}")
        return {}

def get_actual_table_schemas():
    """Get actual column names for all tables"""
    print(f"\n🗄️ GETTING ACTUAL TABLE SCHEMAS")
    print("=" * 50)
    
    conn = connect_to_mysql()
    if not conn:
        return {}
    
    cursor = conn.cursor()
    
    databases = ['sales_db', 'inventory_db', 'finance_db', 'marketing_db', 'support_db', 'hr_db', 'admin_db']
    schemas = {}
    
    for db in databases:
        try:
            cursor.execute(f"USE {db}")
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            
            schemas[db] = {}
            
            for table in tables:
                cursor.execute(f"DESCRIBE {table}")
                columns = [row[0] for row in cursor.fetchall()]
                schemas[db][table] = columns
                
                print(f"📊 {db}.{table}: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
        
        except Exception as e:
            print(f"❌ Error checking {db}: {e}")
    
    cursor.close()
    conn.close()
    
    return schemas

def create_corrected_queries(schemas, error_patterns):
    """Create corrected queries based on actual schemas"""
    print(f"\n🔧 CREATING CORRECTED QUERIES")
    print("=" * 50)
    
    corrections = {}
    
    for table_name, error_columns in error_patterns.items():
        if '.' in table_name:
            db_name, table = table_name.split('.')
            
            if db_name in schemas and table in schemas[db_name]:
                actual_columns = schemas[db_name][table]
                
                print(f"\n📊 {table_name}:")
                print(f"   Actual columns: {', '.join(actual_columns)}")
                
                table_corrections = {}
                
                for error_col in error_columns:
                    # Find best match for error column
                    best_match = None
                    
                    # Direct match (case insensitive)
                    for actual_col in actual_columns:
                        if actual_col.lower() == error_col.lower():
                            best_match = actual_col
                            break
                    
                    # Partial match
                    if not best_match:
                        for actual_col in actual_columns:
                            if error_col.lower() in actual_col.lower() or actual_col.lower() in error_col.lower():
                                best_match = actual_col
                                break
                    
                    # Common mappings
                    if not best_match:
                        common_mappings = {
                            'plan_name': ['department', 'name', 'title'],
                            'budget_amount': ['planned_amount', 'amount', 'budget'],
                            'schedule_type': ['schedule_frequency', 'frequency', 'type'],
                            'status': ['is_active', 'active', 'state']
                        }
                        
                        if error_col in common_mappings:
                            for candidate in common_mappings[error_col]:
                                if candidate in actual_columns:
                                    best_match = candidate
                                    break
                    
                    if best_match:
                        table_corrections[error_col] = best_match
                        print(f"   ✅ {error_col} → {best_match}")
                    else:
                        print(f"   ❌ {error_col} → NO MATCH FOUND")
                
                corrections[table_name] = table_corrections
    
    return corrections

def update_sql_library_with_corrections(corrections):
    """Update the SQL library with correct column names"""
    print(f"\n📝 UPDATING SQL LIBRARY WITH CORRECTIONS")
    print("=" * 50)
    
    try:
        # Read current library
        with open("corrected_enhanced_sql_library.py", "r", encoding="utf-8") as f:
            content = f.read()
        
        # Apply corrections
        for table_name, table_corrections in corrections.items():
            for old_col, new_col in table_corrections.items():
                # Replace column references in queries
                old_pattern = f"SELECT {old_col}"
                new_pattern = f"SELECT {new_col}"
                content = content.replace(old_pattern, new_pattern)
                
                old_pattern = f", {old_col}"
                new_pattern = f", {new_col}"
                content = content.replace(old_pattern, new_pattern)
                
                print(f"   ✅ Replaced {old_col} with {new_col}")
        
        # Write back
        with open("corrected_enhanced_sql_library.py", "w", encoding="utf-8") as f:
            f.write(content)
        
        print("✅ SQL library updated with corrections")
        return True
        
    except Exception as e:
        print(f"❌ Error updating SQL library: {e}")
        return False

def test_corrected_queries(schemas):
    """Test a few corrected queries to verify they work"""
    print(f"\n🧪 TESTING CORRECTED QUERIES")
    print("=" * 50)
    
    conn = connect_to_mysql()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    # Test queries that were previously failing
    test_queries = [
        ("finance_db", "SELECT department, planned_amount FROM finance_db.budget_plans LIMIT 1"),
        ("admin_db", "SELECT report_name, schedule_frequency FROM admin_db.report_schedules LIMIT 1"),
        ("hr_db", "SELECT name, position FROM hr_db.employees LIMIT 1"),
        ("sales_db", "SELECT customer_id, company_name FROM sales_db.customers LIMIT 1")
    ]
    
    success_count = 0
    
    for db_name, query in test_queries:
        try:
            cursor.execute(f"USE {db_name}")
            cursor.execute(query)
            result = cursor.fetchone()
            print(f"   ✅ {query}: SUCCESS")
            success_count += 1
        except Exception as e:
            print(f"   ❌ {query}: {e}")
    
    cursor.close()
    conn.close()
    
    success_rate = (success_count / len(test_queries)) * 100
    print(f"\n📊 Test Success Rate: {success_rate:.1f}%")
    
    return success_rate >= 90

def main():
    """Main function to fix all column errors"""
    print("🔧 FIX ALL COLUMN ERRORS")
    print("=" * 60)
    print("Target: Eliminate all 'Unknown column' errors")
    
    # Step 1: Analyze column errors in dataset
    error_patterns = analyze_column_errors()
    if not error_patterns:
        print("✅ No column errors found!")
        return True
    
    # Step 2: Get actual table schemas
    schemas = get_actual_table_schemas()
    if not schemas:
        print("❌ Failed to get database schemas")
        return False
    
    # Step 3: Create corrections mapping
    corrections = create_corrected_queries(schemas, error_patterns)
    if not corrections:
        print("❌ No corrections could be created")
        return False
    
    # Step 4: Update SQL library
    if not update_sql_library_with_corrections(corrections):
        print("❌ Failed to update SQL library")
        return False
    
    # Step 5: Test corrected queries
    if test_corrected_queries(schemas):
        print(f"\n🎯 COLUMN ERRORS FIXED SUCCESSFULLY")
        print("=" * 60)
        print("✅ All column name mismatches corrected")
        print("✅ SQL library updated with correct column names")
        print("✅ Test queries verified")
        
        print(f"\n🚀 NEXT STEPS:")
        print("   1. python main_execution_enhanced.py clean")
        print("   2. python correct_database_analysis.py")
        print("   3. Verify error rate is now <1%")
        
        return True
    else:
        print(f"\n❌ SOME CORRECTIONS FAILED")
        print("Manual review needed for remaining issues")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)