#!/usr/bin/env python3
"""
Quick fix for schema issues on Ubuntu server
"""

import mysql.connector
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def fix_schema_issues():
    """Fix common schema issues"""
    
    print("🔧 FIXING SCHEMA ISSUES")
    print("=" * 50)
    
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root',  # Adjust if your password is different
            database='uba_db'
        )
        cursor = conn.cursor()
        
        print("✅ Connected to MySQL")
        
        # Check if tables exist and fix missing columns
        tables_to_check = [
            ('customers', 'customer_code', 'VARCHAR(50)'),
            ('orders', 'order_code', 'VARCHAR(50)'),
            ('products', 'product_code', 'VARCHAR(50)'),
            ('employees', 'employee_code', 'VARCHAR(50)')
        ]
        
        for table_name, column_name, column_type in tables_to_check:
            try:
                # Check if table exists
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if cursor.fetchone():
                    # Check if column exists
                    cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE '{column_name}'")
                    if not cursor.fetchone():
                        # Add missing column
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
                        print(f"✅ Added {column_name} to {table_name}")
                    else:
                        print(f"✅ {column_name} already exists in {table_name}")
                else:
                    print(f"⚠️ Table {table_name} doesn't exist, will be created by simulation")
                    
            except mysql.connector.Error as e:
                print(f"⚠️ Issue with {table_name}.{column_name}: {e}")
        
        # Ensure Performance Schema is enabled
        cursor.execute("SET GLOBAL performance_schema = ON")
        print("✅ Performance Schema enabled")
        
        # Check Performance Schema tables
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'performance_schema'")
        ps_tables = cursor.fetchone()[0]
        print(f"✅ Performance Schema has {ps_tables} tables")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("\n🎉 Schema issues fixed successfully!")
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = fix_schema_issues()
    sys.exit(0 if success else 1)