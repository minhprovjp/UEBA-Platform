#!/usr/bin/env python3
"""
Clean database for fresh simulation start
"""

import mysql.connector
import sys

def clean_database():
    """Clean up database for fresh start"""
    
    print("🧹 CLEANING DATABASE FOR FRESH START")
    print("=" * 50)
    
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root',
            database='uba_db'
        )
        cursor = conn.cursor()
        
        print("✅ Connected to MySQL")
        
        # Disable foreign key checks temporarily
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # Get all tables in uba_db
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        if tables:
            print(f"🗑️ Found {len(tables)} tables to clean")
            
            # Drop all tables
            for (table_name,) in tables:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    print(f"   ✅ Dropped {table_name}")
                except Exception as e:
                    print(f"   ⚠️ Could not drop {table_name}: {e}")
        else:
            print("✅ Database is already clean")
        
        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print("\n🎉 Database cleaned successfully!")
        print("Now you can run the simulation without schema conflicts.")
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = clean_database()
    if success:
        print("\n🚀 Ready to run simulation!")
        print("Run: python main_execution_enhanced.py clean")
    sys.exit(0 if success else 1)