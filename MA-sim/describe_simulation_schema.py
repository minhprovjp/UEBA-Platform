import mysql.connector
import json

# DB Config (matching established simulation config)
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root"
}

TARGET_DBS = [
    'sales_db', 'hr_db', 'inventory_db', 'finance_db', 
    'marketing_db', 'support_db', 'admin_db'
]

def get_schema_description():
    print("🔌 Connecting to MySQL...")
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        output_text = "DATABASE SCHEMA:\n"
        
        for db in TARGET_DBS:
            print(f"📖 Inspecting {db}...")
            output_text += f"\nDATABASE: {db}\n"
            
            # Get tables
            cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db}'")
            tables = [row[0] for row in cursor.fetchall()]
            
            if not tables:
                output_text += "  (No tables found)\n"
                continue
                
            for table in tables:
                # Get columns
                cursor.execute(f"""
                    SELECT column_name, data_type, column_type 
                    FROM information_schema.columns 
                    WHERE table_schema = '{db}' AND table_name = '{table}' 
                    ORDER BY ordinal_position
                """)
                columns = cursor.fetchall()
                
                col_strs = []
                for col_name, data_type, col_type in columns:
                    # Simplify types for LLM
                    simple_type = data_type
                    if 'enum' in col_type:
                        simple_type = col_type # keep enum values
                    elif 'int' in col_type:
                        simple_type = 'INT'
                    elif 'decimal' in col_type:
                        simple_type = 'DECIMAL'
                    elif 'varchar' in col_type:
                        simple_type = 'VARCHAR'
                    elif 'text' in col_type:
                        simple_type = 'TEXT'
                    elif 'date' in col_type:
                        simple_type = 'DATE'
                    elif 'timestamp' in col_type:
                        simple_type = 'TIMESTAMP'
                        
                    col_strs.append(f"{col_name} ({simple_type})")
                
                output_text += f"  - TABLE {table}: {', '.join(col_strs)}\n"
        
        conn.close()
        return output_text
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    schema_text = get_schema_description()
    if schema_text:
        print("\n✅ Schema Inspection Complete!")
        # Save to file
        with open("schema_description.txt", "w", encoding="utf-8") as f:
            f.write(schema_text)
        print("💾 Saved to schema_description.txt")
        print("\n--- PREVIEW ---\n")
        print(schema_text[:500] + "...\n(truncated)")
    else:
        print("❌ Failed to get description")
