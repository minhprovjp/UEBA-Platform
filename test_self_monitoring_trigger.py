
import mysql.connector
import time
import json
import os
import sys

# Add current directory to path
sys.path.append(os.getcwd())

def load_db_config():
    config_path = "self_monitoring_config.json"
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
            return config.get('database', {})
    else:
        # Fallback to defaults
        return {
            "host": "localhost",
            "user": "uba_user",
            "password": "", # Assuming no password for local dev or env var
            "database": "uba_db"
        }

def simulate_reconnaissance():
    print("[*] Simulating Reconnaissance Attack...")
    config = load_db_config()
    
    # Try different connection methods
    try:
        conn = mysql.connector.connect(
            host=config.get('host', 'localhost'),
            user=config.get('user', 'root'),
            password=config.get('password', ''),
            database=config.get('database', 'uba_db')
        )
        cursor = conn.cursor()
        
        # 1. Suspicious Table Enumeration
        queries = [
            "SHOW TABLES FROM uba_db",
            "SELECT * FROM information_schema.tables WHERE table_schema = 'uba_db'",
            "DESCRIBE uba_user",
            "SELECT * FROM uba_user LIMIT 1"
        ]
        
        for q in queries:
            print(f"Executing: {q}")
            try:
                cursor.execute(q)
                cursor.fetchall()
            except Exception as e:
                print(f"Query failed (expected): {e}")
            time.sleep(1)
            
        conn.close()
        print("[+] Reconnaissance simulation complete.")
        
    except Exception as e:
        print(f"[-] Connection failed: {e}")

if __name__ == "__main__":
    print("=== Self-Monitoring Test Trigger ===")
    simulate_reconnaissance()
    print("Wait 30-60 seconds for the self-monitoring service to pick this up.")
