
import mysql.connector
import time

def get_schema():
    users_to_try = ['uba_user']
    conn = None
    
    for user in users_to_try:
        try:
            config = {
                'user': user, 
                'password': 'password',
                'host': '127.0.0.1'
            }
            conn = mysql.connector.connect(**config)
            print(f"Connected as {user}")
            break
        except Exception as e:
            pass
            
    if not conn:
        print("Could not connect.")
        return

    cursor = conn.cursor()
    
    databases = ['inventory_db']
    
    for db in databases:
        print(f"\nDATABASE: {db}")
        time.sleep(0.1)
        try:
            cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db}'")
            tables = cursor.fetchall()
            for (table,) in tables:
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db}' AND table_name = '{table}' ORDER BY column_name")
                columns = cursor.fetchall()
                cols = [c[0] for c in columns]
                print(f"  Table: {table}")
                print(f"    Columns: {', '.join(cols)}")
        except Exception as e:
            print(f"  Error reading {db}: {e}")

    conn.close()

if __name__ == "__main__":
    get_schema()
