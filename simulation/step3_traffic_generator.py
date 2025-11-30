# simulation/step3_traffic_generator.py
import mysql.connector
import csv, threading, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CẤU HÌNH ---
SCENARIO_FILE = "simulation/scenario_script_10.csv"
SIM_USER_PASSWORD = "password" 
NUM_THREADS = 20
BATCH_SIZE = 50       

total_sent = 0
stop_event = threading.Event()

def get_real_connection(username):
    target = 'intern_temp' if username in ['unknown_ip', 'script_kiddie', 'apt_group_x', 'unknown', 'dave_insider'] else username
    try:
        # Timeout cực ngắn để fail nhanh nếu lỗi
        return mysql.connector.connect(user=target, password=SIM_USER_PASSWORD, host="localhost", autocommit=True, connection_timeout=2)
    except: return None

def process_batch(batch_data):
    if stop_event.is_set(): return
    
    for row in batch_data:
        if stop_event.is_set(): break
        
        # Query trong CSV đã có sẵn Tag /* SIM_META... */ từ Step 2
        tagged_query = row['query'] 
        user = row['user']
        db = row['database']
        
        conn = get_real_connection(user)
        if conn:
            try:
                cur = conn.cursor()
                try: cur.execute(f"USE {db}")
                except: pass
                
                # Bắn lệnh
                cur.execute(tagged_query)
                if cur.with_rows: cur.fetchall()
                cur.close()
            except: pass
            finally: conn.close()

def run():
    global total_sent
    print(f"🚀 TRAFFIC GENERATOR STARTED ({NUM_THREADS} threads)...")
    try:
        # Tăng field_size_limit để đọc được các query dài có tag
        csv.field_size_limit(2147483647)
        with open(SCENARIO_FILE, 'r', encoding='utf-8') as f: 
            scenarios = list(csv.DictReader(f))
    except Exception as e: print(f"Error: {e}"); return

    batches = [scenarios[i:i + BATCH_SIZE] for i in range(0, len(scenarios), BATCH_SIZE)]
    
    executor = ThreadPoolExecutor(max_workers=NUM_THREADS)
    futures = []
    
    try:
        for b in batches:
            if stop_event.is_set(): break
            futures.append(executor.submit(process_batch, b))
            
        for f in as_completed(futures):
            if stop_event.is_set(): break
            total_sent += BATCH_SIZE
            sys.stdout.write(f"\r⚡ Sent: {total_sent}/{len(scenarios)}")
            sys.stdout.flush()
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping...")
        stop_event.set()
        executor.shutdown(wait=False, cancel_futures=True) 
        sys.exit(0)

    print("\n✅ DONE!")

if __name__ == "__main__":
    run()