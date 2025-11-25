# simulation\step3_fast_multithread.py
import mysql.connector
from mysql.connector import errorcode
import csv, time, uuid, threading, sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CẤU HÌNH ---
SCENARIO_FILE = "simulation/scenario_script_10.csv"
FINAL_DATASET = "final_dataset_10.csv"
DB_CONFIG = {"user": "root",
             "password": "root", # <- thay pass
             "host": "localhost",
             "database": "mysql"}

NUM_THREADS = 10      # số cổng để nhập  
BATCH_SIZE = 50       # 50 query/s

print_lock = threading.Lock()
total_processed = 0
stop_event = threading.Event()

def get_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG, connection_timeout=10, autocommit=True)
    except: return None

def scrub_cursor(cursor):
    try:
        cursor.fetchall()
    except: pass
    try:
        while cursor.nextset():
            try: cursor.fetchall()
            except: pass
    except: pass

def process_batch(batch_data):
    if stop_event.is_set(): return []
    results = []
    conn = get_connection()
    if not conn: return []

    try:
        cursor = conn.cursor(buffered=True)
        
        for row in batch_data:
            if stop_event.is_set(): break
            
            unique_tag = f"/* TAG:{uuid.uuid4().hex[:8]} */"
            tagged_query = f"{row['query']} {unique_tag}"
            
            exec_start = time.time()
            rows_sent = 0
            rows_affected = 0 # Mặc định
            error_code = 0
            error_msg = ""
            
            # 1. CHẠY QUERY
            try:
                cursor.execute(f"USE {row['database']}")
                scrub_cursor(cursor)
                
                cursor.execute(tagged_query)
                
                # Lấy số dòng trả về hoặc số dòng bị ảnh hưởng NGAY LẬP TỨC
                if cursor.with_rows:
                    res = cursor.fetchall()
                    rows_sent = len(res)
                    rows_affected = 0 # Select thì affected = 0
                else:
                    rows_sent = 0
                    rows_affected = cursor.rowcount # Update/Insert/Delete lấy ở đây
                    
            except mysql.connector.Error as err:
                error_code = err.errno
                error_msg = err.msg
                if err.errno in [2006, 2013, 2014]: break 
            except Exception as e:
                error_code = 9999
                error_msg = str(e)
            finally:
                scrub_cursor(cursor)

            # 2. LẤY EXECUTION TIME (Chỉ lấy time, không lấy rows nữa)
            real_exec = 0.001
            try:
                metric_sql = f"""
                SELECT TRUNCATE(TIMER_WAIT/1000000000, 6) as exec_time
                FROM performance_schema.events_statements_history_long
                WHERE SQL_TEXT LIKE '%{unique_tag}%'
                ORDER BY EVENT_ID DESC LIMIT 1
                """
                cursor.execute(metric_sql)
                metric = cursor.fetchone()
                if metric: real_exec = metric[0]
                else: real_exec = time.time() - exec_start
            except: pass
            finally: scrub_cursor(cursor)

            # 3. GHI LOG
            results.append({
                "timestamp": row['timestamp'],
                "user": row['user'],
                "client_ip": "192.168.1." + str(hash(row['user']) % 250),
                "database": row['database'],
                "query": row['query'],
                "execution_time_sec": float(real_exec),
                "rows_returned": rows_sent,
                "rows_affected": rows_affected, # Dùng số liệu từ Python Driver (Chính xác hơn)
                "error_code": error_code,
                "error_message": str(error_msg),
                "is_anomaly": row['is_anomaly'],
                "behavior_type": row.get('behavior_type', 'NORMAL'),
                "source_dbms": "MySQL"
            })
            
    except Exception: pass
    finally:
        try:
            if 'cursor' in locals(): cursor.close()
            if conn.is_connected(): conn.close()
        except: pass
        
    return results

def run_simulation():
    global total_processed
    print("📖 Đang đọc kịch bản...")
    try:
        with open(SCENARIO_FILE, 'r', encoding='utf-8') as f:
            scenarios = list(csv.DictReader(f))
    except:
        print("❌ Lỗi đọc file kịch bản!")
        return

    total_rows = len(scenarios)
    print(f"🚀 BẮT ĐẦU CHẠY (V5.0 - Office Hours Logic & RowCount Fix)")
    print(f"   - Số dòng: {total_rows}")
    
    batches = [scenarios[i:i + BATCH_SIZE] for i in range(0, total_rows, BATCH_SIZE)]
    final_data = []
    start_time = time.time()
    
    executor = ThreadPoolExecutor(max_workers=NUM_THREADS)
    try:
        futures = [executor.submit(process_batch, batch) for batch in batches]
        for future in as_completed(futures):
            if stop_event.is_set(): break
            try:
                res = future.result()
                if res:
                    final_data.extend(res)
                    with print_lock:
                        total_processed += len(res)
                        if total_processed % 500 == 0 or total_processed == total_rows:
                            elapsed = time.time() - start_time
                            speed = total_processed / elapsed if elapsed > 0 else 0
                            print(f"\r⚡ Progress: {total_processed}/{total_rows} | Speed: {speed:.1f} q/s", end="")
            except: pass
    except KeyboardInterrupt:
        print("\n🛑 Đang dừng...")
        stop_event.set()
        executor.shutdown(wait=False)
        
    if final_data:
        print(f"\n💾 Đang lưu file '{FINAL_DATASET}'...")
        df = pd.DataFrame(final_data)
        df.sort_values(by='timestamp', inplace=True)
        df.to_csv(FINAL_DATASET, index=False)
        print(f"✅ HOÀN TẤT! Kiểm tra file: {FINAL_DATASET}")
    else:
        print("\n⚠️ Không có dữ liệu.")

if __name__ == "__main__":
    try:
        c = mysql.connector.connect(**DB_CONFIG)
        cur = c.cursor()
        cur.execute("UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME LIKE 'events_statements_history_long'")
        cur.execute("UPDATE performance_schema.setup_instruments SET ENABLED='YES', TIMED='YES' WHERE NAME LIKE 'statement/%'")
        c.commit()
        c.close()
    except: pass
    run_simulation()