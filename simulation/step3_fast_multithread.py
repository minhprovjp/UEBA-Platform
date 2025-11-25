# simulation/step3_fast_multithread.py
import mysql.connector
from mysql.connector import errorcode
import csv, time, uuid, threading, sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CẤU HÌNH ---
SCENARIO_FILE = "simulation/scenario_script_10.csv"
FINAL_DATASET = "final_dataset_10_1.csv"
DB_CONFIG = {
    "user": "root",
    "password": "root", # <- Thay password của bạn
    "host": "localhost",
    "database": "mysql"
}

NUM_THREADS = 10      # Số luồng chạy song song
BATCH_SIZE = 50       # Kích thước mỗi batch xử lý

print_lock = threading.Lock()
total_processed = 0
stop_event = threading.Event()

def get_connection():
    try:
        # Timeout kết nối 5s để không chờ lâu nếu DB sập
        return mysql.connector.connect(**DB_CONFIG, connection_timeout=5, autocommit=True)
    except: return None

def scrub_cursor(cursor):
    """Vệ sinh cursor để tránh lỗi Unread result found"""
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
        
        # --- CẤU HÌNH PHÒNG THỦ CHO TOOL ---
        # Giới hạn thời gian chạy query là 2000ms (2s)
        # Để tránh bị treo khi chạy các query tấn công như SLEEP(100) hoặc DoS
        cursor.execute("SET SESSION MAX_EXECUTION_TIME=2000") 
        
        for row in batch_data:
            if stop_event.is_set(): break
            
            # Tạo tag duy nhất để truy vết trong performance_schema
            unique_tag = f"/* TAG:{uuid.uuid4().hex[:8]} */"
            tagged_query = f"{row['query']} {unique_tag}"
            
            # Mặc định các giá trị
            rows_sent = 0
            rows_affected = 0
            rows_examined = 0
            lock_time = 0.0
            error_code = 0
            error_msg = ""
            real_exec = 0.0
            
            # 1. CHẠY QUERY
            try:
                cursor.execute(f"USE {row['database']}")
                scrub_cursor(cursor)
                
                cursor.execute(tagged_query)
                
                if cursor.with_rows:
                    res = cursor.fetchall()
                    rows_sent = len(res)
                    rows_affected = 0
                else:
                    rows_sent = 0
                    rows_affected = cursor.rowcount
                    
            except mysql.connector.Error as err:
                error_code = err.errno
                # Làm sạch thông báo lỗi (bỏ xuống dòng, nháy kép để không vỡ CSV)
                error_msg = str(err.msg).replace('\n', ' ').replace('"', "'")
                if err.errno in [2006, 2013, 2014]: # Mất kết nối thì dừng
                    break 
            except Exception as e:
                error_code = 9999
                error_msg = str(e).replace('\n', ' ')
            finally:
                scrub_cursor(cursor)

            # 2. TRÍCH XUẤT FORENSIC DATA TỪ PERFORMANCE_SCHEMA
            # Đây là bước quan trọng nhất để lấy rows_examined, lock_time
            try:
                metric_sql = f"""
                SELECT 
                    TIMER_WAIT / 1000000000000 as exec_time_sec, -- Chuyển Picosecond sang Second
                    LOCK_TIME / 1000000000000 as lock_time_sec,
                    ROWS_EXAMINED,
                    ROWS_SENT,
                    ROWS_AFFECTED,
                    CREATED_TMP_DISK_TABLES
                FROM performance_schema.events_statements_history_long
                WHERE SQL_TEXT LIKE '%{unique_tag}%'
                ORDER BY EVENT_ID DESC LIMIT 1
                """
                cursor.execute(metric_sql)
                metric = cursor.fetchone()
                
                if metric:
                    real_exec = float(metric[0]) if metric[0] else 0.0
                    lock_time = float(metric[1]) if metric[1] else 0.0
                    rows_examined = int(metric[2]) if metric[2] else 0
                    # Ưu tiên lấy rows_sent từ performance_schema nếu có
                    if metric[3] is not None: rows_sent = int(metric[3])
                    
                    # Nếu query bị lỗi Timeout, performance schema vẫn ghi lại time
                    if error_code == 3024: # Query execution was interrupted
                         error_msg = "Query execution time exceeded limit (Simulated DoS prevention)"
                
            except: 
                pass # Nếu không lấy được metric thì chấp nhận dùng giá trị mặc định
            finally: 
                scrub_cursor(cursor)

            # 3. GHI LOG VÀO LIST
            results.append({
                "timestamp": row['timestamp'],
                "user": row['user'],
                # Giả lập IP dựa trên user để IP cố định cho từng user (tốt cho ML học pattern)
                "client_ip": f"192.168.1.{hash(row['user']) % 250 + 1}",
                "database": row['database'],
                "query": row['query'],
                # Các trường Metrics quan trọng
                "execution_time_sec": real_exec,
                "rows_returned": rows_sent,
                "rows_examined": rows_examined,  # <--- Mới thêm
                "rows_affected": rows_affected,
                "lock_time_sec": lock_time,      # <--- Mới thêm
                # Thông tin lỗi
                "error_code": error_code,
                "error_message": error_msg,
                # Nhãn (Label)
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
    print(f"📖 Đang đọc kịch bản: {SCENARIO_FILE}...")
    try:
        with open(SCENARIO_FILE, 'r', encoding='utf-8') as f:
            scenarios = list(csv.DictReader(f))
    except Exception as e:
        print(f"❌ Lỗi đọc file kịch bản: {e}")
        return

    total_rows = len(scenarios)
    print(f"🚀 BẮT ĐẦU CHẠY SIMULATION (Forensics Mode Enabled)")
    print(f"   - Tổng số dòng: {total_rows}")
    print(f"   - Threads: {NUM_THREADS}")
    print(f"   - Timeout bảo vệ: 2 giây/query")
    
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
                        if total_processed % 200 == 0 or total_processed == total_rows:
                            elapsed = time.time() - start_time
                            speed = total_processed / elapsed if elapsed > 0 else 0
                            # In đè dòng cũ cho đẹp
                            print(f"\r⚡ Progress: {total_processed}/{total_rows} | Speed: {speed:.1f} q/s | Errors Detected: {len([x for x in final_data if x['error_code'] != 0])}", end="")
            except: pass
    except KeyboardInterrupt:
        print("\n🛑 Đang dừng khẩn cấp...")
        stop_event.set()
        executor.shutdown(wait=False)
        
    if final_data:
        print(f"\n\n💾 Đang lưu file '{FINAL_DATASET}'...")
        df = pd.DataFrame(final_data)
        # Sắp xếp lại theo thời gian
        df.sort_values(by='timestamp', inplace=True)
        
        # Lưu file
        df.to_csv(FINAL_DATASET, index=False)
        print(f"✅ HOÀN TẤT! File kết quả: {FINAL_DATASET}")
        print(f"   -> Số cột: {len(df.columns)}")
        print(f"   -> Bao gồm: execution_time_sec, lock_time_sec, rows_examined...")
    else:
        print("\n⚠️ Không có dữ liệu được xử lý.")

if __name__ == "__main__":
    # Bật chế độ theo dõi của MySQL trước khi chạy
    try:
        print("🔧 Đang cấu hình MySQL Performance Schema...")
        c = mysql.connector.connect(**DB_CONFIG)
        cur = c.cursor()
        # Bật consumer lịch sử câu lệnh
        cur.execute("UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME LIKE 'events_statements_history_long'")
        # Bật instrument đo thời gian
        cur.execute("UPDATE performance_schema.setup_instruments SET ENABLED='YES', TIMED='YES' WHERE NAME LIKE 'statement/%'")
        c.commit()
        c.close()
    except Exception as e:
        print(f"⚠️ Cảnh báo: Không thể cấu hình Performance Schema. Số liệu thời gian có thể không chính xác. Lỗi: {e}")
        
    run_simulation()