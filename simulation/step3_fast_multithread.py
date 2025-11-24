import mysql.connector
import csv
import time
import uuid
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sys

# --- CẤU HÌNH TỐC ĐỘ CAO ---
SCENARIO_FILE = "simulation/scenario_script_1day.csv"
FINAL_DATASET = "final_dataset_1day.csv"
DB_CONFIG = {"user": "root",
             "password": "root",
             "host": "localhost",
             "database": "mysql"}

# Tinh chỉnh hiệu năng
NUM_THREADS = 20      # 20 luồng
BATCH_SIZE = 100      # 100 query/lô

# --- BIẾN TOÀN CỤC & CỜ DỪNG ---
print_lock = threading.Lock()
total_processed = 0
stop_event = threading.Event() # <--- CÁI PHANH KHẨN CẤP

def get_connection():
    """Tạo kết nối riêng cho mỗi luồng (Timeout ngắn để dễ thoát)"""
    return mysql.connector.connect(**DB_CONFIG, connection_timeout=5)

def process_batch(batch_data, thread_id):
    """Hàm xử lý một lô kịch bản"""
    results = []
    
    # Nếu đã có lệnh dừng thì không mở kết nối mới nữa
    if stop_event.is_set(): return []

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        for row in batch_data:
            # 1. KIỂM TRA CỜ DỪNG LIÊN TỤC
            if stop_event.is_set(): 
                break # Thoát khỏi vòng lặp batch ngay lập tức
            
            # Gắn thẻ (Tagging)
            unique_tag = f"/* TAG:{uuid.uuid4().hex[:8]} */"
            tagged_query = f"{row['query']} {unique_tag}"
            
            exec_start = time.time()
            rows_sent = 0
            error_code = 0
            error_msg = ""
            
            try:
                cursor.execute(f"USE {row['database']}")
                cursor.execute(tagged_query)
                res = cursor.fetchall()
                rows_sent = len(res)
            except mysql.connector.Error as err:
                error_code = err.errno
                error_msg = err.msg
            except: pass
            
            # Lấy Metric thật
            conn.commit() 
            metric_sql = f"""
            SELECT TRUNCATE(TIMER_WAIT/1000000000, 6) as exec_time, ROWS_AFFECTED
            FROM performance_schema.events_statements_history_long
            WHERE SQL_TEXT LIKE '%{unique_tag}%'
            ORDER BY EVENT_ID DESC LIMIT 1
            """
            cursor.execute(metric_sql)
            metric = cursor.fetchone()
            
            real_exec = metric[0] if metric else (time.time() - exec_start)
            real_aff = metric[1] if metric else 0
            
            # Đóng gói
            results.append({
                "timestamp": row['timestamp'],
                "user": row['user'],
                "client_ip": "192.168.1." + str(hash(row['user']) % 250),
                "database": row['database'],
                "query": row['query'],
                "execution_time_sec": float(real_exec),
                "rows_returned": rows_sent,
                "rows_affected": real_aff,
                "error_code": error_code,
                "error_message": str(error_msg),
                "is_anomaly": row['is_anomaly'],
                "source_dbms": "MySQL"
            })
            
    except Exception as e:
        # Chỉ in lỗi nếu không phải đang dừng (để đỡ rác màn hình)
        if not stop_event.is_set():
            with print_lock:
                print(f"\n⚠️ Thread {thread_id} Error: {e}")
    finally:
        if conn: 
            try: conn.close()
            except: pass
        
    return results

def run_fast_simulation():
    global total_processed
    
    print("📖 Đang đọc file kịch bản vào bộ nhớ...")
    try:
        with open(SCENARIO_FILE, 'r', encoding='utf-8') as f:
            scenarios = list(csv.DictReader(f))
    except:
        print("❌ Không tìm thấy file kịch bản CSV!")
        return

    total_rows = len(scenarios)
    print(f"🚀 BẮT ĐẦU TĂNG TỐC (SAFE STOP MODE):")
    print(f"   - Tổng số dòng: {total_rows}")
    print(f"   - Số luồng: {NUM_THREADS}")
    print("👉 Nhấn CTRL+C bất cứ lúc nào để DỪNG và LƯU kết quả.")
    print("------------------------------------------------")
    
    batches = [scenarios[i:i + BATCH_SIZE] for i in range(0, total_rows, BATCH_SIZE)]
    final_data = []
    start_time = time.time()
    
    # Executor quản lý luồng
    executor = ThreadPoolExecutor(max_workers=NUM_THREADS)
    
    try:
        # Gửi việc cho thợ
        future_to_batch = {executor.submit(process_batch, batch, i): i for i, batch in enumerate(batches)}
        
        for future in as_completed(future_to_batch):
            # Nếu bấm dừng, hủy nhận kết quả tiếp theo để thoát nhanh
            if stop_event.is_set(): break
            
            batch_result = future.result()
            if batch_result:
                final_data.extend(batch_result)
                
                with print_lock:
                    total_processed += len(batch_result)
                    if total_processed % 500 == 0:
                        elapsed = time.time() - start_time
                        speed = total_processed / elapsed if elapsed > 0 else 0
                        print(f"\r⚡ Progress: {total_processed}/{total_rows} | Speed: {speed:.1f} q/s | Time: {elapsed:.1f}s", end="")

    except KeyboardInterrupt:
        print("\n\n🛑 ĐÃ NHẬN LỆNH DỪNG (CTRL+C)!")
        print("⏳ Đang đợi các luồng hoàn tất nốt công việc dở dang...")
        stop_event.set() # Bật cờ dừng
        executor.shutdown(wait=False) # Không nhận thêm việc mới
        
    # --- PHẦN LƯU FILE (Luôn chạy dù xong hay bị dừng giữa chừng) ---
    if final_data:
        print(f"\n\n💾 Đang lưu {len(final_data)} dòng dữ liệu vào '{FINAL_DATASET}'...")
        df = pd.DataFrame(final_data)
        df.sort_values(by='timestamp', inplace=True)
        df.to_csv(FINAL_DATASET, index=False)
        print(f"✅ ĐÃ LƯU THÀNH CÔNG! Bạn có thể dùng file này ngay.")
    else:
        print("\n⚠️ Chưa có dữ liệu nào được thu thập.")

    print(f"👋 Kết thúc chương trình.")

if __name__ == "__main__":
    # Đảm bảo Performance Schema bật
    try:
        c = mysql.connector.connect(**DB_CONFIG)
        cur = c.cursor()
        cur.execute("UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME LIKE 'events_statements_history_long'")
        cur.execute("UPDATE performance_schema.setup_instruments SET ENABLED='YES', TIMED='YES' WHERE NAME LIKE 'statement/%'")
        c.commit()
        c.close()
    except: pass
    
    run_fast_simulation()