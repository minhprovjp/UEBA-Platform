# simulation/step3_fast_multithread.py
import mysql.connector
from mysql.connector import errorcode
import csv, time, uuid, threading, sys, math, re, random
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CẤU HÌNH ---
SCENARIO_FILE = "simulation/scenario_script_30d.csv"
FINAL_DATASET = "final_dataset_30d.csv"
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

class NetworkForensicSimulator:
    """
    Trình mô phỏng mạng "Stateful" & "Context-Aware".
    Đảm bảo tính nhất quán của Port/IP dựa trên hành vi và loại User.
    Hỗ trợ đa luồng (Thread-Safe).
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.user_pools = {}     # Lưu pool port cho Web/App users
        self.persistent_sessions = {} # Lưu port cố định cho Admin/Dev/Insider
        self.scan_state = 10000  # Lưu trạng thái port scanning

    def get_socket_info(self, user, behavior_type):
        """
        Trả về (client_ip, client_port) dựa trên ngữ cảnh.
        """
        with self.lock:
            # 1. XỬ LÝ IP (Logic cũ của bạn nhưng đưa vào đây cho gọn)
            # Mặc định IP theo User Hash
            client_ip = f"192.168.1.{hash(user) % 250 + 1}"
            
            # Nếu là unknown_ip hoặc script_kiddie thì IP phải lạ hoặc random
            if user in ['unknown_ip', 'script_kiddie', 'apt_group_x']:
                client_ip = f"10.0.{random.randint(1,254)}.{random.randint(1,254)}"

            # 2. XỬ LÝ PORT THEO HÀNH VI (Context-Aware)
            
            # NHÓM A: SCANNING (Quét cổng)
            # Port tăng dần đều để AI nhận diện pattern
            if behavior_type == 'SCANNING': 
                self.scan_state += 1
                if self.scan_state > 65000: self.scan_state = 10000
                return client_ip, self.scan_state

            # NHÓM B: NOISY ATTACKS (DoS, Brute Force)
            # Random hoàn toàn để giả lập connection storm
            if behavior_type in ['DOS', 'BRUTE_FORCE']:
                return client_ip, random.randint(10000, 65000)

            # NHÓM C: PERSISTENT USERS (Admin, Dev, Insider Threat, Backdoor)
            # Giả lập công cụ quản trị (treo kết nối lâu dài)
            # Insider Threat phải lẩn trốn trong nhóm này -> Dùng port cố định
            if any(role in user for role in ['admin', 'dev', 'dave', 'insider']) or behavior_type == 'PERSISTENCE_BACKDOOR':
                if user not in self.persistent_sessions:
                    # Gán 1 port cố định cho phiên làm việc này
                    self.persistent_sessions[user] = random.randint(10000, 60000)
                return client_ip, self.persistent_sessions[user]

            # NHÓM D: APPLICATION POOLING (Sales, HR, SQL Injection)
            # Đây là nhóm đông nhất. SQL Injection phải nằm ở đây thì mới giống thật!
            # (Hacker bắn SQLi qua Web Browser -> Server Web dùng Connection Pool nối vào DB)
            if user not in self.user_pools:
                # Tạo pool 5-8 ports cho user này
                base = random.randint(10000, 60000)
                self.user_pools[user] = [base + i for i in range(random.randint(5, 8))]
            
            # Giả lập hành vi lấy connection từ pool
            # 95% dùng lại port cũ, 5% mở port mới (recycle)
            pool = self.user_pools[user]
            if random.random() < 0.95:
                return client_ip, random.choice(pool)
            else:
                new_port = random.randint(10000, 65000)
                pool.pop(0) # Bỏ port cũ nhất
                pool.append(new_port)
                return client_ip, new_port
            
# Khởi tạo Simulator toàn cục
net_sim = NetworkForensicSimulator()


# --- CÁC HÀM TÍNH TOÁN FEATURE (PYTHON SIDE) ---
def calculate_entropy(text):
    """Tính độ hỗn loạn (Shannon Entropy) của chuỗi query"""
    if not text: return 0
    prob = [float(text.count(c)) / len(text) for c in dict.fromkeys(list(text))]
    entropy = - sum([p * math.log(p) / math.log(2.0) for p in prob])
    return round(entropy, 4)

def analyze_query_structure(query):
    """Phân tích ngữ cảnh query"""
    q_lower = query.lower()
    
    # 1. Check System Tables
    is_system = 1 if any(x in q_lower for x in ['information_schema', 'mysql.', 'performance_schema', 'sys.']) else 0
    
    # 2. Count Tables (Ước lượng sơ bộ qua từ khóa JOIN/FROM)
    # Đếm số lần xuất hiện của FROM và JOIN
    num_tables = len(re.findall(r'\bfrom\b|\bjoin\b', q_lower))
    if num_tables == 0 and ('select' in q_lower or 'update' in q_lower): num_tables = 1
    
    return is_system, num_tables

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
            
            # --- 1. CHUẨN BỊ DỮ LIỆU ---
            # FIX: Đưa tag lên ĐẦU query để tránh bị cắt khi lưu vào Performance Schema
            unique_tag = f"/* TAG:{uuid.uuid4().hex[:8]} */"
            tagged_query = f"{unique_tag} {row['query']}"
            
            entropy = calculate_entropy(row['query'])
            query_len = len(row['query'])
            is_sys, num_tbls = analyze_query_structure(row['query'])
            
            # --- GỌI SIMULATOR ĐỂ LẤY IP/PORT ---
            behavior_type = row.get('behavior_type', 'NORMAL')
            client_ip, client_port = net_sim.get_socket_info(row['user'], behavior_type)
            
            # Khởi tạo giá trị mặc định (đã xóa lỗi dấu phẩy tuple)
            rows_sent = 0
            rows_affected = 0
            exec_time_ms = 0.0 
            rows_examined = 0
            lock_time = 0.0
            tmp_disk = 0
            tmp_mem = 0
            digest = ""
            digest_text = ""
            errors = 0
            error_code = 0
            error_msg = ""
            real_exec = 0.0
            warnings = 0
            no_index = 0
            
            # --- 2. THỰC THI QUERY ---
            try:
                scrub_cursor(cursor)
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
                error_msg = str(err.msg).replace('\n', ' ').replace('"', "'")
                if err.errno in [2006, 2013, 2014, 2055]: # Mất kết nối thì break batch
                    break 
            except Exception as e:
                error_code = 9999
                error_msg = str(e).replace('\n', ' ')
            finally:
                scrub_cursor(cursor)

            # --- 3. TRÍCH XUẤT METRICS (FORENSICS) ---
            try:
                metric_sql = f"""
                SELECT 
                    TIMER_WAIT / 1000000000000,   -- [0] exec_time (seconds)
                    LOCK_TIME / 1000000000000,    -- [1] lock_time
                    ROWS_EXAMINED,                -- [2]
                    ROWS_SENT,                    -- [3]
                    ROWS_AFFECTED,                -- [4]
                    CREATED_TMP_DISK_TABLES,      -- [5]
                    CREATED_TMP_TABLES,           -- [6]
                    DIGEST,                       -- [7]
                    DIGEST_TEXT,                  -- [8]
                    ERRORS,                       -- [9]
                    WARNINGS,                     -- [10]
                    NO_INDEX_USED                 -- [11]
                FROM performance_schema.events_statements_history_long
                WHERE SQL_TEXT LIKE '{unique_tag}%'
                ORDER BY EVENT_ID DESC LIMIT 1
                """
                cursor.execute(metric_sql)
                metric = cursor.fetchone()
                
                if metric:
                    real_exec = float(metric[0]) if metric[0] else 0.0
                    lock_time = float(metric[1]) if metric[1] else 0.0
                    
                    # FIX LOGIC: Rows Examined ít nhất phải bằng Rows Sent (tránh logic 0 examined)
                    raw_examined = int(metric[2]) if metric[2] else 0
                    raw_sent = int(metric[3]) if metric[3] is not None else 0
                    rows_examined = max(raw_examined, raw_sent)
                    
                    if metric[3] is not None: rows_sent = int(metric[3])
                    if metric[4] is not None: rows_affected = int(metric[4])
                    tmp_disk = int(metric[5]) if metric[5] else 0
                    tmp_mem = int(metric[6]) if metric[6] else 0
                    digest = str(metric[7]) if metric[7] else ""
                    digest_text = str(metric[8]) if metric[8] else ""
                    errors = int(metric[9]) if metric[9] is not None else (1 if error_code else 0)
                    warnings = int(metric[10]) if metric[10] is not None else 0
                    no_index = int(metric[11]) if metric[11] is not None else 0

                    # Tính lại exec_time_ms chính xác
                    exec_time_ms = real_exec * 1000

                    if error_code == 3024: error_msg = "Query execution time exceeded limit"
                
            except Exception: pass
            finally: scrub_cursor(cursor)

            # --- 4. GHI KẾT QUẢ ---
            results.append({
                # --- Nhóm 1: Python tính ---
                "timestamp": row['timestamp'],
                "user": row['user'],
                "client_ip": client_ip,
                "client_port": client_port,
                "database": row['database'],
                "query": row['query'],
                "query_length": query_len,
                "entropy": entropy,
                "is_system_table": is_sys,
                "num_tables": num_tbls,

                # --- Nhóm 2: Từ MySQL Metric ---
                "execution_time_sec": real_exec,
                "execution_time_ms": exec_time_ms,
                "lock_time_sec": lock_time,
                "rows_returned": rows_sent,
                "rows_examined": rows_examined,
                "rows_affected": rows_affected,
                "created_tmp_disk_tables": tmp_disk,
                "created_tmp_tables": tmp_mem,
                "query_digest": digest,
                "normalized_query": digest_text,     
                "warning_count": warnings,
                "no_index_used": no_index,
                
                # --- Nhóm 3: Error Handling ---
                "error_code": error_code,
                "error_message": error_msg,
                "error_count": errors,

                # --- Metadata khác ---
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
    print(f"🚀 BẮT ĐẦU CHẠY SIMULATION ")
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