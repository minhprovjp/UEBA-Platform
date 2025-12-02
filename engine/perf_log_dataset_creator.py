# engine/perf_log_dataset_creator.py
import os, json, logging, sys, time, signal, re, math, csv
from datetime import datetime, timedelta, timezone
from collections import Counter
from redis import Redis
from sqlalchemy import create_engine, text
import pandas as pd

# Import Config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import * 
except: 
    print("Lỗi: Không tìm thấy config.py"); 
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%H:%M:%S")

# --- CẤU HÌNH ---
STATE_FILE = os.path.join(LOGS_DIR, ".mysql_perf_creator.state")
CSV_OUTPUT_FILE = "final_dataset_10days.csv"
STREAM_KEY = f"{REDIS_STREAM_LOGS}:mysql"
is_running = True
total_collected = 0

def sort_final_csv():
    """
    Đọc lại toàn bộ file CSV, sắp xếp theo timestamp tăng dần và lưu lại.
    Giúp dataset chuẩn chỉ cho Time-series Analysis.
    """
    if not os.path.exists(CSV_OUTPUT_FILE): return
    
    print("\n⏳ Sorting dataset by Timestamp... (Do not close)")
    try:
        # Đọc CSV
        df = pd.read_csv(CSV_OUTPUT_FILE)
        
        # Chuyển cột timestamp sang datetime để sort chuẩn
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Sort
        df = df.sort_values(by='timestamp')
        
        # Lưu lại (Ghi đè)
        df.to_csv(CSV_OUTPUT_FILE, index=False, quoting=csv.QUOTE_ALL)
        print(f"✅ Dataset sorted successfully! ({len(df)} rows)")
    except Exception as e:
        print(f"❌ Error sorting CSV: {e}")

def spoof_error_message(error_msg, fake_ip):
    """
    Thay thế 'user'@'localhost' thành 'user'@'fake_ip' trong thông báo lỗi.
    Input: Access denied for user 'dev_user_0'@'localhost' ...
    Output: Access denied for user 'dev_user_0'@'192.168.1.149' ...
    """
    if not error_msg: return ""
    
    # Regex tìm pattern: 'username'@'hostname'
    pattern = r"'([^']+)'@'([^']+)'"
    match = re.search(pattern, error_msg)
    
    if match:
        username = match.group(1)
        hostname = match.group(2) # Thường là localhost hoặc ip thật
        
        if hostname != fake_ip:
            # Thay thế localhost bằng IP fake
            old_str = f"'{username}'@'{hostname}'"
            new_str = f"'{username}'@'{fake_ip}'"
            return error_msg.replace(old_str, new_str)
            
    return error_msg

def handle_shutdown(signum, frame):
    global is_running
    is_running = False
    print("\nStopping Creator...")
    # Khi bấm Ctrl+C, sẽ thực hiện sắp xếp file
    sort_final_csv()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_shutdown)

# --- HELPER FUNCTIONS ---
def extract_extended_metadata(sql_text, db_user, db_host):
    """
    Parse Tag từ Step 3: /* SIM_META:User|IP|Port|ID:x|BEH:type|ANO:0|TS:timestamp */
    """
    pattern = r"/\* SIM_META:(.*?) \*/"
    match = re.search(pattern, sql_text)
    
    meta = {
        "sim_user": db_user, 
        "sim_ip": db_host.split(':')[0], 
        "sim_port": 0,
        "beh_type": "NORMAL", 
        "is_anomaly": 0,
        "sim_ts": None # Thêm trường timestamp giả lập
    }
    
    clean_sql = sql_text
    
    if match:
        parts = match.group(1).split('|')
        if len(parts) >= 3:
            meta["sim_user"] = parts[0]
            meta["sim_ip"] = parts[1]
            try: meta["sim_port"] = int(parts[2])
            except: pass
            
            for p in parts[3:]:
                if p.startswith("BEH:"): meta["beh_type"] = p.replace("BEH:", "")
                if p.startswith("ANO:"): meta["is_anomaly"] = int(p.replace("ANO:", ""))
                if p.startswith("TS:"): meta["sim_ts"] = p.replace("TS:", "") # Bắt timestamp
            
            clean_sql = re.sub(pattern, "", sql_text).strip()
            
    return meta, clean_sql

def calculate_entropy(text):
    if not text: return 0.0
    c = Counter(text); l = len(text)
    return -sum((v/l) * math.log2(v/l) for v in c.values())

def init_csv_file(headers):
    """
    Kiểm tra file CSV. Nếu header cũ không khớp header mới -> Xóa tạo lại.
    """
    should_create = True
    if os.path.exists(CSV_OUTPUT_FILE):
        try:
            with open(CSV_OUTPUT_FILE, 'r', encoding='utf-8') as f:
                existing_header = f.readline().strip().replace('"', '').split(',')
                # So sánh header (bỏ qua khác biệt nhỏ về khoảng trắng)
                if len(existing_header) == len(headers) and existing_header[0] == headers[0]:
                    should_create = False
                else:
                    logging.warning("⚠️ CSV Header mismatch! Deleting old file to prevent corruption.")
        except: pass
    
    if should_create:
        if os.path.exists(CSV_OUTPUT_FILE): os.remove(CSV_OUTPUT_FILE)
        with open(CSV_OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_ALL)
            writer.writeheader()
        logging.info(f"✅ Created new CSV file: {CSV_OUTPUT_FILE}")

# --- CORE LOGIC ---
def process_logs():
    global is_running, total_collected
    engine = create_engine(MYSQL_LOG_DATABASE_URL)
    try: redis = Redis.from_url(REDIS_URL, decode_responses=True)
    except: redis = None
    
    # Init CSV Header
    csv_headers = [
        "timestamp", "event_id", "event_name", 
        "user", "client_ip", "client_port", "database", 
        "query", "normalized_query", "query_digest",
        "query_length", "query_entropy", 
        
        # Nhóm Cờ (Flags)
        "is_system_table", "scan_efficiency", "is_admin_command", "is_risky_command", "has_comment",
        
        # Nhóm Thời gian & Hiệu năng
        "execution_time_ms", "lock_time_ms", 
        "rows_returned", "rows_examined", "rows_affected",
        
        # Nhóm Lỗi
        "error_code", "error_message", "error_count", "has_error", "warning_count",
        
        # Nhóm Optimizer (Rất quan trọng để phát hiện bất thường)
        "created_tmp_disk_tables", "created_tmp_tables", 
        "select_full_join", "select_scan", "sort_merge_passes",
        "no_index_used", "no_good_index_used",
        
        # Nhóm Định danh hệ thống
        "connection_type", "thread_os_id",
        
        # NHÃN (LABELS) - QUAN TRỌNG NHẤT
        "source_dbms", "behavior_type", "is_anomaly"
    ]
    
    # Khởi tạo/Kiểm tra file CSV
    init_csv_file(csv_headers)
    
    # Tạo file CSV và ghi Header nếu file chưa tồn tại
    if not os.path.exists(CSV_OUTPUT_FILE):
        with open(CSV_OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_headers, quoting=csv.QUOTE_ALL)
            writer.writeheader()

    # Load trạng thái cũ để không đọc lại log đã xử lý
    try:
        with open(STATE_FILE, 'r') as f: last_ts = int(json.load(f).get("last_timestamp", 0))
    except: last_ts = 0

    logging.info(f"Dataset Creator started. Monitoring from TIMER_END: {last_ts}")

    # Query lấy Log
    sql_query = text("""
        SELECT 
            e.TIMER_START, e.TIMER_END, 
            e.EVENT_ID, e.EVENT_NAME,
            e.SQL_TEXT, e.DIGEST, e.DIGEST_TEXT, e.CURRENT_SCHEMA,
            TRUNCATE(e.TIMER_WAIT / 1000000000, 4) AS execution_time_ms,
            TRUNCATE(e.LOCK_TIME / 1000000000, 4) AS lock_time_ms,
            e.ROWS_SENT, e.ROWS_EXAMINED, e.ROWS_AFFECTED,
            e.MYSQL_ERRNO, e.MESSAGE_TEXT, e.ERRORS, e.WARNINGS,
            e.CREATED_TMP_DISK_TABLES, e.CREATED_TMP_TABLES,
            e.SELECT_FULL_JOIN, e.SELECT_SCAN, e.SORT_MERGE_PASSES,
            e.NO_INDEX_USED, e.NO_GOOD_INDEX_USED,
            t.PROCESSLIST_USER, 
            COALESCE(t.PROCESSLIST_HOST, 'localhost') AS PROCESSLIST_HOST,
            t.CONNECTION_TYPE, t.THREAD_OS_ID
        FROM performance_schema.events_statements_history_long e
        LEFT JOIN performance_schema.threads t ON e.THREAD_ID = t.THREAD_ID
        WHERE e.TIMER_END > :last_ts
            AND e.SQL_TEXT IS NOT NULL
            AND e.SQL_TEXT NOT LIKE '%performance_schema%'e
            AND (t.PROCESSLIST_USER IS NULL OR t.PROCESSLIST_USER != 'uba_user')
            AND (e.CURRENT_SCHEMA IS NULL OR e.CURRENT_SCHEMA != 'uba_db')
            AND e.SQL_TEXT LIKE '%SIM_META%' 
            AND e.SQL_TEXT != 'rollback'
            AND e.SQL_TEXT != 'FLUSH PRIVILEGES'
            AND e.SQL_TEXT != '%version_comment%'
            AND e.SQL_TEXT != '%auto_commit%'
        ORDER BY e.TIMER_END ASC 
        LIMIT 5000;
    """)
    
    check_pending_sql = text("SELECT COUNT(*) FROM performance_schema.events_statements_history_long WHERE TIMER_END > :last_ts")
    check_max = text("SELECT MAX(TIMER_END) FROM performance_schema.events_statements_history_long")
    get_uptime = text("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='UPTIME'")

    while is_running:
        try:
            with engine.connect() as conn:
                curr_max = conn.execute(check_max).scalar()
                curr_max = int(curr_max) if curr_max is not None else 0
                
                # Nếu DB khởi động lại (Timer reset về 0), ta reset last_ts
                if curr_max < last_ts: last_ts = 0
                pending_count = conn.execute(check_pending_sql, {"last_ts": last_ts}).scalar() or 0
                if pending_count == 0:
                    time.sleep(0.5); continue

                uptime = float(conn.execute(get_uptime).scalar() or 0)
                boot_time = datetime.now(timezone.utc) - timedelta(seconds=uptime)

                results = conn.execute(sql_query, {"last_ts": last_ts})
                batch_max = last_ts
                records = []

                for r in results:
                    r_map = r._mapping
                    if r_map['TIMER_END'] > batch_max: batch_max = r_map['TIMER_END']

                    # 1. Bóc tách Metadata (bao gồm Fake Time)
                    raw_sql = str(r_map['SQL_TEXT'] or '')
                    meta, clean_sql = extract_extended_metadata(
                        raw_sql, str(r_map['PROCESSLIST_USER']), str(r_map['PROCESSLIST_HOST'])
                    )

                    # 2. XỬ LÝ TIME (Quan trọng nhất cho Rule Testing)
                    # Nếu kịch bản có TS, dùng TS đó. Nếu không, tính từ hệ thống.
                    if meta["sim_ts"]:
                        ts_iso = meta["sim_ts"]
                    else:
                        t_start = float(r_map['TIMER_START'] or 0)
                        ts_iso = (boot_time + timedelta(seconds=t_start/1e12)).isoformat().replace("+00:00", "Z")

                    # Tính toán Feature
                    entropy = calculate_entropy(clean_sql)
                    sql_up = clean_sql.upper()
                    
                    rows_sent = int(r_map['ROWS_SENT'] or 0)
                    rows_exam = int(r_map['ROWS_EXAMINED'] or 0)
                    scan_efficiency = rows_sent / (rows_exam + 1)

                    is_admin = 1 if any(k in sql_up for k in ['GRANT ', 'REVOKE ', 'CREATE USER']) else 0
                    is_risky = 1 if any(k in sql_up for k in ['DROP ', 'TRUNCATE ']) else 0
                    has_comment = 1 if ('--' in clean_sql or '/*' in clean_sql or '#' in clean_sql) else 0
                    is_system = 1 if r_map['CURRENT_SCHEMA'] in ['mysql','information_schema','performance_schema','sys'] else 0
                    
                    # Lấy message gốc từ MySQL
                    raw_error_msg = str(r_map['MESSAGE_TEXT'] or "")
                    
                    # Nếu có IP fake (từ tag), thì sửa lại message cho khớp
                    final_error_msg = raw_error_msg
                    if meta["sim_ip"] and raw_error_msg:
                        final_error_msg = spoof_error_message(raw_error_msg, meta["sim_ip"])

                    # 3. Tạo Record
                    rec = {
                        "timestamp": ts_iso,
                        "event_id": int(r_map['EVENT_ID']),
                        "event_name": str(r_map['EVENT_NAME']),
                        "user": meta["sim_user"],
                        "client_ip": meta["sim_ip"],
                        "client_port": meta["sim_port"],
                        "database": str(r_map['CURRENT_SCHEMA'] or 'unknown').lower(),
                        "query": clean_sql,
                        "normalized_query": str(r_map['DIGEST_TEXT'] or ''),
                        "query_digest": str(r_map['DIGEST'] or ''),                        
                        "query_length": len(clean_sql),
                        "query_entropy": float(f"{entropy:.4f}"),
                        "is_system_table": is_system,
                        "scan_efficiency": float(f"{scan_efficiency:.6f}"),
                        "is_admin_command": is_admin,
                        "is_risky_command": is_risky,
                        "has_comment": has_comment,
                        "execution_time_ms": float(r_map['execution_time_ms'] or 0),
                        "lock_time_ms": float(r_map['lock_time_ms'] or 0),
                        "rows_returned": rows_sent,
                        "rows_examined": rows_exam,
                        "rows_affected": int(r_map['ROWS_AFFECTED'] or 0),
                        
                        "error_code": int(r_map['MYSQL_ERRNO'] or 0),
                        "error_message": final_error_msg,
                        "error_count": int(r_map['ERRORS'] or 0),
                        "has_error": 1 if int(r_map['ERRORS'] or 0) > 0 or int(r_map['MYSQL_ERRNO'] or 0) > 0 else 0,
                        "warning_count": int(r_map['WARNINGS'] or 0),
                        
                        "created_tmp_disk_tables": int(r_map['CREATED_TMP_DISK_TABLES'] or 0),
                        "created_tmp_tables": int(r_map['CREATED_TMP_TABLES'] or 0),
                        "select_full_join": int(r_map['SELECT_FULL_JOIN'] or 0),
                        "select_scan": int(r_map['SELECT_SCAN'] or 0),
                        "sort_merge_passes": int(r_map['SORT_MERGE_PASSES'] or 0),
                        "no_index_used": int(r_map['NO_INDEX_USED'] or 0),
                        "no_good_index_used": int(r_map['NO_GOOD_INDEX_USED'] or 0),
                        
                        "connection_type": str(r_map['CONNECTION_TYPE'] or 'unknown'),
                        "thread_os_id": int(r_map['THREAD_OS_ID'] or 0),
                        "source_dbms": "MySQL",
                        "behavior_type": meta["beh_type"],
                        "is_anomaly": meta["is_anomaly"]
                    }
                    records.append(rec)

                # 4. Save
                if records:
                    with open(CSV_OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=csv_headers, quoting=csv.QUOTE_ALL)
                        writer.writerows(records)

                    if redis:
                        pipe = redis.pipeline()
                        for r in records:
                            pipe.xadd(STREAM_KEY, {"data": json.dumps(r, ensure_ascii=False)})
                        pipe.execute()
                    
                    last_ts = batch_max
                    with open(STATE_FILE, 'w') as f: json.dump({"last_timestamp": last_ts}, f)
                    
                    total_collected += len(records)
                    sys.stdout.write(f"\r📥 Total Collected: {total_collected} logs")
                    sys.stdout.flush()
                    if len(records) >= 5000: continue
                
                elif curr_max > last_ts:
                    last_ts = curr_max
                    with open(STATE_FILE, 'w') as f: json.dump({"last_timestamp": last_ts}, f)

        except Exception as e:
            logging.error(f"Error: {e}"); time.sleep(5)

if __name__ == "__main__":
    process_logs()