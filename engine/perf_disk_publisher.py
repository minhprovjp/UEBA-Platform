# engine/perf_disk_publisher.py
import os, json, logging, sys, time, signal
import pandas as pd
from redis import Redis, ConnectionError as RedisConnectionError
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
import math
from collections import Counter

# Setup path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import *
    from engine.utils import save_logs_to_parquet 
except ImportError:
    print("Lỗi: Không thể import 'config' hoặc 'engine.utils'.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [PersistentPublisher] - %(message)s")

# File state chỉ cần lưu ID tự tăng (BigInt)
STATE_FILE = os.path.join(LOGS_DIR, ".mysql_persistent_id.state")
STREAM_KEY = f"{REDIS_STREAM_LOGS}:mysql"
is_running = True

def handle_shutdown(signum, frame):
    global is_running
    logging.info("🛑 Stopping publisher...")
    is_running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# === 1. Quản lý State (Chỉ cần ID) ===
def get_last_id():
    try:
        with open(STATE_FILE, 'r') as f: return int(json.load(f).get("last_id", 0))
    except: return 0

def save_last_id(lid):
    try:
        with open(STATE_FILE, 'w') as f: json.dump({"last_id": lid}, f)
    except: pass

# === 2. Kết nối ===
def connect_db():
    # [FIX] Xử lý chuỗi kết nối để đảm bảo trỏ vào uba_db
    url = MYSQL_LOG_DATABASE_URL
    
    # Logic thay thế tên DB trong URL connection string
    if "/mysql" in url:
        url = url.replace("/mysql", "/uba_db")
    elif "/performance_schema" in url:
        url = url.replace("/performance_schema", "/uba_db")
    elif "/sys" in url:
        url = url.replace("/sys", "/uba_db")
    
    return create_engine(url)

def calculate_entropy(text):
    if not text: return 0.0
    counter = Counter(text)
    length = len(text)
    return -sum((count/length) * math.log2(count/length) for count in counter.values())

# === 3. Main Loop ===
def monitor_persistent_log(poll_interval=1): # Poll nhanh mỗi 1s
    global is_running
    
    engine = connect_db()
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    
    last_id = get_last_id()
    logging.info(f"🚀 Persistent Publisher started. Last ID: {last_id}")

    sql = text("""
        SELECT * FROM uba_db.uba_persistent_log 
        WHERE id > :lid 
          AND (PROCESSLIST_USER IS NULL OR PROCESSLIST_USER NOT IN ('uba_user'))
          AND (CURRENT_SCHEMA IS NULL OR CURRENT_SCHEMA != 'uba_db')
          AND SQL_TEXT IS NOT NULL
          AND e.SQL_TEXT NOT LIKE '%UBA_EVENT%'
        ORDER BY id ASC 
        LIMIT 5000;
    """)

    while is_running:
        batch_start = time.time()
        try:
            with engine.connect() as conn:
                rows = conn.execute(sql, {"lid": last_id}).fetchall()
                
                if not rows:
                    time.sleep(poll_interval)
                    continue

                new_records = []
                max_id = last_id

                for row in rows:
                    r = row._mapping
                    current_id = r['id']
                    if current_id > max_id: max_id = current_id
                    
                    # Convert metrics (trong bảng persistent đã là raw units)
                    # Timer Wait (pico) -> ms
                    exec_ms = float(r['timer_wait'] or 0) / 1000000.0
                    lock_ms = float(r['lock_time'] or 0) / 1000000.0
                    
                    # Text Analysis
                    sql_txt = str(r['sql_text'] or '')
                    sql_up = sql_txt.upper()
                    entropy = calculate_entropy(sql_txt)
                    
                    # Efficiency
                    rows_sent = int(r['rows_sent'] or 0)
                    rows_exam = int(r['rows_examined'] or 0)
                    scan_eff = rows_sent / (rows_exam + 1)
                    
                    # Flags & Security Checks
                    # [FIX TÊN BIẾN] Đổi is_sys thành is_system để khớp bên dưới
                    is_system = 1 if str(r['current_schema']).lower() in ['mysql','information_schema','sys'] else 0
                    is_admin = 1 if any(k in sql_up for k in ['GRANT ','REVOKE ','CREATE USER']) else 0
                    is_risky = 1 if any(k in sql_up for k in ['DROP ','TRUNCATE ']) else 0
                    # [FIX TÊN BIẾN] Đổi has_cmt thành has_comment
                    has_comment = 1 if ('--' in sql_txt or '/*' in sql_txt) else 0
                    
                    # Error Handling
                    err_no = int(r['mysql_errno'] or 0)
                    err_cnt = int(r['errors'] or 0)
                    has_err = 1 if (err_cnt > 0 or err_no != 0) else 0

                    # Timestamp: Lấy trực tiếp từ event_ts (đã được tính chính xác bởi SQL Event)
                    ts_val = r['event_ts']
                    if isinstance(ts_val, datetime):
                        ts_iso = ts_val.isoformat()
                    else:
                        ts_iso = str(ts_val).replace(' ', 'T')

                    record = {
                        # Identity & Time
                        "timestamp": ts_iso,
                        "event_id": int(r['event_id']), # Event ID gốc trong phiên
                        "user": str(r['processlist_user'] or 'unknown'),
                        "client_ip": str(r['processlist_host']).split(':')[0] if r['processlist_host'] else 'unknown',
                        "client_port": 0, 
                        "database": str(r['current_schema'] or 'unknown'),
                        
                        # Content
                        "query": sql_txt,
                        "normalized_query": str(r['digest_text'] or ''),
                        "query_digest": str(r['digest'] or ''),
                        "event_name": str(r['event_name']),
                        
                        # Features
                        "query_length": len(sql_txt),
                        "query_entropy": float(f"{entropy:.4f}"),
                        "scan_efficiency": float(f"{scan_eff:.6f}"),
                        "is_system_table": is_system, # [FIXED] Đã khớp tên biến
                        "is_admin_command": is_admin,
                        "is_risky_command": is_risky,
                        "has_comment": has_comment,   # [FIXED] Đã khớp tên biến
                        "has_error": has_err,
                        
                        # Metrics
                        "execution_time_ms": exec_ms,
                        "lock_time_ms": lock_ms,
                        "cpu_time_ms": float(r['CPU_TIME'] or 0) / 1000000.0, # Pico -> ms
                        "program_name": str(r['program_name'] or 'unknown'),
                        "connector_name": str(r['_connector_name'] or 'unknown'),
                        "client_os": str(r['client_os'] or 'unknown'),
                        "source_host": str(r['source_host'] or 'unknown'),
                        "rows_returned": rows_sent,
                        "rows_examined": rows_exam,
                        "rows_affected": int(r['rows_affected'] or 0),
                        
                        # Errors
                        "error_code": int(r['mysql_errno']) if r['mysql_errno'] else None,
                        "error_message": str(r['message_text'] or ''),
                        "error_count": int(r['errors'] or 0),
                        "warning_count": int(r['warnings'] or 0),
                        
                        # Optimizer
                        "created_tmp_disk_tables": int(r['created_tmp_disk_tables'] or 0),
                        "created_tmp_tables": int(r['created_tmp_tables'] or 0),
                        "select_full_join": int(r['select_full_join'] or 0),
                        "select_scan": int(r['select_scan'] or 0),
                        "sort_merge_passes": int(r['sort_merge_passes'] or 0),
                        "no_index_used": int(r['no_index_used'] or 0),
                        "no_good_index_used": int(r['no_good_index_used'] or 0),
                        
                        "source_dbms": "MySQL",
                        "connection_type": str(r['connection_type'] or ''),
                        "thread_os_id": int(r['thread_os_id'] or 0)
                    }
                    
                    new_records.append(record)

                if new_records:
                    pipe = redis.pipeline()
                    for rec in new_records:
                        pipe.xadd(STREAM_KEY, {"data": json.dumps(rec, default=str)})
                    pipe.execute()
                    
                    save_logs_to_parquet(new_records, source_dbms="MySQL")
                    
                    last_id = max_id
                    save_last_id(last_id)
                    logging.info(f"Published {len(new_records)} logs. (Last DB ID: {last_id})")

        except Exception as e:
            if is_running:
                logging.error(f"Error: {e}")
                time.sleep(5)
                # Reconnect
                try:
                    engine = connect_db()
                    redis = Redis.from_url(REDIS_URL, decode_responses=True)
                except: pass
        
        time.sleep(poll_interval)

    logging.info("Publisher stopped.")

if __name__ == "__main__":
    monitor_persistent_log()