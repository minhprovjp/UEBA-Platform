# engine/perf_log_publisher.py
import os, json, logging, sys, time, signal
import pandas as pd
from redis import Redis, ConnectionError as RedisConnectionError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from datetime import datetime, timedelta, timezone
import math
from collections import Counter
import re

# Thêm thư mục gốc
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import *
    from engine.utils import save_logs_to_parquet 
except ImportError:
    print("Lỗi: Không thể import 'config' hoặc 'engine.utils'.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [PerfSchemaPublisher] - %(message)s"
)

# File state
PERF_SCHEMA_STATE_FILE = os.path.join(LOGS_DIR, ".mysql_perf_schema.state")
STREAM_KEY = f"{REDIS_STREAM_LOGS}:mysql"

# Flag để điều khiển vòng lặp
is_running = True

def handle_shutdown(signum, frame):
    """Xử lý tín hiệu tắt (Ctrl+C) để dừng vòng lặp"""
    global is_running
    logging.info(f"🛑 Nhận tín hiệu dừng. Đang tắt Publisher...")
    is_running = False

# Đăng ký signal
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# === 1. Quản lý State (TIMER_START) ===
def read_last_timer():
    try:
        with open(PERF_SCHEMA_STATE_FILE, 'r', encoding='utf-8') as f:
            state = json.load(f)
            # Trả về 0 nếu file rỗng hoặc lỗi
            return int(state.get("last_timer_start", 0))
    except:
        logging.warning("Không tìm thấy state file. Bắt đầu từ timestamp = 0.")
        return 0
        
def write_last_timer(ts: int):
    state = {"last_timer_start": ts, "last_updated": datetime.now(timezone.utc).isoformat()}
    os.makedirs(os.path.dirname(PERF_SCHEMA_STATE_FILE) or ".", exist_ok=True)
    try:
        with open(PERF_SCHEMA_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logging.error(f"Cannot write state file: {e}")

# === 2. Kết nối ===
def connect_db(db_url: str):
    while is_running:
        try:
            engine = create_engine(db_url)
            with engine.connect() as conn:
                try:
                    conn.execute(text("UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME LIKE 'events_statements_history_long'"))
                except: pass
            logging.info("✅ Kết nối MySQL thành công.")
            return engine
        except Exception as e:
            logging.error(f"❌ Lỗi kết nối MySQL: {e}. Thử lại sau 5s...")
            time.sleep(5)
    return None

def connect_redis():
    while is_running:
        try:
            r = Redis.from_url(REDIS_URL, decode_responses=True)
            r.ping()
            logging.info("✅ Kết nối Redis thành công.")
            return r
        except Exception as e:
            logging.error(f"❌ Lỗi kết nối Redis: {e}. Thử lại sau 5s...")
            time.sleep(5)
    return None

# === 3. Logic Feature ===

# Hàm hỗ trợ tính Entropy
def calculate_entropy(text):
    if not text: return 0.0
    counter = Counter(text)
    length = len(text)
    return -sum((count/length) * math.log2(count/length) for count in counter.values())

# === 4. Main Loop ===
def monitor_performance_schema(poll_interval_sec: int = 2):
    global is_running
    
    db_engine = connect_db(MYSQL_LOG_DATABASE_URL)
    redis_client = connect_redis()
    
    if not db_engine or not redis_client: return

    # Load State (TIMER_START)
    last_timer = read_last_timer()
    TABLE_NAME = "performance_schema.events_statements_history_long"

    logging.info(f"🚀 Publisher bắt đầu. Starting TIMER_START > {last_timer}")

    # Query lấy dữ liệu
    sql_query = text(f"""
        SELECT 
            e.TIMER_START,
            e.TIMER_END,
            e.EVENT_ID,
            e.EVENT_NAME,
            e.SQL_TEXT,
    		e.DIGEST,
            e.DIGEST_TEXT,
            e.CURRENT_SCHEMA,
            e.TIMER_WAIT,
            e.LOCK_TIME,
            e.CPU_TIME,
            (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs a 
            WHERE a.PROCESSLIST_ID = t.PROCESSLIST_ID AND a.ATTR_NAME = 'program_name' LIMIT 1) AS program_name,
            (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs a 
            WHERE a.PROCESSLIST_ID = t.PROCESSLIST_ID AND a.ATTR_NAME = '_connector_name' LIMIT 1) AS connector_name,
            (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs a 
            WHERE a.PROCESSLIST_ID = t.PROCESSLIST_ID AND a.ATTR_NAME = '_os' LIMIT 1) AS client_os,
            (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs a 
            WHERE a.PROCESSLIST_ID = t.PROCESSLIST_ID AND a.ATTR_NAME = '_source_host' LIMIT 1) AS source_host,
            e.ROWS_SENT,
            e.ROWS_EXAMINED,
            e.ROWS_AFFECTED,
		    e.MYSQL_ERRNO,
		    e.MESSAGE_TEXT,
		    e.ERRORS,
		    e.WARNINGS,
            e.CREATED_TMP_DISK_TABLES,
            e.CREATED_TMP_TABLES,       
            e.SELECT_FULL_JOIN,         
            e.SELECT_SCAN,              
            e.SORT_MERGE_PASSES,
            e.NO_INDEX_USED,
            e.NO_GOOD_INDEX_USED,
            t.PROCESSLIST_USER,
            COALESCE(t.PROCESSLIST_HOST, 'unknown') AS PROCESSLIST_HOST,
            t.CONNECTION_TYPE,          
            t.THREAD_OS_ID
        FROM performance_schema.events_statements_history_long e
        LEFT JOIN performance_schema.threads t ON e.THREAD_ID = t.THREAD_ID
        WHERE e.TIMER_START > :last_timer
            AND e.SQL_TEXT IS NOT NULL
            AND (t.PROCESSLIST_USER IS NULL OR t.PROCESSLIST_USER NOT IN ('uba_user'))
            AND (e.CURRENT_SCHEMA IS NULL OR e.CURRENT_SCHEMA != 'uba_db')
            AND SQL_TEXT NOT LIKE '%UBA_EVENT%'
        ORDER BY e.TIMER_START ASC
        LIMIT 5000
    """)

    # Query kiểm tra Max Timer
    check_max_sql = text(f"SELECT MAX(TIMER_START) FROM performance_schema.events_statements_history_long WHERE 'UBA_EVENT' = 'UBA_EVENT'")
    # Query Uptime
    uptime_sql = text("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='UPTIME' AND 'UBA_EVENT' = 'UBA_EVENT'")

    while is_running:
        batch_start = time.time()
        new_records = []
        
        try:
            with db_engine.connect() as conn:
                # 1. Kiểm tra Reset (MySQL Restart)
                curr_max_timer = conn.execute(check_max_sql).scalar()
                if curr_max_timer is None: curr_max_timer = 0
                else: curr_max_timer = int(curr_max_timer)

                # Nếu Timer hiện tại nhỏ hơn Timer đã lưu -> Reset
                if curr_max_timer < last_timer:
                    logging.warning(f"⚠️ DB Restart Detected (DB: {curr_max_timer} < Local: {last_timer}). Resetting state.")
                    last_timer = 0
                    write_last_timer(0)
                    time.sleep(5)
                    continue
                
                # Nếu không có log mới
                if curr_max_timer == last_timer:
                    time.sleep(poll_interval_sec)
                    continue

                # 2. Tính Boot Time
                uptime_res = conn.execute(uptime_sql).scalar()
                uptime_sec = float(uptime_res) if uptime_res else 0
                boot_time = datetime.now(timezone.utc) - timedelta(seconds=uptime_sec)

                # 3. Fetch Logs
                results = conn.execute(sql_query, {"last_timer": last_timer})
                batch_max_timer = last_timer

                for row in results:
                    row_dict = row._mapping
                    
                    # Cập nhật con trỏ (TIMER_START)
                    t_start_raw = int(row_dict['TIMER_START'] or 0)
                    if t_start_raw > batch_max_timer:
                        batch_max_timer = t_start_raw

                    # Tính Timestamp = BootTime + TimerStart (pico -> seconds)
                    if t_start_raw > 0:
                        event_time = boot_time + timedelta(seconds=t_start_raw / 1e12)
                        ts_iso = event_time.isoformat().replace("+00:00", "Z")
                    else:
                        ts_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

                    # --- Feature Extraction Logic ---
                    sql_text = str(row_dict['SQL_TEXT'] or '')
                    sql_upper = sql_text.upper()
                    entropy = calculate_entropy(sql_text)
                    
                    host_str = str(row_dict['PROCESSLIST_HOST'] or '')
                    client_ip, client_port = 'unknown', 0
                    if ':' in host_str:
                        p = host_str.split(':')
                        client_ip = p[0]
                        try: client_port = int(p[1])
                        except: pass
                    else: client_ip = host_str
                    
                    # Build Record
                    record = {
                        "timestamp": ts_iso,
                        "event_id": int(row_dict['EVENT_ID']),
                        "event_name": str(row_dict['EVENT_NAME']),
                        "user": str(row_dict['PROCESSLIST_USER'] or 'unknown'),
                        "client_ip": client_ip,
                        "client_port": client_port,
                        "database": str(row_dict['CURRENT_SCHEMA'] or 'unknown').lower(),
                        "query": sql_text,
                        "normalized_query": str(row_dict['DIGEST_TEXT'] or ''),
                        "query_digest": str(row_dict['DIGEST'] or ''),
                        "query_length": len(sql_text),
                        "query_entropy": float(f"{entropy:.4f}"),
                        "is_system_table": 1 if str(row_dict['CURRENT_SCHEMA'] or '').lower() in ['mysql','information_schema','performance_schema','sys'] else 0,
                        "scan_efficiency": float(f"{int(row_dict['ROWS_SENT'] or 0) / (int(row_dict['ROWS_EXAMINED'] or 0) + 1):.6f}"),
                        "is_admin_command": 1 if any(k in sql_upper for k in ['GRANT','REVOKE','CREATE USER']) else 0,
                        "is_risky_command": 1 if any(k in sql_upper for k in ['DROP','TRUNCATE']) else 0,
                        "has_comment": 1 if ('--' in sql_text or '/*' in sql_text or '#' in sql_text) else 0,
                        "execution_time_ms": float(row_dict['TIMER_WAIT'] or 0) / 1e6, 
                        "lock_time_ms": float(row_dict['LOCK_TIME'] or 0) / 1e6,
                        "cpu_time_ms": float(row_dict['CPU_TIME'] or 0) / 1000000.0, # Pico -> ms
                        "program_name": str(row_dict['program_name'] or 'unknown'),
                        "connector_name": str(row_dict['connector_name'] or 'unknown'),
                        "client_os": str(row_dict['client_os'] or 'unknown'),
                        "source_host": str(row_dict['source_host'] or 'unknown'),
                        "rows_returned": int(row_dict['ROWS_SENT'] or 0),
                        "rows_examined": int(row_dict['ROWS_EXAMINED'] or 0),
                        "rows_affected": int(row_dict['ROWS_AFFECTED'] or 0),
                        
                        # Errors
                        "error_code": int(row_dict['MYSQL_ERRNO']) if row_dict['MYSQL_ERRNO'] else None,
                        "error_message": str(row_dict['MESSAGE_TEXT']) if row_dict['MESSAGE_TEXT'] else None,
                        "error_count": int(row_dict['ERRORS'] or 0),
                        "has_error": 1 if (int(row_dict['ERRORS'] or 0) > 0 or int(row_dict['MYSQL_ERRNO'] or 0) != 0) else 0,
                        "warning_count": int(row_dict['WARNINGS'] or 0),
                        
                        # Optimizer Metrics
                        "created_tmp_disk_tables": int(row_dict['CREATED_TMP_DISK_TABLES'] or 0),
                        "created_tmp_tables": int(row_dict['CREATED_TMP_TABLES'] or 0),
                        "select_full_join": int(row_dict['SELECT_FULL_JOIN'] or 0),
                        "select_scan": int(row_dict['SELECT_SCAN'] or 0),
                        "sort_merge_passes": int(row_dict['SORT_MERGE_PASSES'] or 0),
                        "no_index_used": int(row_dict['NO_INDEX_USED'] or 0),
                        "no_good_index_used": int(row_dict['NO_GOOD_INDEX_USED'] or 0),
                        
                        "connection_type": str(row_dict['CONNECTION_TYPE'] or 'unknown'),
                        "thread_os_id": int(row_dict['THREAD_OS_ID'] or 0),
                        "source_dbms": "MySQL"
                    }
                    new_records.append(record)

                # 5. Push & Update State
                if new_records:
                    pipe = redis_client.pipeline()
                    for rec in new_records:
                        pipe.xadd(STREAM_KEY, {"data": json.dumps(rec, ensure_ascii=False)})
                    pipe.execute()

                    save_logs_to_parquet(new_records, source_dbms="MySQL")
                    
                    last_timer = batch_max_timer
                    write_last_timer(last_timer)
                    logging.info(f"Published {len(new_records)} logs. (Timer: {last_timer})")
                
                else:
                    # Catch-up: Nếu DB trôi đi (do filter), vẫn cập nhật con trỏ
                    if curr_max_timer > last_timer:
                        last_timer = curr_max_timer
                        write_last_timer(last_timer)

        except Exception as e:
            logging.error(f"Error: {e}")
            time.sleep(5)
            try:
                db_engine = connect_db(MYSQL_LOG_DATABASE_URL)
                redis_client = connect_redis()
            except: pass

        # Sleep
        for _ in range(int(poll_interval_sec * 2)):
            if not is_running: break
            time.sleep(0.5)

    logging.info("Publisher đã dừng hoàn toàn.")

if __name__ == "__main__":
    monitor_performance_schema()