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

# Biến cờ để điều khiển vòng lặp
is_running = True

def handle_shutdown(signum, frame):
    """Xử lý tín hiệu tắt (Ctrl+C) để dừng vòng lặp nhẹ nhàng"""
    global is_running
    logging.info(f"Nhận tín hiệu dừng. Đang tắt Publisher...")
    is_running = False

# Đăng ký signal
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# === 1. Quản lý State ===
def read_last_known_event_id(state_file_path=PERF_SCHEMA_STATE_FILE) -> int:
    try:
        with open(state_file_path, 'r', encoding='utf-8') as f:
            state = json.load(f)
            return int(state.get("last_event_id", 0))
    except:
        logging.warning("Không tìm thấy state file. Bắt đầu từ EVENT_ID = 0.")
        return 0
        
def write_last_known_event_id(last_id: int, state_file_path=PERF_SCHEMA_STATE_FILE):
    state = {"last_event_id": last_id, "last_updated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
    os.makedirs(os.path.dirname(state_file_path) or ".", exist_ok=True)
    try:
        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logging.error(f"Cannot write state file: {e}")

# === 2. Logic Kết nối Tin cậy (Robust) ===
def connect_db(db_url: str):
    while is_running:
        try:
            engine = create_engine(db_url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info("Kết nối MySQL (Publisher) thành công.")
            return engine
        except Exception as e:
            logging.error(f"Kết nối MySQL thất bại: {e}. Thử lại sau 5 giây...")
            time.sleep(5)

def connect_redis():
    while is_running:
        try:
            r = Redis.from_url(REDIS_URL, decode_responses=True)
            r.ping()
            logging.info("Kết nối Redis thành công.")
            return r
        except RedisConnectionError as e:
            logging.error(f"Kết nối Redis thất bại: {e}. Thử lại sau 5 giây...")
            time.sleep(5)

# === 3. Logic Publisher chính ===

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

    last_event_id = read_last_known_event_id()
    
    # Sử dụng bảng LONG để không bị mất dữ liệu của các thread đã đóng
    TABLE_NAME = "performance_schema.events_statements_history_long"

    logging.info(f"🚀 Publisher bắt đầu. Starting EVENT_ID > {last_event_id}")

    # Query lấy dữ liệu
    sql_query = text("""
        SELECT 
            TRUNCATE(e.TIMER_START / 1000000000, 4) AS TIMER_START,
            TRUNCATE(e.TIMER_END / 1000000000, 4) AS TIMER_END,
            e.EVENT_ID,
            e.EVENT_NAME,
            e.SQL_TEXT,
    		e.DIGEST,
            e.DIGEST_TEXT,
            e.CURRENT_SCHEMA,
            TRUNCATE(e.TIMER_WAIT / 1000000000, 4) AS execution_time_ms,
            TRUNCATE(e.LOCK_TIME / 1000000000, 4) AS lock_time_ms,
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
        WHERE e.EVENT_ID > :last_id
            AND e.SQL_TEXT IS NOT NULL
            AND e.SQL_TEXT NOT LIKE '%performance_schema%'
            AND (t.PROCESSLIST_USER IS NULL OR t.PROCESSLIST_USER != 'uba_user')
            AND (e.CURRENT_SCHEMA IS NULL OR e.CURRENT_SCHEMA != 'uba_db')
            AND e.SQL_TEXT != 'rollback'
            AND e.SQL_TEXT != 'FLUSH PRIVILEGES'
            AND e.SQL_TEXT != '%version_comment%'
            AND e.SQL_TEXT != '%auto_commit%'
        ORDER BY e.EVENT_ID ASC
        LIMIT 5000
    """)

    # Query kiểm tra Restart
    check_max_id_sql = text(f"SELECT MAX(EVENT_ID) FROM {TABLE_NAME}")
    
    # Query Uptime để tính Timestamp chính xác
    uptime_sql = text("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='UPTIME'")
    
    while is_running:
        new_records = []
        
        try:
            with db_engine.connect() as conn:
                # --- Tính toán Thời gian thực ---
                uptime_res = conn.execute(uptime_sql).scalar()
                db_uptime_sec = float(uptime_res) if uptime_res else 0
                boot_time = datetime.now(timezone.utc) - timedelta(seconds=db_uptime_sec)

                # --- Lấy Log ---
                results = conn.execute(sql_query, {"last_id": last_event_id})

                for row in results:
                    row_dict = row._mapping
                    eid = int(row_dict['EVENT_ID'])

                    # Tính Timestamp chính xác
                    # BootTime + TimerStart (pico -> sec)
                    timer_start_ms = float(row_dict['TIMER_START'] or 0)
                    if timer_start_ms > 0:
                        real_time = boot_time + timedelta(seconds=timer_start_ms)
                        ts_iso = real_time.isoformat().replace("+00:00", "Z")
                    else:
                        ts_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

                    # --- Feature Extraction Logic ---
                    sql_text = str(row_dict['SQL_TEXT'] or '')
                    sql_upper = sql_text.upper()
                    host_str = str(row_dict['PROCESSLIST_HOST'] or '')
                    
                    # Client Port
                    client_ip = 'unknown'
                    client_port = 0
                    if ':' in host_str:
                        parts = host_str.split(':')
                        client_ip = parts[0]
                        try: client_port = int(parts[1])
                        except: pass
                    else:
                        client_ip = host_str

                    # Entropy & Stats
                    entropy = calculate_entropy(sql_text)
                    rows_sent = int(row_dict['ROWS_SENT'] or 0)
                    rows_examined = int(row_dict['ROWS_EXAMINED'] or 0)
                    # Scan Efficiency: 1.0 is good, 0.0001 is bad (scan huge data for few rows)
                    scan_efficiency = rows_sent / (rows_examined + 1)

                    # Risk Flags
                    db_name = str(row_dict['CURRENT_SCHEMA'] or 'unknown').lower()
                    is_system = 1 if db_name in ['mysql', 'information_schema', 'performance_schema', 'sys'] else 0
                    is_admin = 1 if any(k in sql_upper for k in ['GRANT ', 'REVOKE ', 'CREATE USER']) else 0
                    is_risky = 1 if any(k in sql_upper for k in ['DROP ', 'TRUNCATE ']) else 0
                    has_comment = 1 if ('--' in sql_text or '/*' in sql_text or '#' in sql_text) else 0
                    
                    # Errors
                    err_no = int(row_dict['MYSQL_ERRNO'] or 0)
                    err_cnt = int(row_dict['ERRORS'] or 0)
                    has_error = 1 if (err_cnt > 0 or err_no != 0) else 0

                    record = {
                        "timestamp": ts_iso,
                        "event_id": eid,
                        "event_name": str(row_dict['EVENT_NAME']),   
                        "user": str(row_dict['PROCESSLIST_USER'] or 'unknown'),
                        "client_ip": client_ip,
                        "client_port": client_port,
                        "database": db_name,
                        "query": sql_text,
                        "normalized_query": str(row_dict['DIGEST_TEXT'] or ''),
                        "query_digest": str(row_dict['DIGEST'] or ''),    
                        "query_length": len(sql_text),                 
                        "query_entropy": float(f"{entropy:.4f}"),
                        "is_system_table": is_system, 
                        "scan_efficiency": float(f"{scan_efficiency:.6f}"),
                        "is_admin_command": is_admin,
                        "is_risky_command": is_risky,
                        "has_comment": has_comment,
                        "execution_time_ms": float(row_dict['execution_time_ms'] or 0),
                        "lock_time_ms": float(row_dict['lock_time_ms'] or 0),
                        "rows_returned": rows_sent,
                        "rows_examined": rows_examined,
                        "rows_affected": int(row_dict['ROWS_AFFECTED'] or 0),
                        
                        # Errors
                        "error_code": err_no if err_no != 0 else None,
                        "error_message": str(row_dict['MESSAGE_TEXT']) if row_dict['MESSAGE_TEXT'] else None,
                        "error_count": err_cnt,  # Đã có, đảm bảo int
                        "has_error": has_error,
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
                    last_event_id = int(row_dict['EVENT_ID'])

            # Publish if any
            if new_records:
                pipe = redis_client.pipeline()
                for rec in new_records:
                    pipe.xadd(STREAM_KEY, {"data": json.dumps(rec, ensure_ascii=False)})
                pipe.execute()

                save_logs_to_parquet(new_records, source_dbms="MySQL")
                write_last_known_event_id(last_event_id)
                logging.info(f"Published {len(new_records)} new statements (up to EVENT_ID {last_event_id})")

        except Exception as e:
            logging.error(f"Error in publisher loop: {e}", exc_info=True)
            db_engine = connect_db(MYSQL_LOG_DATABASE_URL)
            redis_client = connect_redis()

        time.sleep(poll_interval_sec)

if __name__ == "__main__":
    monitor_performance_schema()