# engine/table_log_publisher.py
import os, json, logging, sys, time, pandas as pd, signal
from redis import Redis, ConnectionError as RedisConnectionError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from typing import Optional
from datetime import datetime, timezone

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

# Tên file state mới
PERF_SCHEMA_STATE_FILE = os.path.join(LOGS_DIR, ".mysql_perf_schema.state")
STREAM_KEY = f"{REDIS_STREAM_LOGS}:mysql"

# Biến cờ để điều khiển vòng lặp
is_running = True

def handle_shutdown(signum, frame):
    """Xử lý tín hiệu tắt (Ctrl+C) để dừng vòng lặp nhẹ nhàng"""
    global is_running
    print("\n")  
    logging.info(f"Nhận tín hiệu dừng. Đang tắt Publisher...")
    is_running = False

# Đăng ký signal
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def read_last_known_timestamp(state_file_path=PERF_SCHEMA_STATE_FILE) -> int:
    try:
        with open(state_file_path, 'r', encoding='utf-8') as f:
            state = json.load(f)
            return int(state.get("last_timestamp", 0))
    except:
        logging.warning("Không tìm thấy state file. Bắt đầu từ timestamp = 0.")
        return 0
        
def write_last_known_timestamp(ts: int, state_file_path=PERF_SCHEMA_STATE_FILE):
    state = {"last_timestamp": ts, "last_updated": datetime.now(timezone.utc).isoformat()}
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
        except OperationalError as e:
            logging.error(f"Kết nối MySQL thất bại: {e}. Thử lại sau 5 giây...")
            time.sleep(5)
    return None

def connect_redis():
    while is_running:
        try:
            r = Redis.from_url(REDIS_URL, decode_responses=True)
            r.ping()
            logging.info("Kết nối Redis (Publisher) thành công.")
            return r
        except RedisConnectionError as e:
            logging.error(f"Kết nối Redis thất bại: {e}. Thử lại sau 5 giây...")
            time.sleep(5)
    return None

# === 3. Logic Publisher chính ===
def monitor_performance_schema(poll_interval_sec: int = 3):
    global is_running
    
    db_engine = connect_db(MYSQL_LOG_DATABASE_URL)
    redis_client = connect_redis()

    last_timestamp = read_last_known_timestamp()

    logging.info(f"Starting from timestamp > {last_timestamp}")

    # PRODUCTION-GRADE QUERY — FULLY COMPATIBLE WITH NEW SCHEMA
    sql_query = text("""
        SELECT 
            e.TIMER_END,
        	e.THREAD_ID,
            e.EVENT_ID,
            e.TIMER_WAIT,
            e.SQL_TEXT,
    		e.DIGEST,
            e.DIGEST_TEXT,
            e.CURRENT_SCHEMA,
            TRUNCATE(e.TIMER_WAIT / 1000000000, 4) AS execution_time_ms,
            e.ROWS_SENT,
            e.ROWS_EXAMINED,
            e.ROWS_AFFECTED,
		    e.MYSQL_ERRNO,
		    e.MESSAGE_TEXT,
		    e.ERRORS,
		    e.WARNINGS,
            e.CREATED_TMP_DISK_TABLES,
            e.NO_INDEX_USED,
            e.NO_GOOD_INDEX_USED,
            e.SELECT_FULL_JOIN,
            t.PROCESSLIST_USER,
            COALESCE(t.PROCESSLIST_HOST, 'unknown') AS PROCESSLIST_HOST
        FROM performance_schema.events_statements_history e
        LEFT JOIN performance_schema.threads t ON e.THREAD_ID = t.THREAD_ID
        WHERE e.TIMER_END > :last_ts
            AND e.SQL_TEXT IS NOT NULL
            AND e.SQL_TEXT NOT LIKE '%performance_schema%'
            AND (t.PROCESSLIST_USER IS NULL OR t.PROCESSLIST_USER != 'uba_user')
        ORDER BY e.TIMER_END ASC
        LIMIT 5000
    """)

    while is_running:
        batch_start = time.time()
        new_records = []
        timestamps = []

        try:
            with db_engine.connect() as conn:
                results = conn.execute(sql_query, {"last_ts": last_timestamp})
                raw_rows = list(results)

            # Logging số lượng record thực tế MySQL trả về
            if raw_rows:
                logging.info(f"Fetched {len(raw_rows)} rows from MySQL (last_ts={last_timestamp})")
            else:
                logging.debug(f"No new rows (last_id={last_timestamp})")

            # Nếu có record → detect overflow
            if raw_rows:
                timestamps = [int(r._mapping["TIMER_END"]) for r in raw_rows]
                new_last = max(timestamps)
                last_timestamp = new_last
                write_last_known_timestamp(new_last)

            # Convert rows
            for row in raw_rows:
                row_dict = row._mapping

                record = {
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "user": str(row_dict['PROCESSLIST_USER'] or 'unknown'),
                    "client_ip": str(row_dict['PROCESSLIST_HOST']).split(':')[0],
                    "database": str(row_dict['CURRENT_SCHEMA'] or 'unknown'),
                    "query": str(row_dict['SQL_TEXT'] or ''),
                    "normalized_query": str(row_dict['DIGEST_TEXT'] or ''),
                    "query_digest": str(row_dict['DIGEST'] or ''),
                    "execution_time_ms": float(row_dict['execution_time_ms'] or 0),
                    "rows_returned": int(row_dict['ROWS_SENT'] or 0),
                    "rows_examined": int(row_dict['ROWS_EXAMINED'] or 0),
                    "rows_affected": int(row_dict['ROWS_AFFECTED'] or 0),
                    "error_code": int(row_dict['MYSQL_ERRNO']) if row_dict['MYSQL_ERRNO'] else None,
                    "error_message": str(row_dict['MESSAGE_TEXT']) if row_dict['MESSAGE_TEXT'] else None,
                    "error_count": int(row_dict['ERRORS'] or 0),
                    "warning_count": int(row_dict['WARNINGS'] or 0),
                    "created_tmp_disk_tables": int(row_dict['CREATED_TMP_DISK_TABLES'] or 0),
                    "no_index_used": int(row_dict['NO_INDEX_USED'] or 0),
                    "source_dbms": "MySQL"
                }

                new_records.append(record)

            # Push to Redis
            if new_records:
                pipe = redis_client.pipeline()
                for rec in new_records:
                    pipe.xadd(STREAM_KEY, {"data": json.dumps(rec, ensure_ascii=False)})
                pipe.execute()

                save_logs_to_parquet(new_records, source_dbms="MySQL")
                logging.info(
                    f"Published {len(new_records)} events "
                    f"Query time: {round(time.time() - batch_start,3)}s"
                )

        except Exception as e:
            logging.error(f"Error in publisher loop: {e}")
            time.sleep(2)
            try:
                db_engine = connect_db(MYSQL_LOG_DATABASE_URL)
                redis_client = connect_redis()
            except:
                pass

        # short sleep
        for _ in range(int(poll_interval_sec * 2)):
            if not is_running:
                break
            time.sleep(0.5)

    logging.info("Publisher đã dừng hoàn toàn.")

if __name__ == "__main__":
    monitor_performance_schema()