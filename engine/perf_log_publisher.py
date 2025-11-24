# engine/table_log_publisher.py
import os, json, logging, sys, time, pandas as pd
from redis import Redis, ConnectionError as RedisConnectionError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from typing import Optional
from datetime import datetime

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

# === 1. Quản lý State (Dùng EVENT_ID thay vì Timestamp) ===
def read_last_known_event_id(state_file_path=PERF_SCHEMA_STATE_FILE) -> int:
    try:
        with open(state_file_path, 'r', encoding='utf-8') as f:
            state = json.load(f)
            return int(state.get("last_event_id", 0))
    except (FileNotFoundError, json.JSONDecodeError):
        logging.warning("Không tìm thấy state file. Bắt đầu từ event_id = 0.")
        return 0
        
def write_last_known_event_id(last_id: int, state_file_path=PERF_SCHEMA_STATE_FILE):
    state = {"last_event_id": last_id, "last_updated": datetime.utcnow().isoformat() + "Z"}
    os.makedirs(os.path.dirname(state_file_path) or ".", exist_ok=True)
    try:
        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logging.error(f"Cannot write state file: {e}")

# === 2. Logic Kết nối Tin cậy (Robust) ===
def connect_db(db_url: str):
    while True:
        try:
            engine = create_engine(db_url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info("Kết nối MySQL (Publisher) thành công.")
            return engine
        except OperationalError as e:
            logging.error(f"Kết nối MySQL (Publisher) thất bại: {e}. Thử lại sau 5 giây...")
            time.sleep(5)

def connect_redis():
    while True:
        try:
            r = Redis.from_url(REDIS_URL, decode_responses=True)
            r.ping()
            logging.info("Kết nối Redis (Publisher) thành công.")
            return r
        except RedisConnectionError as e:
            logging.error(f"Kết nối Redis (Publisher) thất bại: {e}. Thử lại sau 5 giây...")
            time.sleep(5)

# === 3. Logic Publisher chính ===
def monitor_performance_schema(poll_interval_sec: int = 3):
    db_engine = connect_db(MYSQL_LOG_DATABASE_URL)
    redis_client = connect_redis()

    last_event_id = read_last_known_event_id()
    logging.info(f"Starting from EVENT_ID > {last_event_id}")

    # PRODUCTION-GRADE QUERY — FULLY COMPATIBLE WITH NEW SCHEMA
    sql_query = text("""
        SELECT 
            e.EVENT_ID,
            e.TIMER_WAIT,
            e.SQL_TEXT,
            e.DIGEST,
            e.DIGEST_TEXT,
            e.CURRENT_SCHEMA,
            e.ROWS_SENT,
            e.ROWS_AFFECTED,
            e.MYSQL_ERRNO,
            e.MESSAGE_TEXT,
            e.ERRORS,
            e.WARNINGS,
            e.CREATED_TMP_DISK_TABLES,
            e.NO_INDEX_USED,
            t.PROCESSLIST_USER AS user,
            COALESCE(t.PROCESSLIST_HOST, 'unknown') AS host
        FROM performance_schema.events_statements_history e
        LEFT JOIN performance_schema.threads t ON e.THREAD_ID = t.THREAD_ID
        WHERE e.EVENT_ID > :last_id
            AND e.SQL_TEXT IS NOT NULL
            AND e.SQL_TEXT NOT LIKE '%performance_schema%'
            AND (t.PROCESSLIST_USER IS NULL OR t.PROCESSLIST_USER != 'uba_user')
        ORDER BY e.EVENT_ID ASC
        LIMIT 5000
    """)

    while True:
        new_records = []

        try:
            with db_engine.connect() as conn:
                results = conn.execute(sql_query, {"last_id": last_event_id})

                for row in results:
                    row_dict = row._mapping

                    # Convert TIMER_WAIT (picoseconds) → milliseconds
                    exec_time_ms = float(row_dict['TIMER_WAIT']) / 1_000_000_000 if row_dict['TIMER_WAIT'] else 0.0

                    record = {
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "user": str(row_dict['user'] or 'unknown'),
                        "client_ip": str(row_dict['host']).split(':')[0],
                        "database": str(row_dict['CURRENT_SCHEMA'] or 'unknown'),
                        "query": str(row_dict['SQL_TEXT'] or ''),
                        "normalized_query": str(row_dict['DIGEST_TEXT'] or ''),
                        "query_digest": str(row_dict['DIGEST'] or ''),
                        "execution_time_ms": round(float(row_dict['TIMER_WAIT'] or 0) / 1_000_000_000, 6),
                        "rows_returned": int(row_dict['ROWS_SENT'] or 0),
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