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
    state = {"last_event_id": last_id}
    os.makedirs(os.path.dirname(state_file_path) or ".", exist_ok=True)
    try:
        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logging.error(f"Không thể ghi state file: {e}")

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
def monitor_performance_schema(poll_interval_sec: int = 2):
    # Đảm bảo kết nối đến CSDL MySQL nơi chứa Performance Schema
    db_engine = connect_db(MYSQL_LOG_DATABASE_URL) 
    redis_client = connect_redis()
    
    last_event_id = read_last_known_event_id()
    
    logging.info(f"Bắt đầu theo dõi performance_schema từ sau EVENT_ID: {last_event_id}")
    logging.info("PerfSchemaPublisher đang chạy... Nhấn Ctrl+C để dừng.")

    # Câu SQL MỚI: Đọc từ events_statements_history
    sql_query = text(
        """
        SELECT 
            e.EVENT_ID,
            e.TIMER_START,
            (e.TIMER_WAIT / 1000000) AS execution_time_ms, -- Chuyển pS sang mS
            e.SQL_TEXT AS query_text,
            e.ROWS_SENT AS rows_returned,
            e.ROWS_AFFECTED AS rows_affected,
            e.CURRENT_SCHEMA AS db_name,
            t.PROCESSLIST_USER AS user,   -- Lấy từ bảng threads
            t.PROCESSLIST_HOST AS host    -- Lấy từ bảng threads
        FROM 
            performance_schema.events_statements_history AS e
        
        -- Thêm JOIN với bảng threads
        JOIN 
            performance_schema.threads AS t ON e.THREAD_ID = t.THREAD_ID
            
        WHERE 
            e.SQL_TEXT IS NOT NULL
            AND e.EVENT_ID > :last_id
            -- Lọc các lệnh của chính trình publisher này (dùng alias 't')
            AND (t.PROCESSLIST_USER IS NULL OR t.PROCESSLIST_USER != 'uba_user') 
            -- Lọc các lệnh nội bộ của Performance Schema
            AND e.CURRENT_SCHEMA IS NOT NULL AND e.CURRENT_SCHEMA != 'performance_schema'
        ORDER BY 
            e.EVENT_ID ASC
        LIMIT 2000; 
        """
    ) # (Nhớ thay 'uba_user' nếu bạn dùng user khác)

    try:
        while True:
            new_records_to_publish = []
            
            try:
                with db_engine.connect() as conn:
                    results = conn.execute(sql_query, {"last_id": last_event_id})
                    
                    for row in results:
                        row_dict = row._mapping
                        
                        # Sử dụng thời gian hiện tại làm timestamp 
                        event_time_dt = datetime.utcnow()

                        record = {
                            "timestamp": event_time_dt.isoformat() + "Z",
                            "user": str(row_dict['user']),
                            "client_ip": str(row_dict['host']),
                            "database": str(row_dict['db_name']),
                            "query": str(row_dict['query_text']),
                            "source_dbms": "MySQL",
                            "execution_time_ms": float(row_dict['execution_time_ms']),
                            "rows_returned": int(row_dict['rows_returned']),
                            "rows_affected": int(row_dict['rows_affected'])
                        }
                        
                        new_records_to_publish.append(record)
                        last_event_id = int(row_dict['EVENT_ID'])

                if new_records_to_publish:
                    # 1. Đẩy vào Redis
                    pipe = redis_client.pipeline()
                    for rec in new_records_to_publish:
                        pipe.xadd(STREAM_KEY, {"data": json.dumps(rec)})
                    pipe.execute()
                    
                    # 2. Lưu ra Parquet 
                    save_logs_to_parquet(new_records_to_publish, source_dbms="MySQL")
                    
                    logging.info(f"Đã đẩy {len(new_records_to_publish)} bản ghi MỚI vào Redis.")
                    write_last_known_event_id(last_event_id)

            except RedisConnectionError as e:
                logging.error(f"Mất kết nối Redis (Publisher): {e}. Đang kết nối lại...")
                redis_client = connect_redis()
            except (OperationalError, ProgrammingError) as e:
                logging.error(f"Lỗi CSDL MySQL: {e}. Đang kết nối lại...")
                db_engine = connect_db(MYSQL_LOG_DATABASE_URL)
            except Exception as e:
                logging.error(f"Lỗi không xác định: {e}", exc_info=True)

            time.sleep(poll_interval_sec)

    except KeyboardInterrupt:
        logging.info("Đã nhận tín hiệu (Ctrl+C). Publisher đang dừng...")
    finally:
        logging.info("PerfSchemaPublisher đã dừng.")

if __name__ == "__main__":
    monitor_performance_schema()