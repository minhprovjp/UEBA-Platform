# engine/table_log_publisher.py
import os
import json
import logging
import sys
import time
import pandas as pd
from redis import Redis, ConnectionError as RedisConnectionError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from typing import Optional

# Thêm thư mục gốc
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import *
except ImportError:
    print("Lỗi: Không thể import 'config'.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [TablePublisher] - %(message)s"
)

# Tên file state mới để tránh nhầm lẫn với file cũ
TABLE_STATE_FILE = os.path.join(LOGS_DIR, ".mysql_table_publisher.state")
STREAM_KEY = f"{REDIS_STREAM_LOGS}:mysql"

# === 1. Quản lý State (Trạng thái) ===
def read_last_known_timestamp(state_file_path=TABLE_STATE_FILE) -> str:
    """Đọc timestamp cuối cùng đã xử lý từ file JSON."""
    try:
        with open(state_file_path, 'r', encoding='utf-8') as f:
            state = json.load(f)
            # Trả về timestamp dưới dạng string, sẵn sàng cho SQL
            return state.get("last_event_time", "2000-01-01 00:00:00")
    except (FileNotFoundError, json.JSONDecodeError):
        logging.warning("Không tìm thấy state file. Bắt đầu từ 2000-01-01.")
        return "2000-01-01 00:00:00"

def write_last_known_timestamp(last_time_str: str, state_file_path=TABLE_STATE_FILE):
    """Ghi timestamp cuối cùng đã xử lý ra file JSON."""
    state = {"last_event_time": last_time_str}
    os.makedirs(os.path.dirname(state_file_path) or ".", exist_ok=True)
    try:
        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logging.error(f"Không thể ghi state file: {e}")

# === 2. Logic Kết nối Tin cậy (Robust) ===
def connect_db(db_url: str):
    """Kết nối đến MySQL với cơ chế thử lại."""
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
    """Kết nối đến Redis với cơ chế thử lại."""
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
def monitor_log_table(poll_interval_sec: int = 1):
    """
    Theo dõi bảng mysql.general_log, xử lý và đẩy vào Redis Stream.
    """
    db_engine = connect_db(MYSQL_LOG_DATABASE_URL)
    redis_client = connect_redis()
    
    # Tải timestamp cuối cùng đã xử lý
    last_event_time_str = read_last_known_timestamp()
    
    logging.info(f"Bắt đầu theo dõi mysql.general_log từ sau: {last_event_time_str}")
    logging.info("TablePublisher đang chạy... Nhấn Ctrl+C để dừng.")

    # Đây là câu SQL mấu chốt:
    # 1. CONVERT(argument USING utf8mb4) để giải mã HEX
    # 2. Lọc theo command_type và event_time

    sql_query = text(
        """
        SELECT 
            event_time, 
            user_host, 
            thread_id,
            CONVERT(argument USING utf8mb4) AS query_text 
        FROM 
            mysql.general_log 
        WHERE 
            command_type = 'Query' AND event_time > :last_time
            
            AND user_host NOT LIKE 'uba_user[%' 
            
            AND query_text NOT LIKE '%UBA_EVENT%'
            
        ORDER BY 
            event_time ASC
        LIMIT 5000; 
        """
    )

    try:
        while True:
            new_records_to_publish = []
            
            try:
                with db_engine.connect() as conn:
                    results = conn.execute(sql_query, {"last_time": last_event_time_str})
                    
                    for row in results:
                        row_dict = row._mapping
                        
                        # Tách user và host
                        user_host = str(row_dict['user_host'])
                        user = user_host.split('[')[0].strip()
                        host = user_host[user_host.find('[')+1:user_host.find(']')].strip() or 'N/A'
                        
                        # Chuyển đổi event_time (datetime) thành string ISO 8601 UTC
                        # (Giả sử event_time từ DB là local, cần chuẩn hóa)
                        event_time_dt = pd.to_datetime(row_dict['event_time']).tz_localize(None)

                        record = {
                            "timestamp": event_time_dt.isoformat() + "Z", # Chuẩn UTC
                            "user": user,
                            "client_ip": host,
                            "database": "N/A", # General log không cung cấp DB, sẽ được cập nhật bởi 'USE db'
                            "query": str(row_dict['query_text']),
                            "source_dbms": "MySQL"
                        }
                        new_records_to_publish.append(record)
                        
                        # Cập nhật timestamp cuối cùng
                        last_event_time_str = event_time_dt.strftime('%Y-%m-%d %H:%M:%S.%f')

                # Đẩy batch vào Redis
                if new_records_to_publish:
                    pipe = redis_client.pipeline()
                    for rec in new_records_to_publish:
                        pipe.xadd(STREAM_KEY, {"data": json.dumps(rec)})
                    pipe.execute()
                    
                    logging.info(f"Đã đẩy {len(new_records_to_publish)} bản ghi mới vào Redis.")
                    
                    # Ghi state MỚI NHẤT vào file
                    write_last_known_timestamp(last_event_time_str)

            except RedisConnectionError as e:
                logging.error(f"Mất kết nối Redis (Publisher): {e}. Đang kết nối lại...")
                redis_client = connect_redis() # Thử kết nối lại
            
            except (OperationalError, ProgrammingError) as e:
                logging.error(f"Lỗi CSDL MySQL: {e}. Đang kết nối lại...")
                db_engine = connect_db(MYSQL_LOG_DATABASE_URL) # Thử kết nối lại
            
            except Exception as e:
                logging.error(f"Lỗi không xác định: {e}", exc_info=True)

            # Nghỉ 2 giây trước khi poll tiếp
            time.sleep(poll_interval_sec)

    except KeyboardInterrupt:
        logging.info("Đã nhận tín hiệu (Ctrl+C). Publisher đang dừng...")
    
    finally:
        logging.info("TablePublisher đã dừng.")


if __name__ == "__main__":
    # Đảm bảo DATABASE_URL trỏ đến CSDL MySQL (không phải CSDL chính của app)
    # Bạn có thể cần một DATABASE_URL_LOGS riêng
    monitor_log_table()