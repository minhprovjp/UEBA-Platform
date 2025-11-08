# engine/mysql_log_publisher.py
import os, json, time, threading, logging, re, sys
import pandas as pd
from datetime import datetime, timezone
from redis import Redis
from sqlalchemy.engine.url import make_url 
from typing import Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import *
    from engine.utils import save_logs_to_parquet 

    try:
        # Đọc URL của publisher kia để biết nó dùng user nào
        db_url = make_url(MYSQL_LOG_DATABASE_URL)
        FILTER_USER = db_url.username
        logging.info(f"[MySQLPublisher] Sẽ lọc (ignore) tất cả hành vi của user: '{FILTER_USER}'")
    except Exception as e:
        logging.error(f"[MySQLPublisher] Không thể parse MYSQL_LOG_DATABASE_URL để tìm user. Sẽ không lọc. Lỗi: {e}")
        FILTER_USER = None

except ImportError:
    print("Lỗi: Không thể import 'config' hoặc 'engine.utils'.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [MySQLPublisher] - %(message)s"
)

STREAM_KEY = None # Sẽ được set khi chạy

# --- HÀM KẾT NỐI REDIS TIN CẬY ---
def connect_redis():
    """Kết nối đến Redis với cơ chế thử lại vô hạn."""
    while True:
        try:
            r = Redis.from_url(REDIS_URL, decode_responses=True)
            r.ping()
            logging.info("Kết nối Redis (Publisher) thành công.")
            return r
        except ConnectionError as e:
            logging.error(f"Kết nối Redis (Publisher) thất bại: {e}. Thử lại sau 5 giây...")
            time.sleep(5)

# ==============================================================================
# LOGIC TỪ MYSQL_LOG_PARSER.PY
# ==============================================================================

mysql_connect_regex = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z)\s+(?P<thread_id>\d+)\s+Connect\s+(?P<user>[^@]+)@(?P<host>[^\s]+)\s+on(?:\s+(?P<db>\S+))?\s+using.*$"
)
mysql_query_regex = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z)\s+(?P<thread_id>\d+)\s+Query\s+(?P<query>.+)$"
)
mysql_init_db_regex = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z)\s+(?P<thread_id>\d+)\s+Init DB\s+(?P<db_name>\S+)"
)

def read_last_known_state(state_file_path=MYSQL_STATE_FILE_PATH):
    """Đọc state (vị trí byte, sessions) từ file JSON."""
    try:
        with open(state_file_path, 'r', encoding='utf-8') as f:
            state = json.load(f)
            return state.get("last_size", 0), state.get("sessions", {})
    except (FileNotFoundError, json.JSONDecodeError):
        logging.warning(f"Không tìm thấy state file. Bắt đầu từ đầu.")
        return 0, {}

def write_last_known_state(size, sessions, state_file_path=MYSQL_STATE_FILE_PATH):
    """Ghi state (vị trí byte, sessions) ra file JSON."""
    state = {"last_size": size, "sessions": sessions}
    os.makedirs(os.path.dirname(state_file_path) or ".", exist_ok=True)
    try:
        with open(state_file_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logging.error(f"Không thể ghi state file: {e}")

def parse_and_append_log_data(new_lines, sessions, filter_user=None):
    """
    Hàm parse lõi. Nhận dòng log mới và dict sessions hiện tại,
    trả về DataFrame các query đã parse và dict sessions đã cập nhật.
    """
    parsed_data = []
    current_multiline_query_parts = []
    last_query_timestamp, last_query_thread_id = None, None

    def process_complete_query():
        nonlocal current_multiline_query_parts, last_query_timestamp, last_query_thread_id
        if current_multiline_query_parts and last_query_thread_id in sessions:
            full_query = "\n".join(current_multiline_query_parts).strip()
            session_info = sessions[last_query_thread_id]
            use_db_match = re.match(r"USE\s+`?([\w-]+)`?", full_query, re.IGNORECASE)
            if use_db_match:
                sessions[last_query_thread_id]['db'] = use_db_match.group(1)

            parsed_data.append({
                'timestamp': pd.to_datetime(last_query_timestamp, utc=True),
                'user': session_info.get('user'),
                'client_ip': session_info.get('host'),
                'database': session_info.get('db', 'N/A'),
                'query': full_query,
                'source_dbms': 'MySQL_GQL_File'
            })
        current_multiline_query_parts.clear()
        last_query_timestamp, last_query_thread_id = None, None

    for raw in new_lines:
        line = raw.strip()
        connect_match = mysql_connect_regex.match(line)
        query_start_match = mysql_query_regex.match(line)
        init_db_match = mysql_init_db_regex.match(line)
        quit_match = re.match(r"^\d{4}.*?\s+(?P<thread_id>\d+)\s+Quit", line)

        is_new_command_start = bool(connect_match or query_start_match or init_db_match or quit_match)
        if is_new_command_start:
            process_complete_query()

        if connect_match:
            data = connect_match.groupdict()
            connected_user = data['user']
            if filter_user and connected_user == filter_user:
                logging.debug(f"Bỏ qua session 'Connect' từ user bị lọc: {connected_user}")
                continue # Bỏ qua, không thêm session này vào 'sessions'
            db_for_session = data.get('db') if data.get('db') else 'N/A'
            sessions[data['thread_id']] = {
                'user': connected_user,
                'host': data['host'],
                'db': db_for_session
            }
        elif init_db_match:
            data = init_db_match.groupdict()
            if data['thread_id'] in sessions:
                sessions[data['thread_id']]['db'] = data['db_name']
        elif query_start_match:
            data = query_start_match.groupdict()
            last_query_timestamp, last_query_thread_id = data['timestamp'], data['thread_id']
            current_multiline_query_parts.append(data['query'])
        elif quit_match:
            tid = quit_match.group('thread_id')
            if tid in sessions:
                sessions.pop(tid, None)
        elif not is_new_command_start and current_multiline_query_parts:
            # Đây là dòng tiếp theo của một query đa dòng
            if line:
                # Giữ nguyên `raw` để giữ thụt đầu dòng
                current_multiline_query_parts.append(raw) 

    process_complete_query() # Xử lý query cuối cùng (nếu có)
    return pd.DataFrame(parsed_data), sessions

# ==============================================================================
# LOGIC PUBLISHER
# ==============================================================================

def _open_log(path: str):
    """Mở file log ở chế độ text, ignore lỗi mã hoá."""
    return open(path, "r", encoding="utf-8", errors="ignore")

def _jsonify_record(rec: dict) -> str:
    """Đảm bảo timestamp JSON-serializable."""
    r = dict(rec)
    ts = r.get("timestamp")
    if isinstance(ts, pd.Timestamp):
        # chuẩn hoá ISO UTC để consumer parse ổn định
        r["timestamp"] = ts.tz_convert("UTC").isoformat()
    return json.dumps(r, ensure_ascii=False)

def monitor_log_file(
    source_log_path: str,
    redis_url: str,
    stream_key: str,
    state_file_path: str = MYSQL_STATE_FILE_PATH,
    poll_interval_ms: int = 200,          # Tần suất "nhìn" file
    stop_event: Optional[threading.Event] = None,
    filter_user: Optional[str] = None
):
    """
    Theo dõi file log, parse và publish realtime vào Redis Streams.
    Tự xử lý "catch-up" khi khởi động.
    """
    r = connect_redis()

    os.makedirs(os.path.dirname(source_log_path) or ".", exist_ok=True)
    os.makedirs(STAGING_DATA_DIR, exist_ok=True)

    # Trạng thái đọc (từ file JSON)
    last_size, sessions = read_last_known_state(state_file_path)
    published_total = 0
    f = None
    
    if filter_user and sessions:
        initial_count = len(sessions)
        sessions = {tid: s for tid, s in sessions.items() if s.get('user') != filter_user}
        filtered_count = initial_count - len(sessions)
        if filtered_count > 0:
            logging.info(f"Đã lọc {filtered_count} session đang hoạt động của user '{filter_user}' từ file state.")
            
    logging.info(f"Start monitoring {source_log_path} → {stream_key} (poll={poll_interval_ms}ms)")
    logging.info("Publisher đang chạy... Nhấn Ctrl+C để dừng.")

    try:
        # Mở file & seek đến vị trí đã đọc
        try:
            f = _open_log(source_log_path)
        except FileNotFoundError:
            logging.warning(f"Log chưa sẵn sàng: {source_log_path}. Sẽ đợi file xuất hiện...")
            f = None

        if f:
            try:
                # Đây chính là logic "CATCH-UP"
                f.seek(last_size)
                logging.info(f"Đã seek đến byte {last_size} để bắt đầu (catch-up).")
            except Exception:
                f.seek(0)
                last_size = 0
                
        while True:
            if stop_event and stop_event.is_set():
                logging.info("Stop signal received. Exiting monitor loop...")
                break
            try:
                # (Re)open if needed
                if f is None:
                    try:
                        f = _open_log(source_log_path)
                        f.seek(last_size)  # Tiếp tục từ vị trí đã biết
                        logging.info(f"File opened. Resuming from byte {last_size}.")
                    except FileNotFoundError:
                        time.sleep(poll_interval_ms / 1000.0)
                        continue

                # Kiểm tra rotation
                try:
                    current_size = os.path.getsize(source_log_path)
                except FileNotFoundError:
                    logging.warning("Log file missing, waiting to reappear...")
                    try: f.close()
                    except Exception: pass
                    f = None
                    time.sleep(poll_interval_ms / 1000.0)
                    continue
                
                try:
                    current_pos = f.tell()
                except Exception:
                    current_pos = last_size # Fallback

                if current_size < current_pos:
                    logging.warning("Detected rotation/truncate → reopen & reset sessions.")
                    try: f.close()
                    except Exception: pass
                    f = _open_log(source_log_path)
                    f.seek(0)
                    last_size = 0
                    sessions = {}

                # Đọc phần mới
                new_lines = f.readlines()
                if new_lines:
                    try:
                        last_size = f.tell() # Cập nhật last_size ngay sau khi đọc
                    except Exception:
                        last_size = current_size # Fallback

                    # Parse & publish
                    df_new, sessions = parse_and_append_log_data(new_lines, sessions, filter_user)
                    if not df_new.empty:
                        pipe = r.pipeline()
                        recs = df_new.to_dict(orient="records")
                        
                        try:
                            # Đặt tên nguồn log rõ ràng để phân biệt
                            save_logs_to_parquet(recs, source_dbms="MySQL_GQL_File") 
                            logging.info(f"Đã lưu {len(recs)} bản ghi GQL File vào Staging.")
                        except Exception as e:
                            logging.error(f"Lỗi khi lưu file Parquet (Staging): {e}", exc_info=True)
                            
                        for rec in recs:
                            # 1. Gửi đến Redis Stream (Luồng nóng)
                            pipe.xadd(stream_key, {"data": _jsonify_record(rec)})
                        pipe.execute()
                        published_total += len(recs)
                    
                    # Ghi state sau mỗi đợt đọc thành công
                    write_last_known_state(last_size, sessions, state_file_path)

                else:
                    # không có dòng mới
                    time.sleep(poll_interval_ms / 1000.0)
            
            except ConnectionError as e:
                logging.error(f"Mất kết nối Redis (Publisher): {e}. Đang kết nối lại...")
                r = connect_redis()
                
            except Exception as e:
                logging.error(f"Lỗi không xác định trong publisher loop: {e}", exc_info=True)
                time.sleep(5)

    except KeyboardInterrupt:
        logging.info("Đã nhận tín hiệu (Ctrl+C). Publisher đang dừng...")
        
    finally:
        if f:
            f.close()
        
        logging.info(f"Publisher đã dừng. Tổng số đã publish: {published_total}")

def start_mysql_publisher_blocking():
    """Entry chạy đơn giản cho CLI."""
    global STREAM_KEY
    STREAM_KEY = f"{REDIS_STREAM_LOGS}:mysql"
    monitor_log_file(
        source_log_path=SOURCE_MYSQL_LOG_PATH,
        redis_url=REDIS_URL,
        stream_key=STREAM_KEY,
        state_file_path=MYSQL_STATE_FILE_PATH,
        poll_interval_ms=200,
        stop_event=None,
        filter_user=FILTER_USER
    )

if __name__ == "__main__":
    start_mysql_publisher_blocking()