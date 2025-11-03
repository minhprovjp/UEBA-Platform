# engine/mysql_log_parser.py
import re
import json
import os
import argparse
import pandas as pd
from datetime import datetime, timezone
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    SOURCE_MYSQL_LOG_PATH,
    PARSED_MYSQL_LOG_PARQUET_PATH,
    MYSQL_STATE_FILE_PATH,
    STAGING_DATA_DIR,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [MySQLParser] - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])

# --- REGEX: sửa CONNECT để cho phép DB trống ("on  using ...")
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
    try:
        with open(state_file_path, 'r', encoding='utf-8') as f:
            state = json.load(f)
            return state.get("last_size", 0), state.get("sessions", {})
    except (FileNotFoundError, json.JSONDecodeError):
        return 0, {}

def write_last_known_state(size, sessions, state_file_path=MYSQL_STATE_FILE_PATH):
    state = {"last_size": size, "sessions": sessions}
    os.makedirs(os.path.dirname(state_file_path) or ".", exist_ok=True)
    with open(state_file_path, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)

def parse_and_append_log_data(new_lines, sessions):
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
                'source_dbms': 'MySQL'
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
            db_for_session = data.get('db') if data.get('db') else 'N/A'
            sessions[data['thread_id']] = {
                'user': data['user'],
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
            if line:
                current_multiline_query_parts.append(raw)

    process_complete_query()
    return pd.DataFrame(parsed_data), sessions

def _emit_to_staging(df_new: pd.DataFrame) -> str | None:
    """Ghi batch mới ra STAGING (engine sẽ nhặt)."""
    try:
        os.makedirs(STAGING_DATA_DIR, exist_ok=True)
        run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(STAGING_DATA_DIR, f"mysql_general_{run_ts}.parquet")
        df_new.to_parquet(out_path, engine='pyarrow', index=False)
        logging.info(f"Đã ghi batch mới vào STAGING: {out_path}")
        return out_path
    except Exception as e:
        logging.error(f"Lỗi khi ghi Parquet vào STAGING: {e}")
        # fallback CSV để debug nhanh
        try:
            csv_path = out_path.replace(".parquet", ".csv")
            df_new.to_csv(csv_path, index=False, encoding='utf-8')
            logging.warning(f"Fallback CSV -> {csv_path}")
            return csv_path
        except Exception as e2:
            logging.error(f"Fallback CSV cũng lỗi: {e2}")
            return None

def run_parser(source_log_path, output_parquet_path, state_file_path, reset_state=False):
    if not os.path.exists(source_log_path):
        logging.error(f"File log nguồn không tồn tại: {source_log_path}")
        return

    os.makedirs(os.path.dirname(output_parquet_path) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(state_file_path) or ".", exist_ok=True)

    if reset_state and os.path.exists(state_file_path):
        os.remove(state_file_path)
        logging.warning("Đã xoá state file theo yêu cầu --reset-state")

    last_size, sessions = read_last_known_state(state_file_path)
    current_size = os.path.getsize(source_log_path)

    # phát hiện log rotation
    if current_size < last_size:
        logging.warning("Phát hiện log rotation (file nhỏ hơn trước) → reset state.")
        last_size, sessions = 0, {}

    if current_size == last_size:
        logging.info("Không có dữ liệu log mới.")
        return

    logging.info(f"Đọc dữ liệu mới từ byte {last_size} đến {current_size}...")
    with open(source_log_path, 'r', encoding='utf-8', errors='ignore') as f:
        f.seek(last_size)
        new_lines = f.readlines()

    if not new_lines:
        write_last_known_state(current_size, sessions, state_file_path)
        return

    df_new, updated_sessions = parse_and_append_log_data(new_lines, sessions)
    if df_new.empty:
        logging.info("Không có truy vấn hợp lệ trong batch mới.")
        write_last_known_state(current_size, updated_sessions, state_file_path)
        return

    # 1) phát “batch mới” ra STAGING cho engine
    _emit_to_staging(df_new)

    # 2) (tuỳ chọn) cập nhật “master” gộp
    try:
        if os.path.exists(output_parquet_path):
            df_old = pd.read_parquet(output_parquet_path, engine='pyarrow')
            if 'timestamp' in df_old.columns:
                df_old['timestamp'] = pd.to_datetime(df_old['timestamp'], utc=True, errors='coerce')
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_combined = df_new

        df_combined.drop_duplicates(subset=['timestamp', 'user', 'query'], keep='last', inplace=True)
        df_combined.sort_values(by='timestamp', inplace=True, ignore_index=True)
        df_combined.to_parquet(output_parquet_path, engine='pyarrow', index=False)
        logging.info(f"Master parquet hiện có {len(df_combined)} dòng.")
    except Exception as e:
        logging.error(f"Lỗi khi cập nhật master parquet: {e}")

    write_last_known_state(current_size, updated_sessions, state_file_path)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="catch-up")           # để engine truyền vào, hiện chưa dùng
    ap.add_argument("--source", default=SOURCE_MYSQL_LOG_PATH)
    ap.add_argument("--reset-state", action="store_true")
    args = ap.parse_args()

    run_parser(
        source_log_path=args.source,
        output_parquet_path=PARSED_MYSQL_LOG_PARQUET_PATH,
        state_file_path=MYSQL_STATE_FILE_PATH,
        reset_state=args.reset_state,
    )
