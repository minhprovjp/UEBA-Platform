# engine/postgres_log_parser.py
import pandas as pd
import os
import sys
import csv
import re
import json
import argparse
from datetime import datetime
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import *
    from engine.utils import save_logs_to_parquet
except ImportError:
    print("Lỗi: Không thể import 'config' hoặc 'engine.utils'.")
    sys.exit(1)

# --- CÁC HÀM QUẢN LÝ TRẠNG THÁI (Không đổi) ---
def read_parser_state(state_file_path):
    try:
        if os.path.exists(state_file_path):
            with open(state_file_path, 'r') as f:
                return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        pass
    return {"processed_files": {}, "last_run_utc": None}

def write_parser_state(state, state_file_path):
    state['last_run_utc'] = datetime.utcnow().isoformat() + "Z"
    with open(state_file_path, 'w') as f:
        json.dump(state, f, indent=4)

# --- HÀM PHÂN TÍCH FILE LOG (Không đổi) ---
def parse_single_log_file(log_path, start_byte=0):
    parsed_records = []
    print(f"  -> Đang xử lý file: {os.path.basename(log_path)} từ byte {start_byte}...")
    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            f.seek(start_byte)
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 14: continue
                message = row[13]
                if message.startswith("statement: "):
                    log_time_str = re.sub(r' [A-Z]{3}$', '', row[0])
                    parsed_records.append({
                        'timestamp': log_time_str,
                        'user': row[1],
                        'client_ip': row[4].split(':')[0] if row[4] else 'N/A',
                        'database': row[2],
                        'query': message.removeprefix("statement: ").strip(),
                        'source_dbms': 'PostgreSQL' # Thêm nguồn
                    })
    except Exception as e:
        print(f"    -> Lỗi khi đọc file '{os.path.basename(log_path)}': {e}")
    return parsed_records

# --- HÀM CHÍNH ĐÃ ĐƯỢC NÂNG CẤP ---
def run_postgres_parser(source_log_dir, state_file_path):
    """
    Hàm chính: Quét thư mục log, phân tích file mới/thay đổi,
    VÀ GHI RA STAGING PARQUET.
    """
    if not os.path.isdir(source_log_dir):
        print(f"Lỗi: Thư mục log nguồn của PostgreSQL không tồn tại tại '{source_log_dir}'")
        return

    print(f"--- Bắt đầu quét thư mục log của PostgreSQL: {source_log_dir} ---")
    
    state = read_parser_state(state_file_path)
    processed_files_state = state.get("processed_files", {})
    all_new_records = []

    try:
        log_files = sorted([f for f in os.listdir(source_log_dir) if f.endswith(('.log', '.csv'))])
    except FileNotFoundError:
        print(f"Lỗi: Không thể truy cập thư mục log '{source_log_dir}'. Kiểm tra lại quyền.")
        return

    for filename in log_files:
        full_path = os.path.join(source_log_dir, filename)
        try:
            current_size = os.path.getsize(full_path)
        except FileNotFoundError:
            continue # File đã bị xoá
            
        last_processed_size = processed_files_state.get(filename, 0)
        
        if current_size > last_processed_size:
            new_records = parse_single_log_file(full_path, start_byte=last_processed_size)
            if new_records:
                all_new_records.extend(new_records)
            processed_files_state[filename] = current_size
        elif current_size < last_processed_size:
             # File đã bị xoay vòng (rotated) hoặc cắt bớt
            logging.warning(f"Phát hiện xoay vòng file log: {filename}. Đọc lại từ đầu.")
            new_records = parse_single_log_file(full_path, start_byte=0)
            if new_records:
                all_new_records.extend(new_records)
            processed_files_state[filename] = current_size


    if not all_new_records:
        print("Không có dữ liệu log mới nào được tìm thấy.")
        write_parser_state(state, state_file_path) # Vẫn lưu state (ví dụ: last_run_utc)
        return

    # === LOGIC GHI FILE ĐÃ THAY ĐỔI ===
    # Thay vì đọc/ghi file CSV lớn, chỉ cần ghi batch mới ra staging
    num_saved = save_logs_to_parquet(all_new_records, source_dbms="PostgreSQL")
    
    if num_saved > 0:
        # Lưu lại trạng thái mới nhất
        write_parser_state(state, state_file_path)
        print(f"--- Hoàn thành ---")
        print(f"Đã phân tích và lưu {num_saved} bản ghi PostgreSQL mới vào thư mục staging.")
    else:
        print("Đã phân tích log nhưng không lưu được file parquet.")

# --- ĐIỂM KHỞI ĐỘNG CỦA SCRIPT ---
if __name__ == "__main__":
    # Sử dụng config.py thay vì tham số dòng lệnh
    run_postgres_parser(
        source_log_dir=SOURCE_POSTGRES_LOG_PATH,
        state_file_path=POSTGRES_STATE_FILE_PATH
    )