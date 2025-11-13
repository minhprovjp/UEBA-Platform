<<<<<<< Updated upstream
# engine/postgres_parser.py
=======
# engine/postgres_log_parser.py
import logging
>>>>>>> Stashed changes
import pandas as pd
import os
import sys
import csv
import re
import json
import argparse # <--- THÊM IMPORT NÀY
from datetime import datetime

# Thêm thư mục gốc để import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Không cần import config nữa vì các đường dẫn sẽ được truyền qua tham số

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
                        'query': message.removeprefix("statement: ").strip()
                    })
    except Exception as e:
        print(f"    -> Lỗi khi đọc file '{os.path.basename(log_path)}': {e}")
    return parsed_records

# --- HÀM CHÍNH ĐÃ ĐƯỢC NÂNG CẤP ---
def run_postgres_parser(source_log_dir, output_csv_path, state_file_path):
    """
    Hàm chính: Quét thư mục log, phân tích file mới/thay đổi, và ghi đè file CSV đầu ra.
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
        current_size = os.path.getsize(full_path)
        last_processed_size = processed_files_state.get(filename, 0)
        
        if current_size > last_processed_size:
            new_records = parse_single_log_file(full_path, start_byte=last_processed_size)
            if new_records:
                all_new_records.extend(new_records)
            processed_files_state[filename] = current_size

    if not all_new_records:
        print("Không có dữ liệu log mới nào được tìm thấy.")
        return

    df_new = pd.DataFrame(all_new_records)
    df_new['timestamp'] = pd.to_datetime(df_new['timestamp'], utc=True, errors='coerce')
    df_new.dropna(subset=['timestamp'], inplace=True)
    
    if df_new.empty:
        print("Không có bản ghi hợp lệ nào sau khi xử lý.")
        write_parser_state(state, state_file_path)
        return
        
    # === SỬA ĐỔI LOGIC GHI FILE: GHI ĐÈ THAY VÌ NỐI TIẾP ===
    # Đọc file CSV cũ (nếu có)
    try:
        df_old = pd.read_csv(output_csv_path)
        # Chuyển đổi timestamp của file cũ để đảm bảo có thể nối
        df_old['timestamp'] = pd.to_datetime(df_old['timestamp'], utc=True, errors='coerce')
        # Nối dữ liệu cũ và mới
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
    except FileNotFoundError:
        # Nếu file cũ không tồn tại, file mới chính là file kết hợp
        df_combined = df_new

    # Xử lý trùng lặp và sắp xếp lại
    df_combined.drop_duplicates(subset=['timestamp', 'user', 'query'], keep='last', inplace=True)
    df_combined.sort_values(by='timestamp', inplace=True)

    # Tự động tạo thư mục đầu ra
    output_dir = os.path.dirname(output_csv_path)
    os.makedirs(output_dir, exist_ok=True)
    
    # Ghi đè toàn bộ file CSV với dữ liệu đã được kết hợp và làm sạch
    df_combined.to_csv(output_csv_path, index=False, encoding='utf-8')
    
    # Lưu lại trạng thái mới nhất
    write_parser_state(state, state_file_path)
    
    print(f"--- Hoàn thành ---")
    print(f"Phân tích log PostgreSQL thành công. File '{os.path.basename(output_csv_path)}' hiện có {len(df_combined)} dòng.")

# --- ĐIỂM KHỞI ĐỘNG CỦA SCRIPT ---
if __name__ == "__main__":
    # Thiết lập argparse để nhận tham số từ dòng lệnh
    parser = argparse.ArgumentParser(description="Phân tích log PostgreSQL một cách tăng dần.")
    parser.add_argument("--input", type=str, required=True, help="Đường dẫn đến THƯ MỤC chứa file log của PostgreSQL.")
    parser.add_argument("--output", type=str, required=True, help="Đường dẫn đến file CSV đầu ra.")
    args = parser.parse_args()
    
    # Tạo đường dẫn file trạng thái dựa trên file output
    state_file = os.path.join(os.path.dirname(args.output), ".postgres_parser_state.json")
    
    # Gọi hàm chính với các tham số đã nhận
    run_postgres_parser(args.input, args.output, state_file)