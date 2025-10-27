# log_parser.py
"""
================================================================================
MODULE PHÂN TÍCH LOG TĂNG DẦN (INCREMENTAL PARSER)
================================================================================
Đây là một script độc lập, được thiết kế để chạy định kỳ (ví dụ, qua cron job
hoặc scheduler). Nhiệm vụ chính của nó là đọc file general log thô của MySQL
một cách thông minh: nó sẽ "nhớ" vị trí đã đọc lần cuối và chỉ xử lý các
dòng log mới, sau đó ghi nối tiếp vào file CSV đầu ra.

Luồng hoạt động chính:
1.  **Xác thực tính toàn vẹn:** Kiểm tra xem các file trạng thái có đồng bộ không,
    đường dẫn log nguồn có thay đổi không, và có hiện tượng log rotation không.
    Nếu có vấn đề, nó sẽ thực hiện "Hard Reset" để bắt đầu lại từ đầu.
2.  **Đọc phần mới của file log:** So sánh kích thước file hiện tại và lần cuối,
    sau đó chỉ đọc phần dữ liệu mới được thêm vào.
3.  **Phân tích và chuyển đổi:** Chuyển đổi các dòng log mới thành dữ liệu có cấu trúc.
4.  **Ghi nối tiếp:** Ghi các bản ghi mới vào cuối file CSV đã có.
5.  **Cập nhật trạng thái:** Lưu lại kích thước file và thông tin session mới nhất
    để chuẩn bị cho lần chạy tiếp theo.
"""

# Import các thư viện cần thiết
import logging
import re
import csv
import json
import os
import shutil
import argparse
import pandas as pd
from datetime import datetime
import sys
# Thêm thư mục gốc để import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.config import settings 

# ==============================================================================
# I. CÁC BIỂU THỨC CHÍNH QUY (REGEX)
# ==============================================================================
# Các regex này được biên dịch sẵn (`re.compile`) để tăng hiệu suất.
# Chúng được dùng để bóc tách thông tin từ các dòng log có định dạng cụ thể.

# Regex cho dòng `Connect`: Lấy timestamp, thread_id, user, host, እና db (nếu có).
mysql_connect_regex = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z)\s+"
    r"(?P<thread_id>\d+)\s+Connect\s+"
    r"(?P<user>[^@]+)@(?P<host>[^\s]+)\s+on\s+"
    # Group 'db': khớp với các ký tự không phải khoảng trắng NẾU có.
    # Nếu sau 'on ' là khoảng trắng ngay (ví dụ 'on  using...'), group này sẽ không khớp gì cả.
    r"(?P<db>\S+)?\s+"  # Khớp tên DB (nếu có), theo sau là ít nhất một khoảng trắng.
                        # Dấu ? làm cho (?P<db>\S+) là tùy chọn.
    r"using.*$"        # Phần còn lại của dòng phải bắt đầu bằng "using"
)
# Regex cho dòng Query của MySQL
mysql_query_regex = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z)\s+"
    r"(?P<thread_id>\d+)\s+Query\s+(?P<query>.+)$"
)
# Regex cho dòng Init DB của MySQL (khi user kết nối và chỉ định DB)
# 2023-11-21T12:34:56.123456Z	   15 Init DB	mini_capstone_db
mysql_init_db_regex = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z)\s+"
    r"(?P<thread_id>\d+)\s+Init DB\s+(?P<db_name>\S+)"
)


# ==============================================================================
# II. CÁC HÀM QUẢN LÝ TRẠNG THÁI VÀ METADATA
# ==============================================================================
# Các hàm trong mục này chịu trách nhiệm đọc/ghi các file phụ trợ
# (.parser_state, .meta) để duy trì trạng thái và thông tin của quá trình parse.

def read_last_known_state():
    """Đọc trạng thái lần cuối từ file .parser_state."""
    try:
        with open(STATE_FILE_PATH, 'r') as f:
            last_size = int(f.readline().strip()) # Đọc dòng đầu tiên cho size
            sessions_str = f.read() # Đọc phần còn lại cho session dictionary
            if sessions_str:
                sessions = json.loads(sessions_str)
            else:
                sessions = {}
            return last_size, sessions
    except (FileNotFoundError, ValueError, IndexError, json.JSONDecodeError):
        # Nếu file không tồn tại, rỗng, hoặc bị hỏng, trả về trạng thái ban đầu.
        return 0, {}

def write_last_known_state(size, sessions):
    """Ghi kích thước và session dictionary hiện tại vào file .parser_state."""
    with open(STATE_FILE_PATH, 'w') as f:
        f.write(f"{size}\n") # Ghi kích thước file ở dòng đầu tiên
        f.write(json.dumps(sessions, indent=2)) # Ghi dictionary session dưới dạng JSON

def update_metadata_file(output_csv_path, source_log_path, source_log_size):
    """Đọc file CSV, tính toán metadata và ghi vào file .meta một cách an toàn."""
    try:
        if not os.path.exists(output_csv_path) or os.path.getsize(output_csv_path) == 0:
            return

        # === SỬA ĐỔI QUAN TRỌNG: ĐỊNH RÕ CÁCH PARSE NGÀY THÁNG ===
        # Bắt buộc Pandas phải đọc cột 'timestamp' như là datetime
        # và tự động suy ra định dạng. `utc=True` đảm bảo nó là timezone-aware.
        df = pd.read_csv(output_csv_path, parse_dates=['timestamp'])
        
        if df.empty or 'timestamp' not in df.columns:
            return
            
        # Đảm bảo cột timestamp là timezone-aware (UTC)
        if df['timestamp'].dt.tz is None:
            # Nếu nó là naive, giả định là UTC
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        else:
            # Nếu đã có timezone, chuyển nó về UTC để chuẩn hóa
            df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')

        # Lấy giá trị min/max sau khi đã chuẩn hóa
        min_ts = df['timestamp'].min()
        max_ts = df['timestamp'].max()

        metadata = {
            "source_log_path": source_log_path,
            "last_updated_utc": datetime.utcnow().isoformat() + "Z",
            "parser_version": "1.2", # Tăng phiên bản
            "source_log_size_bytes": source_log_size,
            "total_records_in_csv": len(df),
            # .isoformat() trên datetime timezone-aware sẽ bao gồm thông tin múi giờ
            "timestamp_start_in_csv": min_ts.isoformat(),
            "timestamp_end_in_csv": max_ts.isoformat()
        }

        meta_file_path = output_csv_path + ".meta"
        with open(meta_file_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=4)
        logging.info("Metadata file updated successfully.")

    except Exception as e:
        logging.info(f"Error updating metadata file: {e}")

# ==============================================================================
# III. CÁC HÀM XÁC THỰC TÍNH TOÀN VẸN
# ==============================================================================
# Các hàm này được dùng để kiểm tra và đảm bảo rằng trạng thái của hệ thống
# là hợp lệ trước khi bắt đầu một phiên phân tích mới.

def get_timestamp_from_line(line, regex_list):
    """Thử khớp một dòng với danh sách regex và trả về timestamp nếu thành công."""
    for regex in regex_list:
        match = regex.match(line)
        if match:
            # Chuyển chuỗi ISO 8601 (với Z) thành đối tượng datetime timezone-aware (UTC)
            return datetime.fromisoformat(match.group('timestamp').replace('Z', '+00:00'))
    return None

def get_first_timestamp_from_log(log_path, regex_list):
    """Đọc và lấy timestamp hợp lệ đầu tiên từ file log."""
    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f: # Đọc từng dòng từ đầu
                ts = get_timestamp_from_line(line.strip(), regex_list)
                if ts:
                    return ts # Trả về ngay khi tìm thấy
    except Exception as e:
        logging.info(f"Lỗi khi đọc timestamp đầu tiên từ log: {e}")
    return None

def get_last_timestamp_from_log(log_path, regex_list):
    """Đọc và lấy timestamp hợp lệ cuối cùng từ file log một cách hiệu quả."""
    try:
        with open(log_path, 'rb') as f: # Mở ở chế độ binary để seek
            f.seek(0, os.SEEK_END)
            buffer_size = 8192 # Đọc 8KB cuối cùng
            offset = max(0, f.tell() - buffer_size)
            f.seek(offset, os.SEEK_SET)
            buffer = f.read().decode('utf-8', errors='ignore')
            lines = buffer.strip().splitlines()
            
            for line in reversed(lines): # Đọc ngược từ cuối lên
                ts = get_timestamp_from_line(line.strip(), regex_list)
                if ts:
                    return ts # Trả về ngay khi tìm thấy
    except Exception as e:
        logging.info(f"Lỗi khi đọc timestamp cuối cùng từ log: {e}")
    return None

# Hàm helper chuyển dữ liệu vào file mismatch_archives khi dữ liệu của file csv và file log không trùng khớp
def perform_hard_reset(reason: str):
    """
    Thực hiện "Hard Reset": Lưu trữ file CSV để đối chiếu và xóa các file trạng thái.
    """
    logging.info(f"!!! CẢNH BÁO: {reason}")
    logging.info("!!! Thực hiện Hard Reset và Lưu trữ để Đối chiếu...")
    reason_slug = re.sub(r'[^\w-]', '_', reason.lower()).split(':')[0]
    try:
        os.makedirs(settings.MYSQL_MISMATCH_ARCHIVE_DIR, exist_ok=True)
    except OSError as e:
        logging.info(f"    -> LỖI: Không thể tạo thư mục lưu trữ '{settings.MYSQL_MISMATCH_ARCHIVE_DIR}': {e}")
        pass
    try:
        if os.path.exists(PARSED_MYSQL_LOG_FILE_PATH):
            timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            new_filename = f"{timestamp_str}_{reason_slug}.csv"
            destination_path = os.path.join(settings.MYSQL_MISMATCH_ARCHIVE_DIR, new_filename)
            shutil.move(PARSED_MYSQL_LOG_FILE_PATH, destination_path)
            logging.info(f"    -> Đã lưu trữ: {os.path.basename(PARSED_MYSQL_LOG_FILE_PATH)} -> {new_filename}")
    except Exception as e:
        logging.info(f"    -> LỖI khi lưu trữ file CSV: {e}")
    files_to_delete = [META_FILE_PATH, STATE_FILE_PATH]
    for file_path in files_to_delete:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info(f"    -> Đã xóa để reset: {os.path.basename(file_path)}")
        except Exception as e:
            logging.info(f"    -> LỖI khi xóa {os.path.basename(file_path)}: {e}")

# ==============================================================================
# IV. LOGIC PHÂN TÍCH CỐT LÕI
# ==============================================================================

def parse_and_append_log_data(new_lines, sessions):
    """
    Nhận các dòng log MỚI và dictionary session hiện tại, phân tích chúng,
    và trả về một DataFrame mới cùng với dictionary session đã được cập nhật.
    """
    parsed_data = []
    current_multiline_query_parts = []
    last_query_timestamp, last_query_thread_id = None, None

    # Hàm nội bộ để xử lý một query hoàn chỉnh (có thể gồm nhiều dòng)
    def process_complete_query():
        nonlocal current_multiline_query_parts, last_query_timestamp, last_query_thread_id
        # Chỉ xử lý nếu có nội dung và session tương ứng tồn tại
        if current_multiline_query_parts and last_query_thread_id in sessions:
            full_query = "\n".join(current_multiline_query_parts).strip()
            session_info = sessions[last_query_thread_id]
            # Cập nhật database nếu query là lệnh `USE`
            use_db_match_multiline = re.match(r"USE\s+`?([\w-]+)`?", full_query, re.IGNORECASE)
            if use_db_match_multiline:
                db_name_from_use = use_db_match_multiline.group(1)
                sessions[last_query_thread_id]['db'] = db_name_from_use
            # Thêm dữ liệu đã parse vào danh sách
            parsed_data.append({
                'timestamp': pd.to_datetime(last_query_timestamp, utc=True), 
                'user': session_info['user'],
                'client_ip': session_info['host'],
                'database': session_info.get('db', 'N/A'),
                'query': full_query
            })
        # Reset các biến tạm
        current_multiline_query_parts, last_query_timestamp, last_query_thread_id = [], None, None

    # Lặp qua các dòng MỚI đã được đọc từ file log
    for line_content in new_lines:
        line = line_content.strip()
        connect_match = mysql_connect_regex.match(line)
        query_start_match = mysql_query_regex.match(line)
        init_db_match = mysql_init_db_regex.match(line)
        quit_match = re.match(r"^(?P<timestamp>\S+)\s+(?P<thread_id>\d+)\s+Quit", line)
        is_new_command_start = connect_match or query_start_match or init_db_match or quit_match
        
        # Nếu dòng hiện tại là bắt đầu của một lệnh mới, xử lý query đa dòng cũ (nếu có)
        if is_new_command_start: 
            process_complete_query()

        # Xử lý các lệnh mới
        if connect_match:
            data = connect_match.groupdict()
            raw_db_on_connect = data.get('db')
            db_for_session = 'N/A'
            if raw_db_on_connect:
                cleaned_db_name = raw_db_on_connect.strip()
                if cleaned_db_name and cleaned_db_name.lower() != 'null':
                    db_for_session = cleaned_db_name
            sessions[data['thread_id']] = {
                'user': data['user'], 'host': data['host'],
                'db': db_for_session, 'connect_time': data['timestamp']
            }
        elif init_db_match:
            data = init_db_match.groupdict()
            thread_id = data['thread_id']
            if thread_id in sessions:
                sessions[thread_id]['db'] = data['db_name']
        elif query_start_match:
            data = query_start_match.groupdict()
            last_query_timestamp = data['timestamp']
            last_query_thread_id = data['thread_id']
            current_multiline_query_parts.append(data['query'])
        elif quit_match:
            # Khi user thoát, chúng ta có thể xóa session của họ để tiết kiệm bộ nhớ
            thread_id_to_quit = quit_match.group('thread_id')
            if thread_id_to_quit in sessions:
                del sessions[thread_id_to_quit]
        elif not is_new_command_start and current_multiline_query_parts:
            # Đây là phần tiếp theo của một query đa dòng
            if line:
                current_multiline_query_parts.append(line_content) # Giữ nguyên để có newline
    
    # Xử lý query cuối cùng trong file (nếu có) sau khi vòng lặp kết thúc
    process_complete_query()
    return pd.DataFrame(parsed_data), sessions

# ==============================================================================
# V. HÀM CHÍNH (ENTRY POINT CỦA SCRIPT)
# ==============================================================================

def run_incremental_parser():
    """Hàm chính để thực thi toàn bộ logic phân tích tăng dần."""
    
    # --- Bước 1: Xác thực tính toàn vẹn ---
    if not args.no_reset_check:
        logging.info("--- Chế độ khởi động: Đang thực hiện kiểm tra toàn vẹn ---")
        meta_exists = os.path.exists(META_FILE_PATH)
        state_exists = os.path.exists(STATE_FILE_PATH)
    
        # Nếu trạng thái không đồng bộ (một file có, một file không), reset tất cả
        if meta_exists != state_exists:
            perform_hard_reset(f"Phát hiện trạng thái không đồng bộ (meta: {meta_exists}, state: {state_exists}).")
            # Sau khi reset, cập nhật lại trạng thái tồn tại của file
            meta_exists = False
            state_exists = False
        
        # Kiểm tra xem đường dẫn log nguồn hoặc timestamp có thay đổi không
        try:
            if meta_exists:
                with open(META_FILE_PATH, 'r') as f:
                    metadata = json.load(f)
                
                last_source_path = metadata.get("source_log_path")
                
                # Kịch bản 1: Đường dẫn file log nguồn đã thay đổi
                if last_source_path != SOURCE_MYSQL_LOG_PATH:
                    perform_hard_reset(f"Phát hiện file log nguồn thay đổi.")
                else:
                    # Kịch bản 2: Đường dẫn khớp, kiểm tra thời gian
<<<<<<< Updated upstream
                    logging.info("Kiểm tra tính toàn vẹn thời gian của dữ liệu...")
=======
                    print("Kiểm tra tính toàn vẹn thời gian của dữ liệu...")
>>>>>>> Stashed changes
                    start_ts_in_csv = pd.to_datetime(metadata.get("timestamp_start_in_csv"), utc=True, errors='coerce')
                    
                    if pd.notna(start_ts_in_csv):
                        regex_patterns = [mysql_connect_regex, mysql_query_regex, mysql_init_db_regex]
                        first_ts_in_log = get_first_timestamp_from_log(SOURCE_MYSQL_LOG_PATH, regex_patterns)
                        
                        if first_ts_in_log and start_ts_in_csv != first_ts_in_log:
                            perform_hard_reset(f"Phát hiện không khớp timestamp bắt đầu.")
        except Exception as e:
            perform_hard_reset(f"Không đọc được file metadata hoặc file bị hỏng ({e}).")

        # Kiểm tra Log Rotation sau các kiểm tra khác
            last_size_check, _ = read_last_known_state()
            if os.path.exists(SOURCE_MYSQL_LOG_PATH):
                current_size_check = os.path.getsize(SOURCE_MYSQL_LOG_PATH)
                if current_size_check < last_size_check:
                    perform_hard_reset("Phát hiện Log Rotation (kích thước file giảm).")
    else:
        logging.info(f"--- Chế độ Real-time: Bỏ qua kiểm tra toàn vẹn ---")
    
    # --- Bước 2: Logic phân tích tăng dần ---
    if not os.path.exists(SOURCE_MYSQL_LOG_PATH):
        logging.info(f"Lỗi: File log nguồn không tồn tại tại '{SOURCE_MYSQL_LOG_PATH}'")
        return

    # Đọc trạng thái lần cuối và lấy kích thước file hiện tại
    last_size, sessions = read_last_known_state()
    current_size = os.path.getsize(SOURCE_MYSQL_LOG_PATH)

    # Xử lý trường hợp Log Rotation
    if current_size < last_size:
        perform_hard_reset("Phát hiện Log Rotation (kích thước file giảm).")

    if current_size == last_size:
        logging.info("Không có dữ liệu log mới.")
        return
    
    start_pos = last_size
    
    logging.info(f"Đọc dữ liệu mới từ byte {start_pos} đến {current_size}...")
    
    # Đọc phần dữ liệu mới từ file log
    with open(SOURCE_MYSQL_LOG_PATH, 'r', encoding='utf-8', errors='ignore') as f:
        f.seek(last_size)
        new_lines = f.readlines()

    if not new_lines:
        logging.info("Không tìm thấy dòng mới hợp lệ.")
        write_last_known_state(current_size, sessions)
        return

    # Phân tích các dòng mới
    df_new, updated_sessions = parse_and_append_log_data(new_lines, sessions)
    
    if df_new.empty:
        logging.info("Không có truy vấn nào được phân tích từ các dòng mới.")
        write_last_known_state(current_size, updated_sessions)
        return

    # Ghi nối tiếp vào file CSV
    output_csv_path = PARSED_MYSQL_LOG_FILE_PATH
    file_exists = os.path.exists(output_csv_path) and os.path.getsize(output_csv_path) > 0
    df_new.to_csv(
        output_csv_path, mode='a', header=not file_exists, index=False,
        encoding='utf-8', quoting=csv.QUOTE_ALL
    )
    
    # Cập nhật trạng thái và metadata
    write_last_known_state(current_size, updated_sessions)
    if not df_new.empty: logging.info(f"Thành công! Đã xử lý và ghi thêm {len(df_new)} dòng mới.")
    update_metadata_file(PARSED_MYSQL_LOG_FILE_PATH, SOURCE_MYSQL_LOG_PATH, current_size)
    logging.info("Thành công! Đã cập nhật metadata mới.")


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Phân tích log MySQL một cách tăng dần.")
    parser.add_argument(
        '--input', type=str, required=True, help="Đường dẫn đến file log thô của MySQL."
    )
    parser.add_argument(
        '--output', type=str, required=True, help="Đường dẫn đến file CSV đầu ra."
    )
    parser.add_argument(
        '--no-reset-check', action='store_true', help="Bỏ qua kiểm tra toàn vẹn và Hard Reset. Dùng cho chế độ real-time."
    )
    args = parser.parse_args()

    # Cập nhật các biến toàn cục từ tham số để các hàm khác sử dụng đúng đường dẫn
    SOURCE_MYSQL_LOG_PATH = args.input
    PARSED_MYSQL_LOG_FILE_PATH = args.output
    META_FILE_PATH = args.output + ".meta"
    # Đảm bảo state file ở đúng thư mục với file output
    STATE_FILE_PATH = os.path.join(os.path.dirname(args.output), ".mysql_parser_state")
    
    # Điểm khởi đầu khi chạy script này trực tiếp từ terminal
    run_incremental_parser()