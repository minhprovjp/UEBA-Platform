# engine/realtime_monitor.py
import subprocess
import sys
import os
import time
import logging
import threading
import shutil

# Thêm thư mục gốc vào sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SOURCE_MYSQL_LOG_PATH, MYSQL_PARSER_SCRIPT_PATH, PARSED_MYSQL_LOG_FILE_PATH #, SOURCE_POSTGRES_LOG_DIR

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [RealtimeMonitor] - %(message)s')

def run_parser(parser_script_path, source_log_path, output_csv_path):
    """Hàm helper để gọi một script parser cụ thể."""
    try:
        logging.info(f"Triggering parser: {os.path.basename(parser_script_path)} for {os.path.basename(source_log_path)}")
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        command = [sys.executable, parser_script_path, "--input", source_log_path, "--output", output_csv_path]
        
        # Chạy parser và chờ nó hoàn thành
        subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8', errors='ignore', env=env)
        
        logging.info(f"Parser {os.path.basename(parser_script_path)} finished successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing parser {os.path.basename(parser_script_path)}: {e.stderr}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while running parser: {e}")

def monitor_log_file(log_path, parser_script, output_csv):
    """
    Theo dõi một file log duy nhất bằng cách sử dụng các lệnh gốc của HĐH.
    """
    if not os.path.exists(log_path):
        logging.error(f"Log file not found: {log_path}. Cannot start monitoring.")
        return

    logging.info(f"Starting to monitor: {log_path}")

    # Chọn câu lệnh dựa trên hệ điều hành
    if sys.platform == "win32":
    # Prefer pwsh (PowerShell Core), fallback to classic powershell.exe
        powershell_cmd = "pwsh" if shutil.which("pwsh") else "powershell"
        command = [powershell_cmd, "-Command", f"Get-Content '{log_path}' -Wait -Tail 1"]
    else:
        # Dành cho Linux và macOS
        command = ["tail", "-F", "-n", "0", log_path]

    # Sử dụng Popen để chạy lệnh trong một tiến trình nền
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')

    # Liên tục đọc output từ tiến trình (đây là các dòng log mới)
    while True:
        line = process.stdout.readline()
        if not line:
            # Nếu không có dòng nào, kiểm tra xem tiến trình có bị lỗi không
            if process.poll() is not None:
                logging.error(f"Monitoring process for {log_path} terminated unexpectedly.")
                break
            continue # Tiếp tục vòng lặp
        
        # Khi có một dòng mới, ngay lập tức trigger parser
        logging.info(f"New log entry detected in {os.path.basename(log_path)}. Triggering parser.")
        # Chạy parser trong một thread riêng để không block việc đọc log tiếp theo (nâng cao)
        # Tạm thời, chúng ta sẽ chạy tuần tự
        run_parser(parser_script, log_path, output_csv)

if __name__ == "__main__":
    logging.info("Real-time Log Monitor started. Press Ctrl+C to stop.")
    
    # === CẤU HÌNH CÁC FILE CẦN THEO DÕI ===
    # Trong tương lai, có thể đọc từ một file config
    
    # Theo dõi MySQL
    # Lưu ý: `tail -f` và `Get-Content -Wait` hoạt động tốt nhất trên một file duy nhất.
    # Logic cho PostgreSQL sẽ cần được điều chỉnh nếu nó tạo nhiều file.
    # Tạm thời, chúng ta giả định nó là một file cụ thể để minh họa.
    mysql_monitor_thread = threading.Thread(
        target=monitor_log_file,
        args=(SOURCE_MYSQL_LOG_PATH, MYSQL_PARSER_SCRIPT_PATH, PARSED_MYSQL_LOG_FILE_PATH),
        daemon=True
    )
    
    # (Phần nâng cao cho PostgreSQL sẽ được thảo luận bên dưới)
    
    mysql_monitor_thread.start()
    
    try:
        # Giữ cho script chính sống
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Shutting down monitor.")