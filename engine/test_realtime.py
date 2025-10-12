# engine/realtime_monitor.py
import subprocess
import sys
import os
import time
import logging
import redis
import json

# Thêm thư mục gốc vào sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SOURCE_MYSQL_LOG_PATH

# Lấy ra logger gốc
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Đặt cấp độ log cho logger gốc

# Xóa các handler cũ có thể đã được cấu hình bởi các thư viện khác
if logger.hasHandlers():
    logger.handlers.clear()

# Định dạng chung
formatter = logging.Formatter('%(asctime)s - %(levelname)s - [RealtimeMonitor] - %(message)s')

# Tạo File Handler với encoding utf-8
log_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'workers', 'realtime_monitor.log')
file_handler = logging.FileHandler(log_file_path, 'a', 'utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Tạo Stream Handler (Console) với xử lý lỗi encoding
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
stream_handler.encoding = 'utf-8' # Cố gắng dùng utf-8 cho console
stream_handler.errors = 'replace'  # Thay thế ký tự lỗi bằng '?' thay vì bỏ qua
logger.addHandler(stream_handler)

def monitor_log_file(log_path, dbms_type, redis_client):
    """Theo dõi một file log và đẩy các dòng mới vào Redis."""
    if not os.path.exists(log_path):
        logging.error(f"File log không tồn tại: {log_path}. Không thể bắt đầu giám sát.")
        return

    logging.info(f"Bắt đầu giám sát real-time file: {log_path}")

    command = ["powershell", "-Command", f"Get-Content '{log_path}' -Wait -Tail 0"] if sys.platform == "win32" else ["tail", "-F", "-n", "0", log_path]

    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
    except FileNotFoundError:
        logging.error(f"Lỗi: Lệnh '{command[0]}' không được tìm thấy. Hãy đảm bảo PowerShell (trên Windows) hoặc tail (trên Linux/macOS) đã được cài đặt và có trong PATH.")
        return
    except Exception as e:
        logging.error(f"Lỗi không mong muốn khi khởi động tiến trình con: {e}")
        return

    try:
        while True:
            try:
                line = process.stdout.readline()
                if not line:
                    if process.poll() is not None:
                        logging.error(f"Tiến trình giám sát cho {log_path} đã dừng đột ngột. Mã lỗi: {process.returncode}")
                        stderr_output = process.stderr.read()
                        if stderr_output:
                            logging.error(f"Thông báo lỗi từ tiến trình con: {stderr_output}")
                        break # Thoát vòng lặp
                    # Nếu không có dòng nào và tiến trình vẫn chạy, chỉ là đang chờ
                    continue
                
                line = line.strip()
                if not line: # Bỏ qua các dòng trống
                    continue

                # Đóng gói tin nhắn
                message = {"dbms": dbms_type, "log_line": line}
                message_json = json.dumps(message)
                
                # === SỬA ĐỔI QUAN TRỌNG: GỬI VÀ XÁC THỰC ===
                try:
                    # lpush trả về số lượng phần tử trong list sau khi đẩy vào
                    result = redis_client.lpush("raw_logs_queue", message_json)
                    logging.info(f"Đã đẩy 1 dòng log {dbms_type} vào hàng đợi. Tổng số tin nhắn trong queue hiện tại: {result}")
                except redis.exceptions.RedisError as e:
                    logging.error(f"Lỗi Redis khi đẩy tin nhắn: {e}. Đang thử kết nối lại...")
                    # Cố gắng kết nối lại
                    try:
                        redis_client = redis.Redis(decode_responses=True)
                        redis_client.ping()
                        logging.info("Kết nối lại Redis thành công.")
                    except redis.exceptions.ConnectionError:
                        logging.error("Kết nối lại thất bại. Sẽ thử lại ở lần sau.")
                        time.sleep(5) # Chờ 5 giây trước khi thử lại
                except Exception as e:
                    logging.error(f"Lỗi không xác định khi gửi tin nhắn: {e}")
                    
            except Exception as e:
                logging.error(f"Lỗi trong vòng lặp chính của monitor: {e}")
                break
    finally:
        # Khối này sẽ LUÔN LUÔN được thực thi khi vòng lặp kết thúc
        # (kể cả khi bị dừng bởi Ctrl+C)
        logging.info("Đang dừng tiến trình giám sát con...")
        process.terminate() # Gửi tín hiệu dừng cho `tail` / `powershell`
        process.wait()      # Chờ nó kết thúc
        logging.info("Tiến trình giám sát con đã dừng.")


if __name__ == "__main__":
    redis_client = None
    # Cố gắng kết nối đến Redis trong một vòng lặp để tăng độ tin cậy
    for i in range(5):
        try:
            r = redis.Redis(decode_responses=True)
            r.ping()
            redis_client = r
            logging.info("Kết nối đến Redis thành công.")
            break # Thoát vòng lặp nếu thành công
        except redis.exceptions.ConnectionError as e:
            logging.error(f"Không thể kết nối đến Redis server (lần {i+1}/5): {e}. Thử lại sau 3 giây...")
            time.sleep(3)

    if not redis_client:
        logging.critical("KHÔNG THỂ KẾT NỐI ĐẾN REDIS SAU NHIỀU LẦN THỬ. Monitor sẽ thoát.")
        sys.exit(1)

    logging.info("Real-time Log Monitor đã khởi động. Nhấn Ctrl+C để dừng.")
    
    try:
        monitor_log_file(SOURCE_MYSQL_LOG_PATH, 'mysql', redis_client)
    except KeyboardInterrupt:
        logging.info("Nhận tín hiệu KeyboardInterrupt, đang tắt monitor...")