# engine/mysql_parser_worker.py
import redis
import json
import re
import sys
import os
import logging
import pandas as pd # Thêm import pandas

# Thêm thư mục gốc vào sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# === SỬA LỖI QUAN TRỌNG: IMPORT CÁC BIẾN REGEX TỪ PARSER GỐC ===
# Thay vì định nghĩa lại, chúng ta sẽ import trực tiếp
try:
    from engine.mysql_log_parser import mysql_connect_regex, mysql_query_regex, mysql_init_db_regex
except ImportError:
    # Fallback nếu file mysql_log_parser không có sẵn (không nên xảy ra)
    # Định nghĩa lại các regex ở đây nếu cần
    print("Cảnh báo: Không thể import regex từ mysql_log_parser.py")
    mysql_connect_regex = re.compile(r"...") 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [MySQLParserWorker] - %(message)s')

import redis
import json
import re
import sys
import os
import logging
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from engine.mysql_log_parser import mysql_connect_regex, mysql_query_regex, mysql_init_db_regex
except ImportError:
    logging.error("Không thể import regex. Hãy đảm bảo bạn chạy script từ thư mục gốc.")
    sys.exit(1)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - [MySQLParserWorker] - %(message)s')
log_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'workers', 'mysql_parser_worker.log')
file_handler = logging.FileHandler(log_file_path, 'a', 'utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
stream_handler.encoding = 'utf-8'
stream_handler.errors = 'replace'
logger.addHandler(stream_handler)

def main_loop(redis_client):
    logging.info("MySQL Parser Worker đã khởi động, đang chờ log từ 'raw_logs_queue'...")
    try:
        while True:
            try:
                _, message_json = redis_client.brpop("raw_logs_queue")
                message = json.loads(message_json)
                
                if message.get('dbms') != 'mysql':
                    continue

                line = message['log_line']
                
                connect_match = mysql_connect_regex.match(line)
                query_match = mysql_query_regex.match(line)
                init_db_match = mysql_init_db_regex.match(line)
                quit_match = re.search(r"(\d+)\s+Quit\s*$", line)
                
                if connect_match:
                    data = connect_match.groupdict()
                    thread_id = data['thread_id']
                    session_key = f"mysql_session:{thread_id}"
                    session_data = {'user': data['user'], 'host': data['host'], 'db': data.get('db', 'N/A')}
                    redis_client.hset(session_key, mapping=session_data)
                    redis_client.expire(session_key, 3600)
                    logging.info(f"Đã tạo/cập nhật session cho thread_id: {thread_id}")

                elif init_db_match:
                    data = init_db_match.groupdict()
                    thread_id = data['thread_id']
                    session_key = f"mysql_session:{thread_id}"
                    redis_client.hset(session_key, 'db', data['db_name'])
                    logging.info(f"Đã cập nhật DB cho thread_id: {thread_id}")

                elif query_match:
                    data = query_match.groupdict()
                    thread_id = data['thread_id']
                    session_key = f"mysql_session:{thread_id}"
                    
                    session_info = redis_client.hgetall(session_key)
                    
                    # === SỬA ĐỔI QUAN TRỌNG: XỬ LÝ KHI KHÔNG TÌM THẤY SESSION ===
                    if not session_info:
                        logging.warning(f"Không tìm thấy session cho thread_id: {thread_id}. Sử dụng session mặc định.")
                        # Tạo một session mặc định/giả để vẫn có thể xử lý query
                        session_info = {'user': 'unknown', 'host': 'unknown', 'db': 'N/A'}
                    
                    parsed_record = {
                        'timestamp': data['timestamp'],
                        'user': session_info.get('user'),
                        'client_ip': session_info.get('host'),
                        'database': session_info.get('db', 'N/A'),
                        'query': data['query'],
                        'source_dbms': 'mysql'
                    }
                    
                    logging.info(f"Bản ghi đã được parse: {json.dumps(parsed_record)}")
                    
                    redis_client.lpush("parsed_logs_queue", json.dumps(parsed_record))
                    logging.info(f"Đã phân tích và đẩy 1 bản ghi từ thread_id: {thread_id} vào 'parsed_logs_queue'.")

                elif quit_match:
                    thread_id = quit_match.group(1)
                    session_key = f"mysql_session:{thread_id}"
                    redis_client.delete(session_key)
                    logging.info(f"Đã xóa session cho thread_id: {thread_id}")

            except json.JSONDecodeError:
                logging.error(f"Lỗi giải mã JSON từ tin nhắn.")
            except Exception as e:
                logging.error(f"Lỗi không xác định trong worker: {e}", exc_info=True)
    except KeyboardInterrupt:
        logging.info("Nhận tín hiệu KeyboardInterrupt, đang dừng worker...")

if __name__ == "__main__":
    try:
        r = redis.Redis(decode_responses=True)
        r.ping()
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Không thể kết nối đến Redis: {e}")
        sys.exit(1)
        
    main_loop(r)
    logging.info("MySQL Parser Worker đã dừng.")