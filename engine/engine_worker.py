# engine/engine_worker.py
import redis
import json
import pandas as pd
import sys
import os
import logging

try:
    from engine.mysql_log_parser import mysql_connect_regex, mysql_query_regex, mysql_init_db_regex
except ImportError:
    logging.error("Không thể import regex từ mysql_log_parser.py. Đảm bảo file tồn tại và đúng cấu trúc.")
    sys.exit(1)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *
from engine.data_processor import load_and_process_data
from engine.config_manager import load_config
from backend_api.models import Base, Anomaly, engine, SessionLocal

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - [EngineWorker] - %(message)s')
log_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'workers', 'engine_worker.log')
file_handler = logging.FileHandler(log_file_path, 'a', 'utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
stream_handler.encoding = 'utf-8'
stream_handler.errors = 'replace'
logger.addHandler(stream_handler)

from backend_api.models import Anomaly, SessionLocal

def process_pending_queries(redis_client, thread_id, session_data):
    """
    Hàm helper để xử lý các query đang chờ sau khi session được tạo.
    """
    pending_queue_key = f"pending_queries:{thread_id}"
    pending_queries = redis_client.lrange(pending_queue_key, 0, -1)
    
    if not pending_queries:
        return

    logging.info(f"Tìm thấy {len(pending_queries)} query đang chờ cho thread_id: {thread_id}. Đang xử lý lại...")
    for query_json in pending_queries:
        try:
            query_data = json.loads(query_json)
            parsed_record = {
                'timestamp': query_data['timestamp'],
                'user': session_data['user'],
                'client_ip': session_data['host'],
                'database': session_data.get('db', 'N/A'),
                'query': query_data['query'],
                'source_dbms': 'mysql'
            }
            redis_client.lpush("parsed_logs_queue", json.dumps(parsed_record))
        except json.JSONDecodeError:
            logging.error(f"Lỗi giải mã JSON từ hàng đợi chờ: {query_json}")

    redis_client.delete(pending_queue_key)
    logging.info(f"Đã xử lý và xóa hàng đợi chờ cho thread_id: {thread_id}.")

def save_anomalies_to_db(results: dict):
    """
    Nhận dictionary kết quả từ data_processor, trích xuất tất cả các bất thường
    và lưu chúng vào cơ sở dữ liệu.
    """
    db = SessionLocal()
    records_saved = 0
    try:
        # Lặp qua tất cả các cặp key-value trong dictionary kết quả
        for anomaly_key, df_anomaly in results.items():
            
            # Chỉ xử lý các DataFrame chứa bất thường và không rỗng
            if "anomalies_" in anomaly_key and not df_anomaly.empty:
                
                # Lấy ra tên loại bất thường từ key (ví dụ: 'anomalies_late_night' -> 'late_night')
                anomaly_type_name = anomaly_key.replace("anomalies_", "")
                
                # Lặp qua từng dòng trong DataFrame bất thường
                for _, row in df_anomaly.iterrows():
                    
                    # Tạo một đối tượng Anomaly của SQLAlchemy
                    new_anomaly_record = Anomaly(
                        # Lấy timestamp từ cột 'timestamp' hoặc 'start_time' (cho rule multi_table)
                        timestamp=row.get('timestamp') or row.get('start_time'),
                        user=row.get('user'),
                        client_ip=row.get('client_ip'),
                        database=row.get('database'),
                        # Xử lý trường hợp query không tồn tại (cho rule multi_table)
                        query=row.get('query', f"Session-based anomaly: {anomaly_type_name}"),
                        
                        anomaly_type=anomaly_type_name,
                        
                        # Lấy điểm số từ các cột có thể có
                        score=row.get('anomaly_score') or row.get('deviation_score'),
                        
                        # Lấy lý do từ các cột có thể có
                        reason=row.get('violation_reason') or row.get('unusual_activity_reason') or row.get('reasons')
                    )
                    db.add(new_anomaly_record)
                    records_saved += 1
        
        # Nếu có bất kỳ bản ghi nào được thêm, commit thay đổi vào CSDL
        if records_saved > 0:
            db.commit()
            logging.info(f"Lưu thành công {records_saved} bản ghi bất thường mới vào CSDL.")
            
    except Exception as e:
        logging.error(f"Lỗi khi lưu bất thường vào CSDL: {e}")
        # Nếu có lỗi, hoàn tác tất cả các thay đổi trong phiên này
        db.rollback()
    finally:
        # Luôn luôn đóng session CSDL sau khi hoàn tất
        db.close()

def main_loop(redis_client):
    logging.info("MySQL Parser Worker đã khởi động, đang chờ log từ 'raw_logs_queue'...")
    try:
        while True:
            try:
                message_tuple = redis_client.brpop("raw_logs_queue", timeout=1)
                if message_tuple is None:
                    continue
                    
                _, message_json = message_tuple
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
                    
                    db_name = data.get('db') or 'N/A'
                    session_data = {'user': data.get('user'), 'host': data.get('host'), 'db': db_name}
                    
                    redis_client.hset(session_key, mapping=session_data)
                    redis_client.expire(session_key, 3600)
                    logging.info(f"Đã tạo/cập nhật session cho thread_id: {thread_id}")

                    # Gọi hàm để xử lý các query đang chờ (nếu có)
                    process_pending_queries(redis_client, thread_id, session_data)

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
                    
                    if session_info:
                        # Kịch bản TỐT: Session tồn tại
                        parsed_record = {
                            'timestamp': data['timestamp'], 'user': session_info.get('user'),
                            'client_ip': session_info.get('host'), 'database': session_info.get('db', 'N/A'),
                            'query': data['query'], 'source_dbms': 'mysql'
                        }
                        redis_client.lpush("parsed_logs_queue", json.dumps(parsed_record))
                        logging.info(f"Đã phân tích (session tồn tại) và đẩy 1 bản ghi từ thread_id: {thread_id}.")
                    else:
                        # KỊCH BẢN CHỜ: Session chưa tồn tại
                        logging.warning(f"Không tìm thấy session cho thread_id: {thread_id}. Đưa query vào hàng đợi chờ.")
                        pending_queue_key = f"pending_queries:{thread_id}"
                        query_data = {'timestamp': data['timestamp'], 'query': data['query']}
                        redis_client.rpush(pending_queue_key, json.dumps(query_data))
                        redis_client.expire(pending_queue_key, 60)

                elif quit_match:
                    thread_id = quit_match.group(1)
                    session_key = f"mysql_session:{thread_id}"
                    pending_queue_key = f"pending_queries:{thread_id}"
                    redis_client.delete(session_key, pending_queue_key) # Xóa cả session và hàng đợi chờ
                    logging.info(f"Đã xóa session và hàng đợi chờ cho thread_id: {thread_id}")

            except Exception as e:
                logging.error(f"Lỗi không xác định trong worker: {e}", exc_info=True)
                
    except KeyboardInterrupt:
        logging.info("Nhận tín hiệu KeyboardInterrupt, đang dừng MySQL Parser Worker...")

if __name__ == "__main__":
    try:
        r = redis.Redis(decode_responses=True)
        r.ping()
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Không thể kết nối đến Redis: {e}")
        sys.exit(1)
    main_loop(r)
    logging.info("Engine Worker đã dừng.")