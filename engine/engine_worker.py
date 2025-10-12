# engine/engine_worker.py
import redis
import json
import pandas as pd
import sys
import os
import logging

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
    logging.info("Engine Worker đã khởi động, đang chờ log đã phân tích từ 'parsed_logs_queue'...")
    Base.metadata.create_all(bind=engine) # Đảm bảo bảng tồn tại
    try:
        while True:
            _, record_json = redis_client.brpop("parsed_logs_queue")
            parsed_record = json.loads(record_json)
            logging.info(f"Nhận được 1 bản ghi đã phân tích để xử lý.")
            
            # Chuyển đổi thành DataFrame một dòng để tương thích với data_processor
            df_single_log = pd.DataFrame([parsed_record])
            
            # Tải cấu hình mới nhất
            config = load_config()
            analysis_params = config.get("analysis_params", {})
            
            # Gọi hàm phân tích
            results = load_and_process_data(df_single_log, analysis_params)
            
            # Nếu có bất thường, ghi vào CSDL
            if results and any(not df.empty for key, df in results.items() if "anomalies" in key):
                logging.info("Phát hiện bất thường, đang lưu vào CSDL...")
                save_anomalies_to_db(results)
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
    logging.info("Engine Worker đã dừng.")