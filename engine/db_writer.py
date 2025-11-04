# engine/db_writer.py
import logging
import pandas as pd
from backend_api.models import Anomaly, SessionLocal

# Thiết lập logging riêng cho file này
log = logging.getLogger("db_writer")
log.setLevel(logging.INFO)
if not log.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - [DBWriter] - %(message)s'))
    log.addHandler(handler)


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
            if "anomalies_" in anomaly_key and isinstance(df_anomaly, pd.DataFrame) and not df_anomaly.empty:
                
                log.info(f"Phát hiện {len(df_anomaly)} bất thường loại '{anomaly_key}'...")
                anomaly_type_name = anomaly_key.replace("anomalies_", "")
                
                for _, row in df_anomaly.iterrows():
                    # Tạo một đối tượng Anomaly của SQLAlchemy
                    new_anomaly_record = Anomaly(
                        timestamp=row.get('timestamp') or row.get('start_time'),
                        user=row.get('user'),
                        client_ip=row.get('client_ip'),
                        database=row.get('database'),
                        query=row.get('query', f"Session-based anomaly: {anomaly_type_name}"),
                        anomaly_type=anomaly_type_name,
                        score=row.get('anomaly_score') or row.get('deviation_score'),
                        reason=row.get('violation_reason') or row.get('unusual_activity_reason') or row.get('reasons')
                    )
                    db.add(new_anomaly_record)
                    records_saved += 1
        
        if records_saved > 0:
            db.commit()
            log.info(f"Lưu thành công {records_saved} bản ghi bất thường mới vào CSDL.")
            
    except Exception as e:
        log.error(f"Lỗi khi lưu bất thường vào CSDL: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()