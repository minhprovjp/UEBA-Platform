# engine/db_writer.py
import logging
import pandas as pd
from backend_api.models import Anomaly, SessionLocal
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backend_api.models import Anomaly, AllLogs, SessionLocal

# Thiết lập logging riêng cho file này
log = logging.getLogger("db_writer")
log.setLevel(logging.INFO)
if not log.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - [DBWriter] - %(message)s'))
    log.addHandler(handler)


def save_results_to_db(results: dict):
    """
    NÂNG CẤP: Nhận dict kết quả từ data_processor và lưu vào CẢ HAI bảng:
    1. 'all_logs' (mọi thứ)
    2. 'anomalies' (chỉ các bất thường)
    """
    
    # Lấy các DataFrame từ kết quả
    df_normal = results.get("normal_activities")
    
    # Gom tất cả các DataFrame bất thường lại
    df_anomalies_list = []
    for key, df in results.items():
        if "anomalies_" in key and isinstance(df, pd.DataFrame) and not df.empty:
            # Gán 'anomaly_type' cho từng DataFrame
            anomaly_type_name = key.replace("anomalies_", "")
            df['anomaly_type'] = anomaly_type_name
            df_anomalies_list.append(df)

    db = SessionLocal()
    records_saved_logs = 0
    records_saved_anomalies = 0
    
    try:
        # === 1. Xử lý Bảng 'all_logs' ===
        log_records_to_insert = []
        
        # Thêm các log BÌNH THƯỜNG
        if df_normal is not None and not df_normal.empty:
            df_normal['is_anomaly'] = False
            df_normal['analysis_type'] = df_normal['analysis_type'].fillna('Normal')
            df_normal['rows_returned'] = df_normal.get('rows_returned', 0).fillna(0).astype(int)
            df_normal['rows_affected'] = df_normal.get('rows_affected', 0).fillna(0).astype(int)
            log_records_to_insert.extend(df_normal.to_dict('records'))
            
        # Thêm các log BẤT THƯỜNG
        if df_anomalies_list:
            df_all_anomalies = pd.concat(df_anomalies_list, ignore_index=True)
            df_all_anomalies['is_anomaly'] = True
            
            df_all_anomalies['rows_returned'] = df_all_anomalies.get('rows_returned', 0).fillna(0).astype(int)
            df_all_anomalies['rows_affected'] = df_all_anomalies.get('rows_affected', 0).fillna(0).astype(int)
            
            # Xử lý các cột không nhất quán (ví dụ: multi_table có 'start_time')
            if 'start_time' in df_all_anomalies.columns:
                # 1. Nếu cột 'timestamp' KHÔNG TỒN TẠI...
                if 'timestamp' not in df_all_anomalies.columns:
                    # ...hãy TẠO NÓ từ cột 'start_time'.
                    df_all_anomalies['timestamp'] = df_all_anomalies['start_time']
                else:
                    # 2. Nếu nó TỒN TẠI, thì mới fillna (điền giá trị thiếu)
                    df_all_anomalies['timestamp'] = df_all_anomalies['timestamp'].fillna(df_all_anomalies['start_time'])
            
            log_records_to_insert.extend(df_all_anomalies.to_dict('records'))

        # Sử dụng bulk_insert_mappings để chèn hiệu suất cao
        if log_records_to_insert:
            # Chỉ giữ lại các cột có trong model AllLogs để tránh lỗi
            valid_cols = AllLogs.__table__.columns.keys()
            clean_log_records = [
                {k: v for k, v in rec.items() if k in valid_cols}
                for rec in log_records_to_insert
            ]
            
            db.bulk_insert_mappings(AllLogs, clean_log_records)
            records_saved_logs = len(clean_log_records)

        # === 2. Xử lý Bảng 'anomalies' (Như cũ, nhưng hiệu quả hơn) ===
        anomaly_records_to_insert = []
        if df_anomalies_list:
            # df_all_anomalies đã được tạo ở trên
            # Đảm bảo các cột tồn tại trước khi cố gắng kết hợp
            # Sử dụng pd.Series để tạo cột nếu nó thiếu, điền bằng pd.NA
            reason_col_a = df_all_anomalies.get('unusual_activity_reason', pd.Series(pd.NA, index=df_all_anomalies.index))
            reason_col_b = df_all_anomalies.get('deviation_reasons', pd.Series(pd.NA, index=df_all_anomalies.index))
            
            # Sử dụng combine_first để điền giá trị từ cột thứ hai vào NA của cột thứ nhất
            df_all_anomalies['reason'] = reason_col_a.combine_first(reason_col_b)
                    
            score_col_a = df_all_anomalies.get('anomaly_score', pd.Series(pd.NA, index=df_all_anomalies.index))
            score_col_b = df_all_anomalies.get('deviation_score', pd.Series(pd.NA, index=df_all_anomalies.index))

            # df_all_anomalies['score'] = score_col_a.combine_first(score_col_b)
            
            # # Chuẩn hóa các cột 'reason' và 'score'
            # df_all_anomalies['reason'] = df_all_anomalies.get('violation_reason', pd.NA) \
            #     .fillna(df_all_anomalies.get('unusual_activity_reason', pd.NA)) \
            #     .fillna(df_all_anomalies.get('reasons', pd.NA))
                
            # df_all_anomalies['score'] = df_all_anomalies.get('anomaly_score', pd.NA) \
            #     .fillna(df_all_anomalies.get('deviation_score', pd.NA))
            
            df_all_anomalies['query'] = df_all_anomalies['query'].fillna(
                "Session-based anomaly: " + df_all_anomalies['anomaly_type']
            )
            
            anomaly_records_to_insert = df_all_anomalies.to_dict('records')

        if anomaly_records_to_insert:
            # Chỉ giữ lại các cột có trong model Anomaly
            valid_cols = Anomaly.__table__.columns.keys()
            clean_anomaly_records = [
                {k: v for k, v in rec.items() if k in valid_cols}
                for rec in anomaly_records_to_insert
            ]
            
            db.bulk_insert_mappings(Anomaly, clean_anomaly_records)
            records_saved_anomalies = len(clean_anomaly_records)

        # Commit CẢ HAI bảng cùng lúc
        if records_saved_logs > 0 or records_saved_anomalies > 0:
            db.commit()
            log.info(f"Lưu CSDL thành công: {records_saved_logs} bản ghi 'all_logs', "
                     f"{records_saved_anomalies} bản ghi 'anomalies'.")
            
    except Exception as e:
        log.error(f"Lỗi khi lưu vào CSDL (bulk insert): {e}", exc_info=True)
        db.rollback()
        raise # Ném lỗi ra ngoài để realtime_engine biết và KHÔNG ACK tin nhắn
    finally:
        db.close()