# engine/realtime_engine.py
import os, json, logging, sys
import time
import pandas as pd
from redis import Redis, ResponseError
from data_processor import load_and_process_data
from email_alert import send_email_alert

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import *

# Import hàm lưu CSDL từ file mới
from engine.db_writer import save_anomalies_to_db 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [RealtimeEngine] - %(message)s")

# Xây dựng dict config một lần
CFG = dict(
    p_late_night_start_time=LATE_NIGHT_START_TIME_DEFAULT,
    p_late_night_end_time=LATE_NIGHT_END_TIME_DEFAULT,
    p_known_large_tables=KNOWN_LARGE_TABLES_DEFAULT,
    p_time_window_minutes=TIME_WINDOW_DEFAULT_MINUTES,
    p_min_distinct_tables=MIN_DISTINCT_TABLES_THRESHOLD_DEFAULT,
    p_sensitive_tables=SENSITIVE_TABLES_DEFAULT,
    p_allowed_users_sensitive=ALLOWED_USERS_FOR_SENSITIVE_DEFAULT,
    p_safe_hours_start=SAFE_HOURS_START_DEFAULT,
    p_safe_hours_end=SAFE_HOURS_END_DEFAULT,
    p_safe_weekdays=SAFE_WEEKDAYS_DEFAULT,
    p_quantile_start=QUANTILE_START_DEFAULT,
    p_quantile_end=QUANTILE_END_DEFAULT,
    p_min_queries_for_profile=MIN_QUERIES_FOR_PROFILE_DEFAULT,
)

def ensure_group(r: Redis, stream: str, group: str):
    """Đảm bảo Consumer Group tồn tại"""
    try:
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
        logging.info(f"Created consumer group {group} on {stream}")
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info(f"Consumer group {group} already exists on {stream}.")
        else:
            raise

def check_and_send_alert(results: dict):
    """Kiểm tra kết quả, tạo báo cáo và gửi email nếu cần."""
    total_anomalies = 0
    report_lines = []

    for key, df in results.items():
        if "anomalies_" in key and isinstance(df, pd.DataFrame) and not df.empty:
            count = len(df)
            total_anomalies += count
            report_lines.append(f"- {key.replace('anomalies_', '').title()}: {count} trường hợp")
            
            # Lấy 3 ví dụ hàng đầu
            for _, row in df.head(3).iterrows():
                user = row.get('user', 'N/A')
                query_sample = str(row.get('query', 'N/A'))[:50]
                report_lines.append(f"    - User: {user}, Query: {query_sample}...")

    if total_anomalies > 0:
        logging.info(f"Phát hiện {total_anomalies} bất thường, đang gửi email...")
        
        subject = f"[UBA Cảnh Báo] Phát hiện {total_anomalies} hành vi bất thường mới"
        message_body = (
            "Hệ thống Giám sát Hành vi Người dùng (UBA) vừa phát hiện các hoạt động đáng ngờ:\n\n"
            + "\n".join(report_lines)
            + "\n\nVui lòng kiểm tra Bảng điều khiển để biết thêm chi tiết."
        )
        
        try:
            result = send_email_alert(
                subject=subject,
                message=message_body,
                to_recipients=ALERT_EMAIL_SETTINGS["to_recipients"],
                smtp_server=ALERT_EMAIL_SETTINGS["smtp_server"],
                smtp_port=int(ALERT_EMAIL_SETTINGS["smtp_port"]),
                sender_email=ALERT_EMAIL_SETTINGS["sender_email"],
                sender_password=ALERT_EMAIL_SETTINGS["sender_password"],
                bcc_recipients=ALERT_EMAIL_SETTINGS.get("bcc_recipients")
            )
            if result is True:
                logging.info("Gửi email cảnh báo thành công.")
            else:
                logging.error(f"Gửi email cảnh báo thất bại: {result}")
        except Exception as e:
            logging.error(f"Lỗi nghiêm trọng khi gọi send_email_alert: {e}")

def start_engine():
    r = Redis.from_url(REDIS_URL, decode_responses=True)

    for s in STREAMS:
        ensure_group(r, s, REDIS_GROUP_ENGINE)

    logging.info(f"Realtime engine started. Waiting for messages from {list(STREAMS.keys())}...")
    logging.info("Engine đang chạy... Nhấn Ctrl+C để dừng.")
    
    while True:
        try:
            resp = r.xreadgroup(
                groupname=REDIS_GROUP_ENGINE,
                consumername=REDIS_CONSUMER_NAME,
                streams=STREAMS,
                count=ENGINE_BATCH_MAX_MESSAGES,
                block=ENGINE_BATCH_MAX_BLOCK_MS,
            )
            if not resp:
                continue

            rows = []
            ack_ids_map = {stream: [] for stream in STREAMS}

            for (stream, entries) in resp:
                for (msg_id, fields) in entries:
                    ack_ids_map[stream].append(msg_id)
                    raw = fields.get("data")
                    if not raw: continue
                    try:
                        item = json.loads(raw)
                        rows.append(item)
                    except Exception as e:
                        logging.error(f"Bad message data: {e}")

            if not rows:
                for stream, ids in ack_ids_map.items():
                    if ids: r.xack(stream, REDIS_GROUP_ENGINE, *ids)
                continue

            # 1. XỬ LÝ DỮ LIỆU
            logging.info(f"Processing batch of {len(rows)} records...")
            df = pd.DataFrame(rows)
            results = load_and_process_data(df, CFG)

            # 2. LƯU BẤT THƯỜNG
            try:
                save_anomalies_to_db(results)
            except Exception as e:
                logging.error(f"Failed to save anomalies to DB: {e}", exc_info=True)

            # 3. GỬI CẢNH BÁO (MỚI)
            check_and_send_alert(results)

            # 4. ACK MESSAGE
            for stream, ids in ack_ids_map.items():
                if ids:
                    r.xack(stream, REDIS_GROUP_ENGINE, *ids)
        
        except KeyboardInterrupt:
            logging.info("Đã nhận tín hiệu (Ctrl+C). Engine đang dừng...")
            break
        except ResponseError as e:
            logging.error(f"Redis Response Error: {e}. Re-checking group...")
            time.sleep(1)
            for s in STREAMS:
                ensure_group(r, s, REDIS_GROUP_ENGINE)
        except Exception as e:
            logging.error(f"Unhandled error in engine loop: {e}", exc_info=True)
            time.sleep(5)
            
    logging.info("Engine đã dừng.")

if __name__ == "__main__":
    start_engine()