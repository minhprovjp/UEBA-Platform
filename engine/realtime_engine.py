# engine/realtime_engine.py
import os, json, logging, sys
import time
import pandas as pd
from redis import Redis, ResponseError
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.config_manager import load_config
from engine.data_processor import load_and_process_data
from engine.db_writer import save_results_to_db
from email_alert import send_email_alert
from config import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [RealtimeEngine] - %(message)s")


    
# --- HÀM KẾT NỐI REDIS TIN CẬY ---
def connect_redis():
    """Kết nối đến Redis với cơ chế thử lại vô hạn."""
    while True:
        try:
            r = Redis.from_url(REDIS_URL, decode_responses=True)
            r.ping()
            logging.info("Kết nối Redis (Engine) thành công.")
            return r
        except ConnectionError as e:
            logging.error(f"Kết nối Redis (Engine) thất bại: {e}. Thử lại sau 5 giây...")
            time.sleep(5)

def ensure_group(r: Redis, stream: str, group: str):
    """Đảm bảo Consumer Group tồn tại"""
    try:
        r.xgroup_create(stream, group, id="$", mkstream=True)
        logging.info(f"Created consumer group {group} on {stream}")
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info(f"Consumer group {group} already exists on {stream}.")
            pass
        else:
            logging.error(f"❌ Lỗi tạo group {group} trên {stream}: {e}")
            raise e

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

        # try:
        #     result = send_email_alert(
        #         subject=subject,
        #         message=message_body,
        #         to_recipients=ALERT_EMAIL_SETTINGS["to_recipients"],
        #         smtp_server=ALERT_EMAIL_SETTINGS["smtp_server"],
        #         smtp_port=int(ALERT_EMAIL_SETTINGS["smtp_port"]),
        #         sender_email=ALERT_EMAIL_SETTINGS["sender_email"],
        #         sender_password=ALERT_EMAIL_SETTINGS["sender_password"],
        #         bcc_recipients=ALERT_EMAIL_SETTINGS.get("bcc_recipients")
        #     )
        #     if result is True:
        #         logging.info("Gửi email cảnh báo thành công.")
        #     else:
        #         logging.error(f"Gửi email cảnh báo thất bại: {result}")
        # except Exception as e:
        #     logging.error(f"Lỗi nghiêm trọng khi gọi send_email_alert: {e}")

def start_engine():
    r = connect_redis()
    
    logging.info(f"Initializing Consumer Group: {REDIS_GROUP_ENGINE}")
    for stream in STREAMS.values():
        ensure_group(r, stream, REDIS_GROUP_ENGINE)

    ensure_group(r, "uba:logs:mysql", REDIS_GROUP_ENGINE)
    logging.info("Realtime UBA Engine STARTED — Monitoring MySQL Performance Schema")

    while True:
        try:
            msgs = r.xreadgroup(
                groupname=REDIS_GROUP_ENGINE,
                consumername=REDIS_CONSUMER_NAME,
                streams=STREAMS,
                count=10000,
                block=5000
            )

            if not msgs:
                continue

            records = []
            ack_ids = []

            for stream, entries in msgs:
                for msg_id, fields in entries:
                    data = fields.get("data")
                    if data:
                        records.append(json.loads(data))
                        ack_ids.append((stream, msg_id))

            if records:
                df = pd.DataFrame(records)
                results = load_and_process_data(df, {})

                # Save to DB
                save_results_to_db(results)
                # Send alert if high-risk
                anomalies = results.get("anomalies_ml", pd.DataFrame())
                # if not anomalies.empty and anomalies['ml_anomaly_score'].max() > 0.85:
                #     send_email_alert(
                #         subject=f"UBA ALERT: {len(anomalies)} High-Risk Queries (Score > 0.85)",
                #         results_df=anomalies,
                #         top_n=8
                #     )
                # ACK messages
                for stream, msg_id in ack_ids:
                    r.xack(stream, REDIS_GROUP_ENGINE, msg_id)

        except KeyboardInterrupt:
            logging.info("Engine stopped by user")
            break
        except Exception as e:
            logging.error(f"Engine error: {e}", exc_info=True)
            time.sleep(1)

if __name__ == "__main__":
    start_engine()