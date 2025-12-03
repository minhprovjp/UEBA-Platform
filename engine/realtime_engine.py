# engine/realtime_engine.py
import os, json, logging, sys
import time
import pandas as pd
from redis import Redis, ResponseError
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.config_manager import load_config
from engine.data_processor import load_and_process_data
from engine.db_writer import save_results_to_db
import threading
from email_alert import send_email_alert
from utils import generate_html_alert
from active_response import execute_lock_and_kill_strategy
from config import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [RealtimeEngine] - %(message)s")
# Cấu hình logging
logger = logging.getLogger("ResponseHandler")


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
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
        logging.info(f"Created consumer group {group} on {stream}")
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info(f"Consumer group {group} already exists on {stream}.")
        else:
            raise

def handle_email_alerts_async(results: dict):
    """
    Xử lý gửi email với cơ chế:
    1. Aggregation: Gom các bất thường lại.
    2. Throttling: Chỉ gửi nếu đã qua thời gian cooldown.
    3. Async: Gửi bằng thread riêng.
    """
    global LAST_EMAIL_SENT_TIME, PENDING_VIOLATIONS

    # 1. Thu thập dữ liệu tóm tắt từ batch hiện tại
    current_batch_summary = []
    def add_violation(df, title, description):
        if df is not None and not df.empty:
            # Trích xuất User/IP ---
            # Group by User & IP để lấy danh sách đối tượng
            if 'user' in df.columns and 'client_ip' in df.columns:
                users_ips = df.groupby(['user', 'client_ip']).size().reset_index().apply(
                    lambda x: f"{x['user']}@{x['client_ip']}", axis=1
                ).unique().tolist()
            elif 'user' in df.columns:
                users_ips = df['user'].unique().tolist()
            else:
                users_ips = ["Unknown"]

            time_col = 'start_time' if 'start_time' in df.columns else 'timestamp'

            # Đẩy object vào buffer
            current_batch_summary.append({
                'title': title,
                'count': len(df),
                'first_time': df[time_col].min(),  # Giữ dạng datetime để sort/min/max
                'last_time': df[time_col].max(),
                'desc': description,
                'targets': users_ips  # List các user liên quan
            })
    # Trích xuất dữ liệu
    add_violation(results.get("anomalies_late_night"), "Late-Night Access", "Queries executed outside the allowed time window")
    add_violation(results.get("anomalies_dump"), "Large Data Dump", "Queries without a WHERE clause or with always-true conditions, ...")
    add_violation(results.get("anomalies_multi_table"), "Multi-Table Access", "A session scanning many different tables")
    add_violation(results.get("anomalies_sensitive"), "Sensitive Data", "Unauthorized access to protected tables")
    add_violation(results.get("anomalies_user_time"), "Abnormal Profile", "Activity different from the user’s normal behavior.")

    # Nếu có vi phạm mới, thêm vào buffer chung
    if current_batch_summary:
        PENDING_VIOLATIONS.extend(current_batch_summary)

    # 2. Kiểm tra Cooldown (Throttling)
    # Chỉ gửi khi: (Có dữ liệu trong buffer) VÀ (Đã qua thời gian cooldown)
    now = datetime.now()
    time_since_last = (now - LAST_EMAIL_SENT_TIME).total_seconds()

    if PENDING_VIOLATIONS and (time_since_last > EMAIL_COOLDOWN_SECONDS):
        # Sao chép buffer để gửi và xóa buffer gốc (tránh race condition)
        data_to_send = PENDING_VIOLATIONS.copy()
        PENDING_VIOLATIONS.clear()
        LAST_EMAIL_SENT_TIME = now


        # 3. Gửi Async bằng Thread
        email_thread = threading.Thread(
            target=send_email_thread_worker,
            args=(data_to_send,)
        )
        email_thread.daemon = True  # Thread sẽ tự tắt khi chương trình chính tắt
        email_thread.start()

def aggregate_violations(violation_list):
    """
    Gộp các vi phạm cùng loại lại với nhau.
    Input: List các dict rời rạc.
    Output: List các dict đã gộp (Unique theo Title).
    """
    aggregated = {}

    for item in violation_list:
        title = item['title']

        if title not in aggregated:
            aggregated[title] = {
                'title': title,
                'desc': item['desc'],
                'count': 0,
                'first_time': item['first_time'],
                'last_time': item['last_time'],
                'targets': set()
            }

        # Cộng dồn
        agg = aggregated[title]
        agg['count'] += item['count']
        agg['targets'].update(item['targets'])

        # Cập nhật thời gian min/max
        if item['first_time'] < agg['first_time']:
            agg['first_time'] = item['first_time']
        if item['last_time'] > agg['last_time']:
            agg['last_time'] = item['last_time']

    # Chuyển đổi lại sang format list để render
    final_list = []
    for val in aggregated.values():
        # Format lại thời gian và user list
        val['time_range'] = f"{val['first_time'].strftime('%H:%M:%S')} - {val['last_time'].strftime('%H:%M:%S')}"
        val['target_str'] = ", ".join(sorted(list(val['targets'])))
        final_list.append(val)

    return final_list

def send_email_thread_worker(summary_data):
    """Hàm worker chạy trong thread riêng để gửi email thật."""
    try:
        # 1. GỌI HÀM GOM NHÓM - summary_data (raw list) -> aggregated_data (grouped list)
        aggregated_data = aggregate_violations(summary_data)
        # 2. Tạo nội dung Text (Fallback)
        text_content = "[UEBA ALERT]: Detected abnormal behavior:\n\n"
        for item in aggregated_data:
            text_content += f"⚠ {item['title']} ({item['count']} events)\n"
            text_content += f"   • Target: {item['target_str']}\n"
            text_content += f"   • Time: {item['time_range']}\n"
            text_content += f"   • Desc: {item['desc']}\n\n"

        text_content += "──────────────────────────────\nPlease check Dashboard for details."

        # 3. Tạo nội dung HTML
        html_content = generate_html_alert(aggregated_data)

        # 4. Tiêu đề email
        email_subject = f"[UEBA ALERT] Detect {len(aggregated_data)} type/s of abnormal behavior"

        # 5. Sending
        success = send_email_alert(
            subject=email_subject,
            text_content=text_content,
            html_content=html_content,
            to_recipients=ALERT_EMAIL_SETTINGS["to_recipients"],
            smtp_server=ALERT_EMAIL_SETTINGS["smtp_server"],
            smtp_port=ALERT_EMAIL_SETTINGS["smtp_port"],
            sender_email=ALERT_EMAIL_SETTINGS["sender_email"],
            sender_password=ALERT_EMAIL_SETTINGS["sender_password"],
            bcc_recipients=ALERT_EMAIL_SETTINGS["bcc_recipients"]
        )

        if success is True:
            logger.info("--> [Security Alert Triggered] Send successfully.")
        else:
            logger.error(f"--> [Security Alert] Send failed: {success}")

    except Exception as e:
        logger.error(f"--> [Security Alert] Exception error: {e}")

def handle_active_responses(results: dict):
    """
    Kiểm tra danh sách user vượt ngưỡng và thực hiện Lock/Kill.
    Args:
        results (dict): Dictionary trả về từ data_processor.
    """
    users_to_lock = results.get("users_to_lock", [])

    if not users_to_lock:
        return  # Không có user nào cần xử lý

    admin_user = ACTIVE_RESPONSE_SETTINGS.get('mysql_user', '')

    for offender in users_to_lock:
        user_name = offender['user']
        total_count = offender['total_violation_count']

        # === SAFETY SWITCH ===
        if admin_user and user_name == admin_user:
            continue

        reason = f"Automatic response: Over the threshold ({total_count})"

        try:
            execute_lock_and_kill_strategy(user_name, ACTIVE_RESPONSE_SETTINGS, reason)
        except Exception as e:
            logger.error(f"Lỗi khi thực thi Active Response cho user {user_name}: {e}")


def start_engine():
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    for stream in STREAMS.values():
        try:
            r.xgroup_create(stream, REDIS_GROUP_ENGINE, id="0", mkstream=True)
        except:
            pass

    logging.info("Realtime UEBA Engine STARTED — Monitoring MySQL Performance Schema")

    while True:
        try:
            msgs = r.xreadgroup(
                groupname=REDIS_GROUP_ENGINE,
                consumername=REDIS_CONSUMER_NAME,
                streams=STREAMS,
                count=100,
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

                # try:
                #     handle_email_alerts_async(results)    # Sending Alert (nếu có nội dung)
                # except Exception as e:
                #     logging.error(f"[Email Error] Error creating email sending thread: {e}", exc_info=True)
                #
                # try:
                #     handle_active_responses(results)   # Active Response (nếu có user vượt ngưỡng)
                # except Exception as e:
                #     logging.error(f"[Active Response Error] Error while executing Lock/Kill: {e}", exc_info=True)

                # ACK messages
                for stream, msg_id in ack_ids:
                    r.xack(stream, REDIS_GROUP_ENGINE, msg_id)

        except KeyboardInterrupt:
            logging.info("Engine stopped by user")
            break
        except Exception as e:
            logging.error(f"Engine error: {e}", exc_info=True)

if __name__ == "__main__":
    start_engine()