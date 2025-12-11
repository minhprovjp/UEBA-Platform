# engine/realtime_engine.py
import os, json, logging, sys, signal
import time
import threading
import pandas as pd
from redis import Redis, ResponseError, ConnectionError as RedisConnectionError, RedisError
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.config_manager import load_config
from engine.data_processor import load_and_process_data
from engine.db_writer import save_results_to_db
from email_alert import send_email_alert
from active_response import execute_lock_and_kill_strategy
from utils import generate_html_alert
from engine.utils import configure_redis_for_reliability, handle_redis_misconf_error
from config import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [RealtimeEngine] - %(message)s")
# Cấu hình logging
logger = logging.getLogger("ResponseHandler")

# Flag để điều khiển vòng lặp
is_running = True

def handle_shutdown(signum, frame):
    """Xử lý tín hiệu tắt (Ctrl+C) để dừng vòng lặp"""
    global is_running
    logging.info(f"🛑 Nhận tín hiệu dừng. Đang tắt Publisher...")
    is_running = False
    
# Đăng ký signal
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)
    
# --- HÀM KẾT NỐI REDIS ---
            
def connect_redis():
    while is_running:
        try:
            r = Redis.from_url(
                REDIS_URL, 
                decode_responses=True,
                socket_keepalive=True,
                socket_keepalive_options={},
                health_check_interval=30,
                retry_on_timeout=True,
                socket_connect_timeout=5
            )
            r.ping()
            
            # Configure Redis for better reliability
            configure_redis_for_reliability(r)
            
            logging.info("✅ Kết nối Redis thành công.")
            return r
        except Exception as e:
            logging.error(f"❌ Lỗi kết nối Redis: {e}. Thử lại sau 5s...")
            time.sleep(5)
    return None

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


def handle_email_alerts_async(results: dict):
    """
    Xử lý gửi email (Passive Response) tương thích với cấu trúc Rule mới.
    """
    global LAST_EMAIL_SENT_TIME, PENDING_VIOLATIONS

    # 1. Thu thập dữ liệu
    current_batch_summary = []
    # Set để theo dõi các dòng log đã xử lý
    processed_log_indices = set()

    # Hàm helper nội bộ
    def add_violation_from_group(df, group_name_fallback):
        if df is not None and not df.empty:

            new_logs = df[~df.index.isin(processed_log_indices)]
            if new_logs.empty:
                return
            # Cập nhật danh sách đã xử lý
            processed_log_indices.update(new_logs.index.tolist())

            if 'specific_rule' in new_logs.columns:
                grouped = new_logs.groupby('specific_rule')
                iterator = grouped
            else:
                iterator = [(group_name_fallback, new_logs)]

            for rule_name, sub_df in iterator:
                if sub_df.empty: continue

                # Trích xuất chi tiết User/IP
                if 'user' in sub_df.columns and 'client_ip' in sub_df.columns:
                    users_ips = sub_df.groupby(['user', 'client_ip'], observed=True).size().reset_index().apply(
                        lambda x: f"{x['user']}@{x['client_ip']}", axis=1
                    ).unique().tolist()
                elif 'user' in sub_df.columns:
                    users_ips = sub_df['user'].unique().tolist()
                else:
                    users_ips = ["Unknown"]

                # Xử lý cột thời gian (hỗ trợ cả session 'start_time' và log 'timestamp')
                time_col = 'start_time' if 'start_time' in sub_df.columns else 'timestamp'

                # Mô tả (Description) dựa trên tên Rule
                description = f"Detected in group {group_name_fallback}"
                if "SQL Injection" in rule_name:
                    description = "Contains SQL injection patterns/signatures"
                elif "Sensitive" in rule_name:
                    description = "Unauthorized access to sensitive tables"
                elif "Late Night" in rule_name:
                    description = "Activity outside allowed business hours"
                elif "Brute-force" in rule_name:
                    description = "Multiple failed login attempts followed by success"
                elif "multi_table" in str(rule_name) or "Multi" in group_name_fallback:
                    description = "Session accessing multiple distinct tables rapidly"

                current_batch_summary.append({
                    'title': str(rule_name),
                    'count': len(sub_df),
                    'first_time': sub_df[time_col].min(),
                    'last_time': sub_df[time_col].max(),
                    'desc': description,
                    'targets': users_ips
                })

    # 1. Technical Attacks (SQLi, DoS...)
    add_violation_from_group(results.get("rule_technical"), "Technical Attack")

    # 2. Data Destruction
    add_violation_from_group(results.get("rule_destruction"), "Data Destruction")

    # 3. Insider Threats
    add_violation_from_group(results.get("rule_insider"), "Insider Threat")

    # 4. Access Anomalies
    add_violation_from_group(results.get("rule_access"), "Access Anomaly")

    # 5. Behavior & Multi-table
    add_violation_from_group(results.get("rule_behavior_profile"), "Behavioral Anomaly")
    add_violation_from_group(results.get("rule_multi_table"), "Multi-Table Scanning")

    # --- LOGIC GỬI THREAD GIỮ NGUYÊN ---
    if current_batch_summary:
        PENDING_VIOLATIONS.extend(current_batch_summary)

    now = datetime.now()
    time_since_last = (now - LAST_EMAIL_SENT_TIME).total_seconds()

    if PENDING_VIOLATIONS and (time_since_last > EMAIL_COOLDOWN_SECONDS):
        data_to_send = PENDING_VIOLATIONS.copy()
        PENDING_VIOLATIONS.clear()
        LAST_EMAIL_SENT_TIME = now

        email_thread = threading.Thread(
            target=send_email_thread_worker,
            args=(data_to_send,)
        )
        email_thread.daemon = True
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
    global is_running
    
    r = connect_redis()
    
    logging.info(f"Initializing Consumer Group: {REDIS_GROUP_ENGINE}")
    for stream in STREAMS.values():
        ensure_group(r, stream, REDIS_GROUP_ENGINE)

    ensure_group(r, "uba:logs:mysql", REDIS_GROUP_ENGINE)
    logging.info("Realtime UBA Engine STARTED — Monitoring MySQL Performance Schema")

    while is_running:
        try:
            # Check if Redis connection is still valid
            if not r:
                logging.warning("⚠️ Redis connection is None, reconnecting...")
                r = connect_redis()
                if not r:
                    time.sleep(5)
                    continue
            
            msgs = r.xreadgroup(
                groupname=REDIS_GROUP_ENGINE,
                consumername=REDIS_CONSUMER_NAME,
                streams=STREAMS,
                count=10000,
                block=50000
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
        
        except ResponseError as e:
            # Redis stream/group errors (NOGROUP, etc.) - recreate consumer groups
            if "NOGROUP" in str(e):
                logging.warning(f"Consumer group missing: {e}")
                logging.info("🔄 Recreating consumer groups...")
                try:
                    for stream in STREAMS.values():
                        ensure_group(r, stream, REDIS_GROUP_ENGINE)
                    ensure_group(r, "uba:logs:mysql", REDIS_GROUP_ENGINE)
                    logging.info("✅ Consumer groups recreated")
                except Exception as group_error:
                    logging.error(f"Failed to recreate groups: {group_error}")
                    time.sleep(2)
            else:
                logging.error(f"Redis response error: {e}")
                time.sleep(1)
        
        except (RedisConnectionError, ConnectionResetError, BrokenPipeError) as e:
            # Redis connection errors - attempt reconnection
            logging.error(f"Redis connection error: {e}")
            logging.info("🔄 Attempting to reconnect to Redis...")
            time.sleep(3)
            try:
                if r:
                    r.close()  # Close the broken connection
                r = connect_redis()
                if r:
                    logging.info("✅ Redis reconnection successful")
                    # Re-ensure consumer groups after reconnection
                    for stream in STREAMS.values():
                        ensure_group(r, stream, REDIS_GROUP_ENGINE)
                    ensure_group(r, "uba:logs:mysql", REDIS_GROUP_ENGINE)
                else:
                    logging.error("❌ Redis reconnection failed, will retry...")
            except Exception as reconnect_error:
                logging.error(f"Redis reconnect error: {reconnect_error}")
        
        except Exception as e:
            # Other unexpected errors
            logging.error(f"Unexpected engine error: {e}", exc_info=True)
            time.sleep(1)

if __name__ == "__main__":
    start_engine()