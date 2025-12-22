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
    Xử lý gửi email (Passive Response)
    """
    global LAST_EMAIL_SENT_TIME, PENDING_VIOLATIONS

    # 1. Thu thập dữ liệu tóm tắt từ batch hiện tại
    current_batch_summary = []

    # Set để theo dõi các dòng log đã xử lý
    processed_log_indices = set()

    def add_violation_category(df, category_title):
        if df is None or df.empty:
            return

        new_logs = df[~df.index.isin(processed_log_indices)]

        if new_logs.empty:
            return

        # Cập nhật danh sách đã xử lý
        processed_log_indices.update(new_logs.index.tolist())

        # 2. Tổng hợp danh sách các Rule cụ thể đã vi phạm để đưa vào mô tả
        specific_rules_desc = "Detected behaviors: "
        if 'specific_rule' in new_logs.columns:
            # Lấy danh sách các rule unique, loại bỏ None/Rỗng
            unique_rules = new_logs['specific_rule'].dropna().unique().tolist()
            # Làm sạch list
            clean_rules = set()
            for r in unique_rules:
                if r:
                    parts = [p.strip() for p in r.split(';')]
                    clean_rules.update(parts)

            if clean_rules:
                specific_rules_desc += ", ".join(sorted(list(clean_rules)))
            else:
                specific_rules_desc += "General anomaly detected"
        else:
            specific_rules_desc += "Anomaly detected (No specific rule detail)"

        # 3. Trích xuất chi tiết User/IP (Target Aggregation)
        if 'user' in new_logs.columns and 'client_ip' in new_logs.columns:
            users_ips = new_logs.groupby(['user', 'client_ip'], observed=True).size().reset_index().apply(
                lambda x: f"{x['user']}@{x['client_ip']}", axis=1
            ).unique().tolist()
        elif 'user' in new_logs.columns:
            users_ips = new_logs['user'].unique().tolist()
        else:
            users_ips = ["Unknown"]

        # 4. Xử lý thời gian (Time Range Aggregation)
        time_col = 'start_time' if 'start_time' in new_logs.columns else 'timestamp'
        first_time = new_logs[time_col].min()
        last_time = new_logs[time_col].max()

        # 5. Thêm vào danh sách tóm tắt (1 Item duy nhất cho cả nhóm)
        current_batch_summary.append({
            'title': category_title,  # Tiêu đề nhóm (VD: TECHNICAL ATTACKS)
            'count': len(new_logs),  # Tổng số lượng vi phạm
            'first_time': first_time,
            'last_time': last_time,
            'desc': specific_rules_desc,  # Mô tả chứa danh sách các rule cụ thể
            'targets': users_ips
        })

    # --- THỨ TỰ GỌI (ƯU TIÊN ĐỘ NGHIÊM TRỌNG) ---
    # 1. TECHNICAL ATTACKS
    add_violation_category(results.get("rule_technical"), "TECHNICAL ATTACKS")

    # 2. DATA DESTRUCTION
    add_violation_category(results.get("rule_destruction"), "DATA DESTRUCTION")

    # 3. INSIDER THREATS
    add_violation_category(results.get("rule_insider"), "INSIDER THREATS")

    # 4. ACCESS ANOMALIES
    add_violation_category(results.get("rule_access"), "ACCESS ANOMALIES")

    # 5. MULTI-TABLE ACCESS
    # add_violation_category(results.get("rule_multi_table"), "MULTI-TABLE ACCESS")

    # # 6. BEHAVIORAL ANOMALY (Profile deviation, ML)
    # add_violation_category(results.get("rule_behavior_profile"), "BEHAVIORAL ANOMALY")

    # ml_df = results.get("anomalies_ml")
    # if ml_df is not None and not ml_df.empty:
    #     ml_df = ml_df.copy()
    #     if 'specific_rule' not in ml_df.columns:
    #         ml_df['specific_rule'] = 'AI Detected Anomaly'
    #     add_violation_category(ml_df, "BEHAVIORAL ANOMALY")

    # --- LOGIC GỬI THREAD (GIỮ NGUYÊN) ---
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
        # --- BƯỚC 1: ĐỌC CẤU HÌNH ĐỘNG TỪ JSON ---
        # Mỗi lần gửi mail sẽ đọc lại file config mới nhất
        current_config = load_config()
        email_settings = current_config.get("email_alert_config", {})

        # Kiểm tra xem tính năng email có được bật không
        if not email_settings.get("enable_email_alerts", True):
            logger.info("🚫 Email alerts are disabled in configuration.")
            return

        # Lấy thông tin đăng nhập
        smtp_server = email_settings.get("smtp_server")
        smtp_port = email_settings.get("smtp_port")
        sender_email = email_settings.get("sender_email")
        sender_password = email_settings.get("sender_password")
        to_recipients = email_settings.get("to_recipients", [])
        bcc_recipients = email_settings.get("bcc_recipients", [])

        if not sender_email or not sender_password or not to_recipients:
            logger.warning("⚠️ Email configuration is missing in engine_config.json. Skipping alert.")
            return

        # --- BƯỚC 2: GOM NHÓM DỮ LIỆU ---
        aggregated_data = aggregate_violations(summary_data)
        
        # --- BƯỚC 3: TẠO NỘI DUNG TEXT (Fallback) ---
        text_content = "[UEBA ALERT]: Detected abnormal behavior:\n\n"
        for item in aggregated_data:
            text_content += f"⚠ {item['title']} ({item['count']} events)\n"
            text_content += f"   • Target: {item['target_str']}\n"
            text_content += f"   • Time: {item['time_range']}\n"
            text_content += f"   • Desc: {item['desc']}\n\n"

        text_content += "──────────────────────────────\nPlease check Dashboard for details."

        # --- BƯỚC 4: TẠO NỘI DUNG HTML ---
        html_content = generate_html_alert(aggregated_data)

        # --- BƯỚC 5: GỬI EMAIL ---
        email_subject = f"[UEBA ALERT] Detect {len(aggregated_data)} type/s of abnormal behavior"

        # Gọi hàm gửi email với các tham số lấy từ config JSON
        success = send_email_alert(
            subject=email_subject,
            text_content=text_content,
            html_content=html_content,
            to_recipients=to_recipients,      # Lấy từ JSON
            smtp_server=smtp_server,          # Lấy từ JSON
            smtp_port=int(smtp_port),         # Lấy từ JSON (đảm bảo là int)
            sender_email=sender_email,        # Lấy từ JSON
            sender_password=sender_password,  # Lấy từ JSON
            bcc_recipients=bcc_recipients     # Lấy từ JSON
        )

        if success is True:
            logger.info(f"--> [Security Alert Triggered] Sent successfully to {len(to_recipients)} recipients.")
        else:
            logger.error(f"--> [Security Alert] Send failed: {success}")

    except Exception as e:
        logger.error(f"--> [Security Alert] Exception error: {e}", exc_info=True)

def handle_active_responses(results: dict):
    """
    Kiểm tra danh sách user vượt ngưỡng và thực hiện Lock/Kill.
    Args:
        results (dict): Dictionary trả về từ data_processor.
    """
    users_to_lock = results.get("users_to_lock", [])

    if not users_to_lock:
        return  # Không có user nào cần xử lý

    current_config = load_config()
    ar_config = current_config.get("active_response_config", {})
    
    # Kiểm tra công tắc Bật/Tắt
    if not ar_config.get("enable_active_response", True):
        logger.info(f"🚫 Active Response is DISABLED. Skipping action for {len(users_to_lock)} users.")
        return

    admin_user = ACTIVE_RESPONSE_SETTINGS.get('mysql_user', '')

    for offender in users_to_lock:
        user_name = offender['user']
        total_count = offender['total_violation_count']

        # === SAFETY SWITCH ===
        if admin_user and user_name == admin_user:
            logger.warning(f"⚠️ Detected violation on ADMIN user '{user_name}' but ignoring due to safety switch.")
            continue

        custom_reason = offender.get('lock_reason')

        if custom_reason:
            reason = f"Automatic response: {custom_reason}"
        else:
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
                       
                try:
                    handle_email_alerts_async(results)    # Sending Alert (nếu có nội dung)
                except Exception as e:
                    logging.error(f"[Email Error] Error creating email sending thread: {e}", exc_info=True)
                
                try:
                    handle_active_responses(results)   # Active Response (nếu có user vượt ngưỡng)
                except Exception as e:
                    logging.error(f"[Active Response Error] Error while executing Lock/Kill: {e}", exc_info=True)
                
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