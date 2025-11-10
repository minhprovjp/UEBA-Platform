# run_simulation.py (VERSION 4.0 - FINAL & SELF-HEALING)
#
# PHIÊN BẢN HOÀN CHỈNH:
#   - Tự động "giết và thay thế" các kết nối bị lỗi để đảm bảo mô phỏng luôn tiếp tục.
#   - Giữ lại tất cả các cơ chế bảo vệ trước đó.

import os
import sys
import json
import time
import logging
import random
import pandas as pd
import mysql.connector
from redis import Redis, ConnectionError as RedisConnectionError
from datetime import datetime, timedelta, time as dt_time

# Thêm thư mục gốc
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import *
    from engine.utils import save_logs_to_parquet
except ImportError:
    print("Lỗi: Không thể import 'config' hoặc 'engine.utils'.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [Simulator] - %(message)s"
)
log = logging.getLogger("Simulator")

# ==============================================================================
# CONFIG MÔ PHỎNG
# ==============================================================================
SIMULATION_START_TIME = datetime(2025, 11, 10, 7, 0, 0)
SIMULATION_DURATION_DAYS = 7
SESSIONS_PER_HOUR_BASE = 50
WORK_START = dt_time(7, 0)
WORK_END = dt_time(17, 0)
OVERTIME_END = dt_time(20, 0)

# Các cờ bật/tắt kịch bản giữ nguyên...
ENABLE_COMPROMISED_ACCOUNT = True
ENABLE_SCENARIO_PRIVILEGE_ABUSE = True
ENABLE_SCENARIO_DATA_LEAKAGE = True
ENABLE_SCENARIO_OT_IP_THEFT = True
ENABLE_SCENARIO_SABOTAGE = True
ENABLE_SCENARIO_PRIV_ESCALATION = True
ENABLE_SCENARIO_PRIVESC_DAY = True
ENABLE_INSIDER_BEHAVIOR = True
ALWAYS_WORK_HOURS_ONLY = False
ALLOW_DAYTIME_ATTACK = False

compromise_tracker = {}
BASE_COMPROMISE_PROB = 0.02
WEEKEND_COMPROMISE_PROB = 0.15
MAX_COMPROMISE_PER_HOUR = 2
ATTACK_DAY_PROB = 0.30
attack_calendar = {}
DAYTIME_COMPROMISE_PROB = 0.001

MALICIOUS_IP_POOL = ["103.77.161.88", "113.21.55.88", "185.220.101.4", "100.71.22.9"]
SANDBOX_HOST = os.getenv("SANDBOX_DB_HOST", "localhost")
SANDBOX_PORT = os.getenv("SANDBOX_DB_PORT", "3306")

USER_CONNECTIONS = {
    "sales_anh":    {"user": "anh_sales", "password": os.getenv("SANDBOX_USER_ANH_SALES"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "sales_linh":   {"user": "linh_sales", "password": os.getenv("SANDBOX_USER_LINH_SALES"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "sales_quang":  {"user": "quang_sales", "password": os.getenv("SANDBOX_USER_QUANG_SALES"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "sales_trang":  {"user": "trang_sales", "password": os.getenv("SANDBOX_USER_TRANG_SALES"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "mkt_binh":     {"user": "binh_mkt", "password": os.getenv("SANDBOX_USER_BINH_MKT"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "mkt_mai":      {"user": "mai_mkt", "password": os.getenv("SANDBOX_USER_MAI_MKT"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "mkt_vy":       {"user": "vy_mkt", "password": os.getenv("SANDBOX_USER_VY_MKT"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "hr_chi":       {"user": "chi_hr", "password": os.getenv("SANDBOX_USER_CHI_HR"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "hr_db"},
    "hr_hoa":       {"user": "hoa_hr", "password": os.getenv("SANDBOX_USER_HOA_HR"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "hr_db"},
    "support_dung": {"user": "dung_support", "password": os.getenv("SANDBOX_USER_DUNG_SUPPORT"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "support_loan": {"user": "loan_support", "password": os.getenv("SANDBOX_USER_LOAN_SUPPORT"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "support_khang":{"user": "khang_support", "password": os.getenv("SANDBOX_USER_KHANG_SUPPORT"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "dev_em":       {"user": "em_dev", "password": os.getenv("SANDBOX_USER_EM_DEV"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "dev_tam":      {"user": "tam_dev", "password": os.getenv("SANDBOX_USER_TAM_DEV"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "dev_ly":       {"user": "ly_data", "password": os.getenv("SANDBOX_USER_LY_DATA"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "dev_quoc":     {"user": "quoc_app", "password": os.getenv("SANDBOX_USER_QUOC_APP"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "dev_dave":     {"user": "dave_dev", "password": os.getenv("SANDBOX_USER_DAVE_DEV"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "it_thanh":     {"user": "thanh_admin", "password": os.getenv("SANDBOX_USER_THANH_ADMIN"), "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "mysql"},
}

EMPLOYEES = {
    "sales_anh": {"username": "anh_sales", "persona": "Sales_Normal", "dept": "Sales", "ip": "192.168.1.10", "alt_ips": [], "normal_hours": (dt_time(8,30), dt_time(17,30)), "overtime_prob": 0.15},
    "sales_linh": {"username": "linh_sales", "persona": "Sales_Normal", "dept": "Sales", "ip": "192.168.1.21", "alt_ips": [], "normal_hours": (dt_time(9,0), dt_time(18,0)), "overtime_prob": 0.10},
    "sales_quang": {"username": "quang_sales", "persona": "Sales_Normal", "dept": "Sales", "ip": "192.168.1.22", "alt_ips": [], "normal_hours": (dt_time(8,0), dt_time(17,0)), "overtime_prob": 0.05},
    "sales_trang": {"username": "trang_sales", "persona": "Sales_Normal", "dept": "Sales", "ip": "192.168.1.23", "alt_ips": [], "normal_hours": (dt_time(10,0), dt_time(19,0)), "overtime_prob": 0.30},
    "mkt_binh": {"username": "binh_mkt", "persona": "Sales_Normal", "dept": "Marketing", "ip": "192.168.1.11", "alt_ips": [], "normal_hours": (dt_time(9,0), dt_time(18,0)), "overtime_prob": 0.05},
    "mkt_mai": {"username": "mai_mkt", "persona": "Sales_Normal", "dept": "Marketing", "ip": "192.168.1.24", "alt_ips": [], "normal_hours": (dt_time(10,0), dt_time(20,0)), "overtime_prob": 0.40},
    "mkt_vy": {"username": "vy_mkt", "persona": "Sales_Normal", "dept": "Marketing", "ip": "192.168.1.25", "alt_ips": [], "normal_hours": (dt_time(8,0), dt_time(17,0)), "overtime_prob": 0.10},
    "hr_chi": {"username": "chi_hr", "persona": "HR_Normal", "dept": "HR", "ip": "192.168.1.12", "alt_ips": [], "normal_hours": (dt_time(8,0), dt_time(17,0)), "overtime_prob": 0.01},
    "hr_hoa": {"username": "hoa_hr", "persona": "HR_Normal", "dept": "HR", "ip": "192.168.1.26", "alt_ips": [], "normal_hours": (dt_time(9,0), dt_time(19,0)), "overtime_prob": 0.30},
    "support_dung": {"username": "dung_support", "persona": "Sales_Normal", "dept": "Support", "ip": "192.168.1.13", "alt_ips": [], "normal_hours": (dt_time(7,30), dt_time(19,0)), "overtime_prob": 0.20},
    "support_loan": {"username": "loan_support", "persona": "Sales_Normal", "dept": "Support", "ip": "192.168.1.27", "alt_ips": [], "normal_hours": (dt_time(7,0), dt_time(15,0)), "overtime_prob": 0.05},
    "support_khang": {"username": "khang_support", "persona": "Sales_Normal", "dept": "Support", "ip": "192.168.1.28", "alt_ips": ["100.71.22.9"], "normal_hours": (dt_time(12,0), dt_time(20,0)), "overtime_prob": 0.35},
    "dev_em": {"username": "em_dev", "persona": "Sales_Normal", "dept": "Engineering", "ip": "192.168.1.15", "alt_ips": [], "normal_hours": (dt_time(10,0), dt_time(20,0)), "overtime_prob": 0.40},
    "dev_tam": {"username": "tam_dev", "persona": "Sales_Normal", "dept": "Engineering", "ip": "192.168.1.29", "alt_ips": [], "normal_hours": (dt_time(11,0), dt_time(20,0)), "overtime_prob": 0.35},
    "dev_ly": {"username": "ly_data", "persona": "Sales_Normal", "dept": "Engineering", "ip": "192.168.1.30", "alt_ips": [], "normal_hours": (dt_time(9,30), dt_time(18,30)), "overtime_prob": 0.20},
    "dev_quoc": {"username": "quoc_app", "persona": "Sales_Normal", "dept": "Engineering", "ip": "192.168.1.31", "alt_ips": [], "normal_hours": (dt_time(12,0), dt_time(22,0)), "overtime_prob": 0.50},
    "dev_dave": {"username": "dave_dev", "persona": "Sales_Normal", "dept": "Engineering", "ip": "192.168.1.16", "alt_ips": ["113.21.55.88"], "normal_hours": (dt_time(10,0), dt_time(18,0)), "overtime_prob": 0.35},
    "it_thanh": {"username": "thanh_admin", "persona": "ITAdmin_Normal", "dept": "IT", "ip": "192.168.1.2", "alt_ips": ["185.220.101.4"], "normal_hours": (dt_time(6,30), dt_time(19,0)), "overtime_prob": 0.30},
}

STREAM_KEY = f"{REDIS_STREAM_LOGS}:mysql"

# ==============================================================================
# HÀM TIỆN ÍCH
# ==============================================================================
def load_query_library():
    path = "engine/query_library.json"
    try:
        with open(path, 'r', encoding='utf-8') as f:
            log.info(f"Đã tải thư viện truy vấn từ {path}")
            return json.load(f)
    except Exception as e:
        log.critical(f"Không thể tải '{path}'. Hãy chạy 'build_query_library.py' trước. Lỗi: {e}")
        sys.exit(1)

def get_db_connection(user_key):
    creds = USER_CONNECTIONS.get(user_key)
    if not creds:
        log.warning(f"Không tìm thấy thông tin kết nối cho user_key: {user_key}")
        return None
    try:
        conn = mysql.connector.connect(**creds)
        return conn
    except mysql.connector.Error as err:
        log.error(f"Lỗi kết nối MySQL cho user {user_key}: {err}")
        return None

def connect_redis():
    while True:
        try:
            r = Redis.from_url(REDIS_URL, decode_responses=True)
            r.ping()
            log.info("Kết nối Redis (Simulator) thành công.")
            return r
        except RedisConnectionError as e:
            log.error(f"Kết nối Redis (Simulator) thất bại: {e}. Thử lại sau 5 giây...")
            time.sleep(5)

def is_time_in_range(t, start, end): return start <= t <= end if start <= end else t >= start or t <= end
def rand_customer_id(): return random.randint(1, 500)
def rand_employee_id(): return random.randint(1, 100)
def rand_order_id(): return random.randint(1, 10000)
def rand_prod_sku(): return f"SKU{random.randint(1,100):03d}"

# ==============================================================================
# HÀM LÕI: THỰC THI VÀ ĐO LƯỜNG (PHIÊN BẢN "SIÊU BỀN VỮNG")
# ==============================================================================
def execute_query_and_measure(conn, query_template: str, sim_time_dt: datetime, client_ip: str):
    params = ()
    try:
        num_params = query_template.count('%s')
        if num_params > 0:
            all_possible_params = (rand_customer_id(), rand_order_id(), rand_prod_sku(), rand_employee_id())
            params = all_possible_params[:num_params]
    except Exception as e:
        log.warning(f"Lỗi khi chuẩn bị tham số cho query: {query_template}. Lỗi: {e}")
        return None

    start_timer = time.perf_counter()
    rows_returned = 0
    rows_affected = 0
    statement = query_template
    
    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        results_iterator = cursor.execute(query_template, params, True)
        
        if results_iterator:
            for result in results_iterator:
                if result.with_rows:
                    rows_returned += len(result.fetchall())
        
        statement = cursor.statement or query_template
        rows_affected = cursor.rowcount if cursor.rowcount != -1 else 0

    except mysql.connector.Error as err:
        log.warning(f"Lỗi khi thực thi SQL (user: {conn.user}): {err.msg}. Vẫn ghi log.")
        statement = getattr(cursor, 'statement', query_template)
        # SỬA LỖI: Trả về None để báo hiệu kết nối hỏng
        return None

    except Exception as e:
        log.error(f"Lỗi execute_query_and_measure không xác định (user: {conn.user}): {e}", exc_info=True)
        return None
    finally:
        if cursor:
            try:
                cursor.close()
            except mysql.connector.errors.InternalError as ie:
                if "Unread result found" in str(ie):
                    log.warning(f"Bỏ qua lỗi 'Unread result' khi đóng cursor cho user: {conn.user}.")
                else:
                    log.error(f"Lỗi InternalError không mong muốn khi đóng cursor: {ie}")
        
    end_timer = time.perf_counter()
    execution_time_ms = (end_timer - start_timer) * 1000

    db_name = "N/A"
    try:
        db_name = conn.database or "N/A"
    except mysql.connector.Error:
        log.warning(f"Connection cho user {conn.user} bị lỗi, không thể lấy tên DB.")

    record = {
        "timestamp": sim_time_dt.isoformat() + "Z",
        "user": conn.user or "unknown",
        "client_ip": client_ip,
        "database": db_name,
        "query": statement,
        "source_dbms": "MySQL_Simulated",
        "execution_time_ms": round(execution_time_ms, 3),
        "rows_returned": int(rows_returned),
        "rows_affected": int(rows_affected)
    }
    return record

# ==============================================================================
# BỘ LẬP LỊCH VÀ CHỌN VAI TRÒ
# ==============================================================================
def choose_session_ip(user_info, is_workday, is_off_hours, is_compromised):
    if is_compromised: return random.choice(MALICIOUS_IP_POOL)
    ip = user_info["ip"]
    if user_info.get("alt_ips") and (not is_workday or is_off_hours) and random.random() < 0.4:
        ip = random.choice(user_info["alt_ips"])
    return ip

def choose_actor(current_dt):
    # (Hàm này không thay đổi)
    is_workday = current_dt.weekday() < 5
    hm = dt_time(current_dt.hour, current_dt.minute)
    hour = current_dt.hour
    is_overtime = is_workday and (WORK_END.hour <= hour < OVERTIME_END.hour)
    is_off_hours = (not is_workday) or (hour < WORK_START.hour) or (hour >= OVERTIME_END.hour)
    candidates, weights = [], []
    for key, info in EMPLOYEES.items():
        start, end = info["normal_hours"]
        w = 0.0
        if is_time_in_range(hm, start, end): w += 1.0
        if is_overtime: w += info.get("overtime_prob", 0.0)
        if not is_workday and info["dept"] not in ("IT", "Engineering", "Support"): w *= 0.1
        if w > 0: candidates.append(key); weights.append(w)
    today_key = current_dt.date().isoformat()
    attacker_allowed = attack_calendar.get(today_key, False)
    compromised, mode = False, "none"
    if is_off_hours:
        chosen_key = random.choices(candidates, weights=weights, k=1)[0] if candidates else random.choice(["it_thanh", "dev_dave"])
        if ENABLE_COMPROMISED_ACCOUNT and attacker_allowed and (2 <= hour < 5):
            hour_key = current_dt.strftime("%Y-%m-%d %H")
            if compromise_tracker.get(hour_key, 0) < MAX_COMPROMISE_PER_HOUR:
                prob = WEEKEND_COMPROMISE_PROB if not is_workday else BASE_COMPROMISE_PROB
                if random.random() < prob:
                    compromised, mode = True, "loud" if not is_workday else "stealth"
                    compromise_tracker[hour_key] = compromise_tracker.get(hour_key, 0) + 1
    else:
        chosen_key = random.choices(candidates, weights=weights, k=1)[0] if candidates else "dev_em"
        if ENABLE_COMPROMISED_ACCOUNT and ALLOW_DAYTIME_ATTACK and is_workday and attacker_allowed and (WORK_START.hour <= hour < WORK_END.hour):
            if random.random() < DAYTIME_COMPROMISE_PROB:
                hour_key = current_dt.strftime("%Y-%m-%d %H")
                if compromise_tracker.get(hour_key, 0) < MAX_COMPROMISE_PER_HOUR:
                    compromised, mode = True, "stealth"
                    compromise_tracker[hour_key] = compromise_tracker.get(hour_key, 0) + 1
    return chosen_key, EMPLOYEES[chosen_key], is_workday, is_overtime, is_off_hours, compromised, mode

def generate_schedule():
    # (Hàm này không thay đổi)
    sessions, current_time = [], SIMULATION_START_TIME
    end_time = SIMULATION_START_TIME + timedelta(days=SIMULATION_DURATION_DAYS)
    day = SIMULATION_START_TIME.date()
    while day <= end_time.date():
        if str(day) not in attack_calendar: attack_calendar[str(day)] = (random.random() < ATTACK_DAY_PROB)
        day += timedelta(days=1)
    while current_time < end_time:
        hour, is_workday = current_time.hour, current_time.weekday() < 5
        if ALWAYS_WORK_HOURS_ONLY and (not is_workday or not (WORK_START.hour <= hour < WORK_END.hour)):
            current_time += timedelta(hours=1); continue
        if (9 <= hour < 12 or 14 <= hour < 16) and is_workday: m = (1.0, 1.5)
        elif WORK_START.hour <= hour < WORK_END.hour and is_workday: m = (0.8, 1.0)
        else: m = (0.1, 0.5)
        for _ in range(int(SESSIONS_PER_HOUR_BASE*random.uniform(*m))): sessions.append(current_time + timedelta(seconds=random.randint(0,3599)))
        current_time += timedelta(hours=1)
    sessions.sort()
    return sessions

# ==============================================================================
# HÀM MÔ PHỎNG CHÍNH
# ==============================================================================

def run_simulation():
    query_library = load_query_library()
    redis_client = connect_redis()
    all_generated_logs = []
    
    log.info(f"Bắt đầu tạo lịch trình (schedule) cho {SIMULATION_DURATION_DAYS} ngày...")
    schedule = generate_schedule()
    log.info(f"Đã tạo {len(schedule)} phiên (session) mô phỏng. Bắt đầu chạy...")
    
    active_connections = {}
    with open("simulation_labels.csv", "w", encoding="utf-8") as label_file:
        label_file.write("timestamp,user,label,query\n")

        for sim_time_dt in schedule:
            actor_key, actor_info, is_workday, is_ot, is_off, compromised, compromised_mode = choose_actor(sim_time_dt)
            
            if actor_key not in active_connections:
                conn = get_db_connection(actor_key)
                if not conn: continue
                active_connections[actor_key] = conn
            conn = active_connections[actor_key]

            # (Logic chọn persona, label giữ nguyên)
            persona_key = actor_info["persona"]
            is_anomaly, label = False, "Normal"
            if compromised:
                is_anomaly = True
                if compromised_mode == "stealth": persona_key, label = "Insider_DataLeak", "Compromised_Stealth"
                else:
                    persona_key = random.choice(["Insider_PrivEsc", "Insider_Sabotage", "Insider_DOS"])
                    label = f"Compromised_Loud_{persona_key}"
            elif actor_info["username"] == "dave_dev" and (is_ot or is_off) and ENABLE_INSIDER_BEHAVIOR:
                if ENABLE_SCENARIO_DATA_LEAKAGE and random.random() < 0.2: is_anomaly, persona_key, label = True, "Insider_DataLeak", "Insider_DataLeak"
                elif ENABLE_SCENARIO_SABOTAGE and random.random() < 0.1: is_anomaly, persona_key, label = True, "Insider_Sabotage", "Insider_Sabotage"
                elif ENABLE_SCENARIO_PRIV_ESCALATION and random.random() < 0.1: is_anomaly, persona_key, label = True, "Insider_PrivEsc", "Insider_PrivEsc"
                elif random.random() < 0.05: is_anomaly, persona_key, label = True, "Insider_DOS", "Insider_DOS"
            elif actor_info["dept"] == "Engineering" and is_workday and ENABLE_SCENARIO_PRIVESC_DAY and random.random() < 0.005:
                is_anomaly, persona_key, label = True, "Insider_PrivEsc", "Developer_PrivEsc_Day"

            session_ip = choose_session_ip(actor_info, is_workday, is_off, compromised)
            
            if persona_key not in query_library or not query_library[persona_key]:
                log.warning(f"Không có truy vấn trong thư viện cho: {persona_key}. Bỏ qua."); continue
            query_template = random.choice(query_library[persona_key])

            log_record = execute_query_and_measure(conn, query_template, sim_time_dt, session_ip)
            
            # SỬA LỖI: Logic "Giết và Thay thế"
            if log_record is None:
                # Nếu hàm trả về None, kết nối đã hỏng. Đóng và xóa nó.
                log.warning(f"Kết nối cho user {actor_key} bị lỗi. Sẽ tạo lại trong lần tới.")
                if actor_key in active_connections:
                    try:
                        active_connections[actor_key].close()
                    except:
                        pass # Bỏ qua lỗi nếu đóng không thành công
                    del active_connections[actor_key]
                continue # Bỏ qua phiên này và đi đến phiên tiếp theo
            
            if is_anomaly:
                label_file.write(f"{log_record['timestamp']},{log_record['user']},{label},\"{log_record['query'].replace('\"', '\"\"')}\"\n")

            try:
                redis_client.xadd(STREAM_KEY, {"data": json.dumps(log_record)})
                all_generated_logs.append(log_record)
            except RedisConnectionError as e:
                log.error(f"Mất kết nối Redis (Simulator): {e}. Đang kết nối lại..."); redis_client = connect_redis()
            except Exception as e:
                log.error(f"Lỗi khi đẩy log: {e}")

    for conn in active_connections.values():
        conn.close()
        
    if all_generated_logs:
        save_logs_to_parquet(all_generated_logs, source_dbms="MySQL_Simulated")
        log.info(f"Đã lưu tổng cộng {len(all_generated_logs)} bản ghi mô phỏng vào Parquet.")

    log.info("✅ Mô phỏng hoàn tất.")
    log.info(f"File nhãn (labels) đã được tạo tại: simulation_labels.csv")

if __name__ == "__main__":
    run_simulation()