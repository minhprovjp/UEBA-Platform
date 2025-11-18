# run_simulation.py
#
# PHIÊN BẢN HOÀN CHỈNH:
#   - Sửa lỗi 1: Ghi cứng (hardcode) USER_CONNECTIONS với password="password"
#   - Sửa lỗi 2: Sửa lỗi 'TypeError' (multi=True -> True)
#   - Sửa lỗi 3: Sửa lỗi 'AttributeError' (next_resultset -> next_result)

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
    # Vẫn import config, nhưng CHỈ để lấy các giá trị
    # như REDIS_URL, STREAM_KEY, và đường dẫn STAGING_DATA_DIR
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
# CONFIG MÔ PHỎNG (Lấy từ file 'generate_simulated_general_log.py')
# ==============================================================================
SIMULATION_START_TIME = datetime(2025, 11, 15, 7, 0, 0)
SIMULATION_DURATION_DAYS = 7
SESSIONS_PER_HOUR_BASE = 50
WORK_START = dt_time(7, 0)
WORK_END = dt_time(17, 0)
OVERTIME_END = dt_time(20, 0)

# --- Bật/Tắt kịch bản ---
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

# --- Biến toàn cục cho bộ lập lịch ---
compromise_tracker = {}
BASE_COMPROMISE_PROB = 0.02
WEEKEND_COMPROMISE_PROB = 0.15
MAX_COMPROMISE_PER_HOUR = 2
ATTACK_DAY_PROB = 0.30
attack_calendar = {}
DAYTIME_COMPROMISE_PROB = 0.001

MALICIOUS_IP_POOL = ["103.77.161.88", "113.21.55.88", "185.220.101.4", "100.71.22.9"]

# === NÂNG CẤP: GHI CỨNG (HARDCODE) THÔNG TIN SANDBOX ===
SANDBOX_HOST = "localhost"
SANDBOX_PORT = 3306
SANDBOX_PASSWORD = "password" # Mật khẩu chuẩn cho TẤT CẢ user

USER_CONNECTIONS = {
    # Sales
    "sales_anh":    {"user": "anh_sales", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "sales_linh":   {"user": "linh_sales", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "sales_quang":  {"user": "quang_sales", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "sales_trang":  {"user": "trang_sales", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    # Marketing
    "mkt_binh":     {"user": "binh_mkt", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "mkt_mai":      {"user": "mai_mkt", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "mkt_vy":       {"user": "vy_mkt", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    # HR
    "hr_chi":       {"user": "chi_hr", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "hr_db"},
    "hr_hoa":       {"user": "hoa_hr", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "hr_db"},
    # Support
    "support_dung": {"user": "dung_support", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "support_loan": {"user": "loan_support", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "support_khang":{"user": "khang_support", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    # Dev / Engineering
    "dev_em":       {"user": "em_dev", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "dev_tam":      {"user": "tam_dev", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "dev_ly":       {"user": "ly_data", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "dev_quoc":     {"user": "quoc_app", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    "dev_dave":     {"user": "dave_dev", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "sales_db"},
    # IT Admin
    "it_thanh":     {"user": "thanh_admin", "password": SANDBOX_PASSWORD, "host": SANDBOX_HOST, "port": SANDBOX_PORT, "database": "mysql"},
}
# === KẾT THÚC NÂNG CẤP ===

# === ĐIỀN ĐẦY ĐỦ EMPLOYEES (VÀ THÊM IP) ===
EMPLOYEES = {
    # -------- Sales team (4 người) --------
    "sales_anh": {
        "username": "anh_sales", "persona": "Sales_Normal", "dept": "Sales", "ip": "192.168.1.10", "alt_ips": [],
        "normal_hours": (dt_time(8,30), dt_time(17,30)), "overtime_prob": 0.15, "priv_compromise_profile": "low",
    },
    "sales_linh": {
        "username": "linh_sales", "persona": "Sales_Normal", "dept": "Sales", "ip": "192.168.1.21", "alt_ips": [],
        "normal_hours": (dt_time(9,0), dt_time(18,0)), "overtime_prob": 0.10, "priv_compromise_profile": "low",
    },
    "sales_quang": {
        "username": "quang_sales", "persona": "Sales_Normal", "dept": "Sales", "ip": "192.168.1.22", "alt_ips": [],
        "normal_hours": (dt_time(8,0), dt_time(17,0)), "overtime_prob": 0.05, "priv_compromise_profile": "low",
    },
    "sales_trang": {
        "username": "trang_sales", "persona": "Sales_Normal", "dept": "Sales", "ip": "192.168.1.23", "alt_ips": [],
        "normal_hours": (dt_time(10,0), dt_time(19,0)), "overtime_prob": 0.30, "priv_compromise_profile": "low",
    },
    # -------- Marketing team (3 người) --------
    "mkt_binh": {
        "username": "binh_mkt", "persona": "Sales_Normal", "dept": "Marketing", "ip": "192.168.1.11", "alt_ips": [],
        "normal_hours": (dt_time(9,0), dt_time(18,0)), "overtime_prob": 0.05, "priv_compromise_profile": "low",
    },
    "mkt_mai": {
        "username": "mai_mkt", "persona": "Sales_Normal", "dept": "Marketing", "ip": "192.168.1.24", "alt_ips": [],
        "normal_hours": (dt_time(10,0), dt_time(20,0)), "overtime_prob": 0.40, "priv_compromise_profile": "low",
    },
    "mkt_vy": {
        "username": "vy_mkt", "persona": "Sales_Normal", "dept": "Marketing", "ip": "192.168.1.25", "alt_ips": [],
        "normal_hours": (dt_time(8,0), dt_time(17,0)), "overtime_prob": 0.10, "priv_compromise_profile": "low",
    },
    # -------- HR team (2 người) --------
    "hr_chi": {
        "username": "chi_hr", "persona": "HR_Normal", "dept": "HR", "ip": "192.168.1.12", "alt_ips": [],
        "normal_hours": (dt_time(8,0), dt_time(17,0)), "overtime_prob": 0.01, "priv_compromise_profile": "sensitive",
    },
    "hr_hoa": {
        "username": "hoa_hr", "persona": "HR_Normal", "dept": "HR", "ip": "192.168.1.26", "alt_ips": [],
        "normal_hours": (dt_time(9,0), dt_time(19,0)), "overtime_prob": 0.30, "priv_compromise_profile": "sensitive",
    },
    # -------- Support team (3 người) --------
    "support_dung": {
        "username": "dung_support", "persona": "Sales_Normal", "dept": "Support", "ip": "192.168.1.13", "alt_ips": [],
        "normal_hours": (dt_time(7,30), dt_time(19,0)), "overtime_prob": 0.20, "priv_compromise_profile": "low",
    },
    "support_loan": {
        "username": "loan_support", "persona": "Sales_Normal", "dept": "Support", "ip": "192.168.1.27", "alt_ips": [],
        "normal_hours": (dt_time(7,0), dt_time(15,0)), "overtime_prob": 0.05, "priv_compromise_profile": "low",
    },
    "support_khang": {
        "username": "khang_support", "persona": "Sales_Normal", "dept": "Support", "ip": "192.168.1.28", "alt_ips": ["100.71.22.9"],
        "normal_hours": (dt_time(12,0), dt_time(20,0)), "overtime_prob": 0.35, "priv_compromise_profile": "low",
    },
    # -------- Engineering / Dev team (5 người) --------
    "dev_em": {
        "username": "em_dev", "persona": "Sales_Normal", "dept": "Engineering", "ip": "192.168.1.15", "alt_ips": [],
        "normal_hours": (dt_time(10,0), dt_time(20,0)), "overtime_prob": 0.40, "priv_compromise_profile": "low",
    },
    "dev_tam": {
        "username": "tam_dev", "persona": "Sales_Normal", "dept": "Engineering", "ip": "192.168.1.29", "alt_ips": [],
        "normal_hours": (dt_time(11,0), dt_time(20,0)), "overtime_prob": 0.35, "priv_compromise_profile": "low",
    },
    "dev_ly": {
        "username": "ly_data", "persona": "Sales_Normal", "dept": "Engineering", "ip": "192.168.1.30", "alt_ips": [],
        "normal_hours": (dt_time(9,30), dt_time(18,30)), "overtime_prob": 0.20, "priv_compromise_profile": "low",
    },
    "dev_quoc": {
        "username": "quoc_app", "persona": "Sales_Normal", "dept": "Engineering", "ip": "192.168.1.31", "alt_ips": [],
        "normal_hours": (dt_time(12,0), dt_time(22,0)), "overtime_prob": 0.50, "priv_compromise_profile": "low",
    },
    "dev_dave": {
        "username": "dave_dev", "persona": "Sales_Normal", "dept": "Engineering", "ip": "192.168.1.16", "alt_ips": ["113.21.55.88"],
        "normal_hours": (dt_time(10,0), dt_time(18,0)), "overtime_prob": 0.35, "priv_compromise_profile": "low",
    },
    # -------- IT Admin (1 người) --------
    "it_thanh": {
        "username": "thanh_admin", "persona": "ITAdmin_Normal", "dept": "IT", "ip": "192.168.1.2", "alt_ips": ["185.220.101.4"],
        "normal_hours": (dt_time(6,30), dt_time(19,0)), "overtime_prob": 0.30, "priv_compromise_profile": "admin",
    },
}

# Hàng đợi Redis (Đọc từ config.py)
STREAM_KEY = f"{REDIS_STREAM_LOGS}:mysql"

# ==============================================================================
# HÀM TIỆN ÍCH MÔ PHỎNG
# ==============================================================================
# (Hàm load_query_library, connect_redis, is_time_in_range, và các hàm rand_... giữ nguyên)
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
    
    connect_config = creds.copy()
    if connect_config.get("database") == "mysql":
        connect_config.pop("database", None)
        
    try:
        conn = mysql.connector.connect(**connect_config)
        conn.autocommit = True
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

def is_time_in_range(t, start, end):
    if start <= end: return start <= t <= end
    else: return t >= start or t <= end

def rand_customer_id(): return random.randint(1, 500)
def rand_employee_id(): return random.randint(1, 100)
def rand_order_id(): return random.randint(1, 10000)
def rand_prod_sku(): return f"SKU{random.randint(1,100):03d}"

# ==============================================================================
# HÀM LÕI: THỰC THI VÀ ĐO LƯỜNG
# ==============================================================================

def execute_query_and_measure(conn, user_key: str, query_template: str, sim_time_dt: datetime, client_ip: str):
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
    rows_returned, rows_affected = 0, 0
    statement = query_template
    
    cursor = None
    try:
        cursor = conn.cursor(dictionary=True, buffered=True)
        # SỬA LỖI: Dùng tham số vị trí `True` thay vì keyword `multi=True`
        results_iterator = cursor.execute(query_template, params)
        if cursor.with_rows:
            rows = cursor.fetchall()
            rows_returned = len(rows)
        rows_affected = cursor.rowcount if cursor.rowcount != -1 else 0

        statement = cursor.statement or query_template
        
    except mysql.connector.Error as err:
        log.warning(f"Lỗi khi thực thi SQL (user: {user_key}): {err.msg}.")
        # TRẢ VỀ NONE ĐỂ BÁO HIỆU LỖI
        return None

    except Exception as e:
        log.error(f"Lỗi execute_query_and_measure không xác định (user: {user_key}): {e}", exc_info=True)
        return None
    finally:
        if cursor:
            try:
                cursor.close()
            except mysql.connector.errors.InternalError as ie:
                if "Unread result found" in str(ie):
                    log.warning(f"Bỏ qua lỗi 'Unread result' khi đóng cursor cho user: {user_key}.")
                else:
                    log.error(f"Lỗi InternalError không mong muốn khi đóng cursor: {ie}")
        
    end_timer = time.perf_counter()
    execution_time_ms = (end_timer - start_timer) * 1000

    db_name = "N/A"
    try:
        db_name = conn.database or "N/A"
    except mysql.connector.Error:
        log.warning(f"Connection cho user {user_key} bị lỗi, không thể lấy tên DB.")

    record = {
        "timestamp": sim_time_dt.isoformat() + "Z", "user": user_key, "client_ip": client_ip,
        "database": db_name, "query": statement, "source_dbms": "MySQL_Simulated",
        "execution_time_ms": round(execution_time_ms, 3), "rows_returned": int(rows_returned),
        "rows_affected": int(rows_affected)
    }
    return record

# ==============================================================================
# BỘ LẬP LỊCH VÀ CHỌN VAI TRÒ (Lấy từ file gốc)
# ==============================================================================
# (Toàn bộ hàm 'choose_session_ip', 'choose_actor', 'generate_schedule' giữ nguyên)
def choose_session_ip(user_info, is_workday, is_off_hours, is_compromised):
    """Quyết định IP sẽ được sử dụng cho session (phiên) này."""
    if is_compromised:
        return random.choice(MALICIOUS_IP_POOL)
    
    ip = user_info["ip"]
    if user_info.get("alt_ips"):
        if (not is_workday or is_off_hours) and random.random() < 0.4:
            ip = random.choice(user_info["alt_ips"])
    return ip

def choose_actor(current_dt):
    """
    Chọn một user (actor) để thực hiện hành động dựa trên thời gian.
    Trả về: (actor_key, actor_info, is_workday, is_ot, is_off, compromised, compromised_mode)
    """
    is_workday = current_dt.weekday() < 5
    hour = current_dt.hour
    minute = current_dt.minute
    hm = dt_time(hour, minute)

    is_overtime_period = is_workday and (WORK_END.hour <= hour < OVERTIME_END.hour)
    is_off_hours = (not is_workday) or (hour < WORK_START.hour) or (hour >= OVERTIME_END.hour)

    candidates = []
    weights = []
    for key, info in EMPLOYEES.items():
        start, end = info["normal_hours"]
        w = 0.0

        if is_time_in_range(hm, start, end):
            w += 1.0
        if is_overtime_period:
            w += info.get("overtime_prob", 0.0)
        if not is_workday and info["dept"] not in ("IT", "Engineering", "Support"): 
             w *= 0.1
        
        if w > 0:
            candidates.append(key)
            weights.append(w)

    today_key = current_dt.date().isoformat()
    attacker_allowed_today = attack_calendar.get(today_key, False)

    compromised = False
    compromised_mode = "none" # 'stealth' hoặc 'loud'

    # Trường hợp 1: Ngoài giờ (Đêm / Cuối tuần)
    if is_off_hours:
        if candidates:
            chosen_key = random.choices(candidates, weights=weights, k=1)[0]
        else:
            chosen_key = random.choice(["it_thanh", "dev_dave"])
        
        chosen_info = EMPLOYEES[chosen_key]

        # Logic kiểm tra "Compromised Account" (Tài khoản bị xâm phạm)
        if (ENABLE_COMPROMISED_ACCOUNT and attacker_allowed_today and (2 <= hour < 5)):
            hour_key = current_dt.strftime("%Y-%m-%d %H")
            count_so_far = compromise_tracker.get(hour_key, 0)
            
            if count_so_far < MAX_COMPROMISE_PER_HOUR:
                if is_workday:
                    prob = BASE_COMPROMISE_PROB
                    mode = "stealth"
                else:
                    prob = WEEKEND_COMPROMISE_PROB
                    mode = "loud"
                
                if random.random() < prob:
                    compromised = True
                    compromised_mode = mode
                    compromise_tracker[hour_key] = count_so_far + 1
        
        return chosen_key, chosen_info, is_workday, is_overtime_period, is_off_hours, compromised, compromised_mode

    # Trường hợp 2: Trong giờ làm việc (hoặc OT < 20h)
    if candidates:
        chosen_key = random.choices(candidates, weights=weights, k=1)[0]
    else:
        chosen_key = "dev_em" # Fallback
    
    chosen_info = EMPLOYEES[chosen_key]

    # Logic "Compromised Account" ban ngày (rất hiếm)
    if (
        ENABLE_COMPROMISED_ACCOUNT
        and ALLOW_DAYTIME_ATTACK
        and is_workday
        and (WORK_START.hour <= hour < WORK_END.hour)
    ):
        if attacker_allowed_today and random.random() < DAYTIME_COMPROMISE_PROB:
            hour_key = current_dt.strftime("%Y-%m-%d %H")
            count_so_far = compromise_tracker.get(hour_key, 0)
            if count_so_far < MAX_COMPROMISE_PER_HOUR:
                compromised = True
                compromised_mode = "stealth"
                compromise_tracker[hour_key] = count_so_far + 1
            
    return chosen_key, chosen_info, is_workday, is_overtime_period, is_off_hours, compromised, compromised_mode

def generate_schedule():
    """Tạo lịch trình (schedule) các phiên (session) mô phỏng."""
    sessions = []
    current_virtual_time = SIMULATION_START_TIME
    end_time = SIMULATION_START_TIME + timedelta(days=SIMULATION_DURATION_DAYS)

    day = SIMULATION_START_TIME.date()
    while day <= end_time.date():
        if str(day) not in attack_calendar:
            attack_calendar[str(day)] = (random.random() < ATTACK_DAY_PROB)
        day = day + timedelta(days=1)

    while current_virtual_time < end_time:
        hour = current_virtual_time.hour
        is_workday_flag = current_virtual_time.weekday() < 5

        if ALWAYS_WORK_HOURS_ONLY:
            if not is_workday_flag or hour < WORK_START.hour or hour >= WORK_END.hour:
                current_virtual_time += timedelta(hours=1)
                continue

        # Tính toán số lượng session trong giờ này
        if (9 <= hour < 12 or 14 <= hour < 16) and is_workday_flag:
            sessions_this_hour = int(SESSIONS_PER_HOUR_BASE * random.uniform(1.0, 1.5))
        elif WORK_START.hour <= hour < WORK_END.hour and is_workday_flag:
            sessions_this_hour = int(SESSIONS_PER_HOUR_BASE * random.uniform(0.8, 1.0))
        else:
            sessions_this_hour = int(SESSIONS_PER_HOUR_BASE * random.uniform(0.1, 0.5))

        for _ in range(sessions_this_hour):
            sess_time = current_virtual_time + timedelta(
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            sessions.append(sess_time)
        
        current_virtual_time += timedelta(hours=1)
    
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
    
    # Mở file label bên ngoài vòng lặp
    with open("simulation_labels.csv", "w", encoding="utf-8") as label_file:
        label_file.write("timestamp,user,label,query\n")

        # Lặp qua lịch trình đã tạo
        for sim_time_dt in schedule:
            
            # 1. Chọn ai sẽ hành động vào thời điểm này
            actor_key, actor_info, is_workday, is_ot, is_off, compromised, compromised_mode = choose_actor(sim_time_dt)
            
            # 2. Lấy kết nối CSDL cho vai trò đó
            # (Logic "Giết và Thay thế" kết nối hỏng)
            conn = active_connections.get(actor_key)
            if conn is None or not conn.is_connected():
                if conn is not None: # Nếu conn bị hỏng (chứ không phải None)
                    log.warning(f"Kết nối cho user {actor_key} bị hỏng. Đang tạo lại...")
                    try: conn.close()
                    except: pass
                
                conn = get_db_connection(actor_key)
                if not conn:
                    continue # Bỏ qua nếu không thể kết nối
                active_connections[actor_key] = conn
            
            # 3. Quyết định hành vi (Bình thường hay Bất thường)
            # (Logic chọn 'persona_key', 'is_anomaly', 'label' giữ nguyên)
            persona_key = actor_info["persona"]
            is_anomaly = False
            label = "Normal"
            if compromised:
                is_anomaly = True
                if compromised_mode == "stealth":
                    persona_key = "Insider_DataLeak" 
                    label = "Compromised_Stealth"
                else:
                    persona_key = random.choice(["Insider_PrivEsc", "Insider_Sabotage", "Insider_DOS"])
                    label = f"Compromised_Loud_{persona_key}"
            elif actor_info["username"] == "dave_dev" and (is_ot or is_off) and ENABLE_INSIDER_BEHAVIOR:
                if ENABLE_SCENARIO_DATA_LEAKAGE and random.random() < 0.2:
                    persona_key = "Insider_DataLeak"; is_anomaly = True; label = "Insider_DataLeak"
                elif ENABLE_SCENARIO_SABOTAGE and random.random() < 0.1:
                    persona_key = "Insider_Sabotage"; is_anomaly = True; label = "Insider_Sabotage"
                elif ENABLE_SCENARIO_PRIV_ESCALATION and random.random() < 0.1:
                    persona_key = "Insider_PrivEsc"; is_anomaly = True; label = "Insider_PrivEsc"
                elif random.random() < 0.05:
                    persona_key = "Insider_DOS"; is_anomaly = True; label = "Insider_DOS"
            elif actor_info["dept"] == "Engineering" and is_workday and ENABLE_SCENARIO_PRIVESC_DAY and random.random() < 0.005:
                    persona_key = "Insider_PrivEsc"; is_anomaly = True; label = "Developer_PrivEsc_Day"

            # 4. Lấy IP Giả lập
            session_ip = choose_session_ip(actor_info, is_workday, is_off, compromised)

            # 5. Lấy một truy vấn ngẫu nhiên từ thư viện
            if persona_key not in query_library or not query_library[persona_key]:
                log.warning(f"Không có truy vấn trong thư viện cho: {persona_key}. Bỏ qua.")
                continue
                
            query_template = random.choice(query_library[persona_key])
            query_template = query_template.replace('?', '%s')

            # 6. Thực thi, Đo lường, và Tạo bản ghi
            log_record = execute_query_and_measure(conn, actor_key, query_template, sim_time_dt, session_ip)
            
            if log_record is None:
                log.warning(f"[{actor_key}] Kết nối bị lỗi (execute trả về None). Đang đóng và loại bỏ kết nối.")
                # Đóng và loại bỏ kết nối hỏng
                conn_h = active_connections.get(actor_key)
                if conn_h:
                    try:
                        conn_h.close()
                    except Exception as e:
                        log.error(f"Đóng kết nối cho user {actor_key} thất bại: {e}")
                if actor_key in active_connections:
                    del active_connections[actor_key]
                continue
                
            # 7. Ghi lại Nhãn (Label) nếu là bất thường
            if is_anomaly:
                label_file.write(f"{log_record['timestamp']},{log_record['user']},{label},\"{log_record['query'].replace('\"', '\"\"')}\"\n")

            # 8. Xuất bản (Publish)
            try:
                redis_client.xadd(STREAM_KEY, {"data": json.dumps(log_record)})
                all_generated_logs.append(log_record)
            
            except RedisConnectionError as e:
                log.error(f"Mất kết nối Redis (Simulator): {e}. Đang kết nối lại..."); redis_client = connect_redis()
            except Exception as e:
                log.error(f"Lỗi khi đẩy log: {e}")

    # --- Kết thúc vòng lặp mô phỏng ---
    
    # 9. Đóng kết nối
    for conn in active_connections.values():
        conn.close()
    
    label_file.close()
        
    # 10. Lưu phần còn lại của Batch Layer
    if all_generated_logs:
        save_logs_to_parquet(all_generated_logs, source_dbms="MySQL_Simulated")
        log.info(f"Đã lưu tổng cộng {len(all_generated_logs)} bản ghi mô phỏng vào Parquet.")

    log.info("✅ Mô phỏng hoàn tất.")
    log.info(f"File nhãn (labels) đã được tạo tại: simulation_labels.csv")

if __name__ == "__main__":
    run_simulation()