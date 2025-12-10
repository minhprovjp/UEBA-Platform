# main_execution_mt.py
import json
import time
import random
import sys
import threading
import uuid
import mysql.connector
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from agents import EmployeeAgent, MaliciousAgent
from translator import SQLTranslator
from executor import SQLExecutor
from stats_utils import StatisticalGenerator

# --- CẤU HÌNH TURBO ---
NUM_THREADS = 10           # 50 luồng spam
SIMULATION_SPEED_UP = 3600 # 1 ngày ảo / 1 giây thực
START_DATE = datetime(2025, 1, 1, 8, 0, 0)
TOTAL_REAL_SECONDS = 600   # Chạy 10 phút
DB_PASSWORD = "password"

# Biến đếm toàn cục
total_queries_sent = 0
lock = threading.Lock()

# --- CLIENT PROFILES ---
CLIENT_PROFILES = {
    "SALES_OFFICE": {
        "os": ["Windows 10", "Windows 11"],
        "prog": ["CRM_App_v2.1", "Tableau Desktop", "Microsoft Excel"],
        "conn": ["libmysql", "odbc-connector", "mysql-connector-net"], # Dân văn phòng dùng App/Excel
        "ip_range": "192.168.10."
    },
    "HR_OFFICE": {
        "os": ["Windows 11"],
        "prog": ["HRM_Portal_Browser", "Chrome"],
        "conn": ["mysql-connector-java", "libmysql"], # Web app thường dùng Java/PHP driver
        "ip_range": "192.168.20."
    },
    "DEV_WORKSTATION": {
        "os": ["Ubuntu 22.04", "MacOS 14.2"],
        "prog": ["MySQL Workbench", "DBeaver", "Python Script", "IntelliJ IDEA"],
        "conn": ["c++-connector", "mysql-connector-python", "jdbc-driver"], # Dev dùng tool
        "ip_range": "192.168.50."
    },
    "HACKER_TOOLKIT": {
        "os": ["Kali Linux", "Unknown", "Windows XP"],
        "prog": ["sqlmap/1.6", "nmap_sE", "python-requests", "curl/7.8", "hydra"],
        "conn": ["None", "python-requests", "libmysql"], # Tool hack thường ẩn hoặc dùng thư viện script
        "ip_range": "10.66.6."
    }
}

def generate_profile(role, is_malicious=False):
    """Sinh ra thông tin thiết bị dựa trên vai trò"""
    if is_malicious:
        base = CLIENT_PROFILES["HACKER_TOOLKIT"]
    elif role == "SALES":
        base = CLIENT_PROFILES["SALES_OFFICE"]
    elif role == "HR":
        base = CLIENT_PROFILES["HR_OFFICE"]
    elif role == "DEV":
        base = CLIENT_PROFILES["DEV_WORKSTATION"]
    else:
        base = CLIENT_PROFILES["SALES_OFFICE"]

    # Source Host: Thường là tên máy tính (VD: DESKTOP-XYZ hoặc macbook-pro)
    rnd_id = random.randint(100, 999)
    src_host = f"{role}-{rnd_id}-{random.choice(['PC', 'LAPTOP'])}"
    if is_malicious: src_host = random.choice(["kalibox", "unknown", f"pwned-{rnd_id}"])

    return {
        "client_os": random.choice(base["os"]),
        "program_name": random.choice(base["prog"]),
        "connector_name": random.choice(base["conn"]), # [NEW]
        "source_host": src_host,                       # [NEW] Tên máy tính
        "source_ip": base["ip_range"] + str(random.randint(2, 250)) # [RENAMED] IP giả lập
    }

# Biến toàn cục quản lý thời gian
class VirtualClock:
    def __init__(self, start_time, speed_up):
        self.start_real = time.time()
        self.start_sim = start_time
        self.speed_up = speed_up

    def get_current_sim_time(self):
        """Tính thời gian ảo dựa trên thời gian trôi qua thực tế"""
        now = time.time()
        elapsed_real = now - self.start_real
        # Ngẫu nhiên mili-giây
        return self.start_sim + timedelta(seconds=elapsed_real * self.speed_up)

def load_config():
    try:
        with open("simulation/users_config.json", 'r') as f:
            user_config = json.load(f)
        with open("simulation/db_state.json", 'r') as f:
            db_state = json.load(f)
        return user_config, db_state
    except:
        print("❌ Thiếu config."); sys.exit(1)

def get_db_connection(username):
    target = "intern_temp" if username in ["script_kiddie", "unknown_hacker"] else username
    if "insider" in username: target = username
    try:
        return mysql.connector.connect(
            host="localhost", user=target, password=DB_PASSWORD,
            autocommit=True, connection_timeout=5
        )
    except: return None

def user_worker_fast(agent_template, translator, v_clock, stop_event):
    """
    Worker thông minh:
    - Persistent Connection để tối ưu tốc độ.
    - Pareto Think Time để tạo Burstiness.
    - Full Client Attributes giả lập.
    """
    global total_queries_sent
    
    # 1. Sinh Profile (Cố định cho thread này)
    my_profile = generate_profile(agent_template.role, agent_template.is_malicious)
    
    # 2. Mở kết nối
    current_db_user = "intern_temp" if agent_template.is_malicious else agent_template.username
    conn = get_db_connection(current_db_user)
    
    if not conn: return # Fail silently
    cursor = conn.cursor()
    
    while not stop_event.is_set():
        try:
            # 3. Lấy giờ ảo
            sim_time = v_clock.get_current_sim_time()
            hour = sim_time.hour

            # 4. Nghỉ đêm (Siêu nhanh)
            if (hour >= 22 or hour < 6) and not agent_template.is_malicious:
                time.sleep(0.001) 
                continue

            # 5. Sinh hành động
            intent = agent_template.step()
            if intent['action'] in ["START", "LOGOUT"]:
                continue

            # 6. Dịch SQL
            sql = translator.translate(intent)
            
            # 7. Tagging
            sim_id = uuid.uuid4().hex[:6]
            ts_str = sim_time.isoformat()
            
            # Lấy thông tin từ profile
            fake_ip = my_profile["source_ip"]
            fake_prog = my_profile["program_name"]
            fake_os = my_profile["client_os"]
            fake_conn = my_profile["connector_name"]
            fake_host = my_profile["source_host"]
            
            tag = f"/* SIM_META:{intent['user']}|{fake_ip}|0|ID:{sim_id}|BEH:{intent['action']}|ANO:{intent['is_anomaly']}|PROG:{fake_prog}|OS:{fake_os}|CONN:{fake_conn}|HOST:{fake_host}|TS:{ts_str} */"
            final_sql = f"{tag} {sql}"

            # 8. Thực thi (Re-use connection)
            try:
                cursor.execute(final_sql)
                if cursor.with_rows: cursor.fetchall()
                
                with lock: total_queries_sent += 1
                
                # Log ngẫu nhiên (30%)
                if random.random() < 0.3:
                    print(f"[{ts_str}] {intent['user']} | {intent['action']}")

            except mysql.connector.Error:
                if not conn.is_connected():
                    conn = get_db_connection(current_db_user)
                    if conn: cursor = conn.cursor()
            
            # 9. Think Time (Pareto Distribution)
            # Tạo hiệu ứng "bùng nổ" (Burstiness)
            min_wait = 2
            mode_wait = 15
            if "UPDATE" in intent['action'] or "CREATE" in intent['action']: mode_wait = 45
            
            sim_wait = StatisticalGenerator.generate_pareto_delay(min_wait, mode_wait)
            real_wait = sim_wait / v_clock.speed_up
            
            # Giới hạn tối thiểu để tránh spam quá mức CPU
            time.sleep(max(real_wait, 0.001))

        except Exception:
            time.sleep(0.1)

    if conn: conn.close()

def main():
    print(f"🚀 BẮT ĐẦU MÔ PHỎNG ĐA LUỒNG (x{SIMULATION_SPEED_UP} speed)")
    print(f"   - Start Time (Sim): {START_DATE}")
    
    user_config, db_state = load_config()
    users_map = user_config.get("users", {})
    
    pool_agents = []
    # 1. Normal Users
    for username, role in users_map.items():
        if role in ["SALES", "HR", "DEV"]:
            agent = EmployeeAgent(0, username, role, db_state)
            agent.current_state = "LOGIN"
            pool_agents.append(agent)
    
    # Hacker
    pool_agents.append(MaliciousAgent(999, db_state))

    translator = SQLTranslator(db_state)
    v_clock = VirtualClock(START_DATE, SIMULATION_SPEED_UP)
    stop_event = threading.Event()
    
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        for _ in range(NUM_THREADS):
            agent = random.choice(pool_agents)
            executor.submit(user_worker_fast, agent, translator, v_clock, stop_event)
            
        try:
            start_run = time.time()
            while (time.time() - start_run) < TOTAL_REAL_SECONDS:
                time.sleep(1)
                # In trạng thái thời gian ảo
                curr_sim = v_clock.get_current_sim_time()
                sys.stdout.write(f"\r⚡ Sent: {total_queries_sent} | Sim Time: {curr_sim.strftime('%Y-%m-%d %H:%M')} ")
                sys.stdout.flush()
        except KeyboardInterrupt:
            print("\n🛑 Stopping...")
        finally:
            stop_event.set()
            print("\n✅ Finished.")

if __name__ == "__main__":
    main()