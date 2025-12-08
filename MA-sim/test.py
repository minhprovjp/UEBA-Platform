# step3_fast_multithread.py
import json
import time
import random
import sys
import threading
import uuid
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from agents import EmployeeAgent, MaliciousAgent
from translator import SQLTranslator
from executor import SQLExecutor

# --- CẤU HÌNH ---
NUM_THREADS = 10
SIMULATION_SPEED_UP = 86400 # 1s = 1 ngày
START_DATE = datetime(2025, 1, 1, 8, 0, 0)
TOTAL_REAL_SECONDS = 600

# --- ĐỊNH NGHĨA CLIENT PROFILES (SỰ ĐA DẠNG) ---
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
        "conn": ["c++-connector", "mysql-connector-python", "jdbc-driver"], # Dev dùng tool xịn
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
    if base == CLIENT_PROFILES["HACKER_TOOLKIT"]:
        src_host = random.choice(["kalibox", "unknown", "localhost", f"owned-pc-{rnd_id}"])
    else:
        src_host = f"{role}-{rnd_id}-{random.choice(['PC', 'LAPTOP'])}"

    return {
        "client_os": random.choice(base["os"]),
        "program_name": random.choice(base["prog"]),
        "connector_name": random.choice(base["conn"]), # [NEW]
        "source_host": src_host,                       # [NEW] Tên máy tính
        "source_ip": base["ip_range"] + str(random.randint(2, 250)) # [RENAMED] IP giả lập
    }

# ... (Class VirtualClock giữ nguyên) ...
class VirtualClock:
    def __init__(self, start_time, speed_up):
        self.start_real = time.time()
        self.start_sim = start_time
        self.speed_up = speed_up
    def get_current_sim_time(self):
        now = time.time()
        elapsed_real = now - self.start_real
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

def user_worker_fast(agent, translator, executor, v_clock, stop_event):
    # Mỗi user có 1 profile thiết bị cố định trong phiên này
    my_profile = generate_profile(agent.role, agent.is_malicious)
    
    # Biến đổi profile nếu là Insider Threat (Dev dùng tool lạ)
    if agent.is_malicious and agent.role != "ATTACKER":
         if random.random() < 0.5: # 50% lộ tool
             my_profile["program_name"] = "python-requests" 

    while not stop_event.is_set():
        sim_time = v_clock.get_current_sim_time()
        hour = sim_time.hour
        
        # Logic nghỉ đêm
        if (hour >= 22 or hour < 6) and not agent.is_malicious:
            time.sleep(0.005)
            continue
            
        intent = agent.step()
        if intent['action'] in ["START", "LOGOUT"]:
            time.sleep(0.001); continue

        sql = translator.translate(intent)
        ts_str = sim_time.isoformat()
        
        # [QUAN TRỌNG] Truyền Profile vào Executor
        success = executor.execute(intent, sql, sim_timestamp=ts_str, client_profile=my_profile)
        
        # In log tượng trưng
        if random.random() < 0.05:
            print(f"[{ts_str}] {agent.username} ({my_profile['program_name']}) | {intent['action']} -> {'OK' if success else 'FAIL'}")

        # Think time
        time.sleep(random.randint(1, 3) / v_clock.speed_up)

def main():
    print(f"🚀 BẮT ĐẦU MÔ PHỎNG (Tên thật + Thiết bị thật)...")
    user_config, db_state = load_config()
    users_map = user_config.get("users", {})
    
    pool_agents = []
    # 1. Normal Users
    for username, role in users_map.items():
        if role in ["SALES", "HR", "DEV"]:
            agent = EmployeeAgent(0, username, role, db_state)
            agent.current_state = "LOGIN"
            pool_agents.append(agent)

    # 2. Hackers (Ngoài)
    for _ in range(3):
        hacker = MaliciousAgent(999, db_state)
        pool_agents.append(hacker)

    translator = SQLTranslator(db_state)
    executor = SQLExecutor()
    v_clock = VirtualClock(START_DATE, SIMULATION_SPEED_UP)
    stop_event = threading.Event()
    
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor_pool:
        # Mỗi thread đảm nhận 1 user ngẫu nhiên từ pool
        for _ in range(NUM_THREADS):
            agent = random.choice(pool_agents)
            executor_pool.submit(user_worker_fast, agent, translator, executor, v_clock, stop_event)
            
        try:
            start_run = time.time()
            while (time.time() - start_run) < TOTAL_REAL_SECONDS:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            stop_event.set()

if __name__ == "__main__":
    main()