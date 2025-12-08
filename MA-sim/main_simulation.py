# main_execution.py
import json
import time
import random
import sys
from datetime import datetime
from agents import EmployeeAgent, MaliciousAgent
from translator import SQLTranslator
from executor import SQLExecutor

# --- CẤU HÌNH ---
ANOMALY_LEVEL = 0.05  # 5% tổng số hành động là bất thường (Insider hoặc Hacker)
EXECUTION_SPEED = 0.1 # Thời gian nghỉ giữa các query (giây). 0.1 = rất nhanh.

def load_config():
    try:
        with open("simulation/users_config.json", 'r') as f:
            user_config = json.load(f)
        with open("simulation/db_state.json", 'r') as f:
            db_state = json.load(f)
        return user_config, db_state
    except FileNotFoundError:
        print("❌ Thiếu file config. Chạy setup_full_environment.py trước.")
        sys.exit(1)

def main():
    print(f"🚀 BẮT ĐẦU THỰC THI REALTIME (Anomaly Rate: {ANOMALY_LEVEL*100}%)")
    print("   -> Press Ctrl+C to stop.")
    
    user_config, db_state = load_config()
    users_map = user_config.get("users", {})
    
    # 1. Khởi tạo đội ngũ nhân viên (Normal Agents)
    employees = []
    for username, role in users_map.items():
        if role in ["SALES", "HR", "DEV"]:
            agent = EmployeeAgent(0, username, role, db_state)
            agent.current_state = "LOGIN" # Ép vào trạng thái sẵn sàng
            employees.append(agent)
            
    # 2. Khởi tạo Hacker (Bad Agent)
    hacker = MaliciousAgent(999, db_state)
    
    translator = SQLTranslator(db_state)
    executor = SQLExecutor()
    
    counter = 0
    try:
        while True:
            # --- LOGIC ĐIỀU PHỐI ---
            
            # Quyết định xem lượt này là Người tốt hay Kẻ xấu
            if random.random() < ANOMALY_LEVEL:
                # == KỊCH BẢN XẤU ==
                if random.random() < 0.5:
                    # A. Hacker tấn công từ ngoài
                    agent = hacker
                else:
                    # B. Insider Threat (Nhân viên làm bậy)
                    agent = random.choice(employees)
                    agent.is_malicious = True # Bật chế độ xấu xa (tạm thời)
                    # Insider thường làm gì? Dump data hoặc xem lương sếp
                    # Ở đây ta hack nhẹ: ép intent
                    override_intent = {
                        "user": agent.username, "role": agent.role,
                        "action": "DUMP_DATA", "params": {},
                        "is_anomaly": 1
                    }
            else:
                # == KỊCH BẢN BÌNH THƯỜNG ==
                agent = random.choice(employees)
                agent.is_malicious = False

            # --- SINH & THỰC THI ---
            
            # 1. Lấy ý định (Nếu chưa bị override ở trên)
            if 'override_intent' in locals() and agent.is_malicious and override_intent:
                intent = override_intent
                override_intent = None # Reset
            else:
                intent = agent.step()
                
            # Bỏ qua trạng thái chờ
            if intent['action'] in ["START", "LOGOUT"]: continue

            # 2. Dịch sang SQL
            sql = translator.translate(intent)
            
            # 3. Bắn vào Database
            success = executor.execute(intent, sql)
            
            # Log ra màn hình console cho đẹp
            status_icon = "🔴" if intent['is_anomaly'] else "🟢"
            print(f"{status_icon} [{intent['user']}] {intent['action']} -> {sql[:60]}...")
            
            counter += 1
            time.sleep(EXECUTION_SPEED) # Điều chỉnh tốc độ spam

    except KeyboardInterrupt:
        print(f"\n🛑 Đã dừng. Tổng số query đã bắn: {counter}")

if __name__ == "__main__":
    main()