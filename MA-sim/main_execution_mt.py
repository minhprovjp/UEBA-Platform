# main_execution_mt.py
import json
import time
import random
import sys
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from agents import EmployeeAgent, MaliciousAgent
from translator import SQLTranslator
from executor import SQLExecutor
from stats_utils import StatisticalGenerator

# --- CẤU HÌNH ---
NUM_THREADS = 5           # Số lượng User chạy song song cùng lúc
SIMULATION_SPEED_UP = 3600 # 1 giây thực tế = 1 giờ (3600s) trong giả lập
START_DATE = datetime(2025, 12, 1, 8, 0, 0) # Bắt đầu từ 8h sáng ngày 1/1
TOTAL_REAL_SECONDS = 30000   # Chạy tool trong 300 giây thực tế (5 phút)

# Biến toàn cục quản lý thời gian
class VirtualClock:
    def __init__(self, start_time, speed_up):
        self.start_real = time.time()
        self.start_sim = start_time
        self.speed_up = speed_up
        self.lock = threading.Lock()

    def get_current_sim_time(self):
        """Tính thời gian ảo dựa trên thời gian trôi qua thực tế"""
        now = time.time()
        elapsed_real = now - self.start_real
        elapsed_sim = elapsed_real * self.speed_up
        
        # Thêm chút jitter (ngẫu nhiên mili-giây) để log không bị trùng khít
        current_sim = self.start_sim + timedelta(seconds=elapsed_sim)
        return current_sim

def load_config():
    try:
        with open("simulation/users_config.json", 'r') as f:
            user_config = json.load(f)
        with open("simulation/db_state.json", 'r') as f:
            db_state = json.load(f)
        return user_config, db_state
    except:
        print("❌ Thiếu config. Chạy setup_full_environment.py trước."); sys.exit(1)

# Hàm chạy của từng Thread (Mỗi thread đóng vai 1 User trong 1 khoảng thời gian)
def user_worker(agent, translator, executor, v_clock, stop_event):
    while not stop_event.is_set():
        # 1. Lấy giờ ảo hiện tại
        sim_time = v_clock.get_current_sim_time()
        hour = sim_time.hour
        
        # 2. Logic nghỉ ngơi (Sleep) theo giờ ảo
        # Nếu là đêm (22h - 6h), giảm tần suất hoạt động cực thấp
        if (hour >= 22 or hour < 6) and not agent.is_malicious:
            time.sleep(0.5) # Ngủ 0.5s thực (tương đương 30p ảo)
            continue

        # 3. Sinh hành động
        intent = agent.step()
        
        # Bỏ qua các bước đệm không sinh query
        if intent['action'] in ["START", "LOGOUT"]:
            time.sleep(0.01)
            continue

        # 4. Dịch & Bắn
        sql = translator.translate(intent)
        
        # Convert Sim Time sang String ISO để gửi kèm
        ts_str = sim_time.isoformat()
        
        success = executor.execute(intent, sql, sim_timestamp=ts_str)
        
        # 5. Log tiến độ (Chỉ in tượng trưng để đỡ lag console)
        if random.random() < 0.05: # In 5% số log thôi
            print(f"[{ts_str}] {intent['user']}: {intent['action']} -> {'OK' if success else 'FAIL'}")

        # 6. [UPDATE] Nghỉ ngơi (Think Time) theo phân phối Pareto
        # Thay vì random.randint(5, 30) (Uniform)
        
        # Logic:
        # - Hành động nhanh (Search/View): nghỉ ngắn, thỉnh thoảng nghỉ dài
        # - Hành động chậm (Update/Create): nghỉ lâu hơn
        
        min_wait = 2  # Giây ảo
        mode_wait = 15 # Giây ảo phổ biến
        
        if "UPDATE" in intent['action'] or "CREATE" in intent['action']:
            mode_wait = 45 # Thao tác ghi thường tốn thời gian suy nghĩ hơn
            
        # Sinh thời gian chờ ảo
        sim_wait_seconds = StatisticalGenerator.generate_pareto_delay(min_wait, mode_wait)
        
        # Chuyển đổi sang thời gian thực (để thread sleep)
        real_sleep_seconds = sim_wait_seconds / v_clock.speed_up
        
        # Giới hạn sleep thực tế tối thiểu để tránh spam quá tải CPU (ví dụ 0.001s)
        time.sleep(max(real_sleep_seconds, 0.001))

def main():
    print(f"🚀 BẮT ĐẦU MÔ PHỎNG ĐA LUỒNG (x{SIMULATION_SPEED_UP} speed)")
    print(f"   - Start Time (Sim): {START_DATE}")
    
    user_config, db_state = load_config()
    users_map = user_config.get("users", {})
    
    # Tạo danh sách tất cả Agent
    all_agents = []
    for username, role in users_map.items():
        if role in ["SALES", "HR", "DEV"]:
            agent = EmployeeAgent(0, username, role, db_state)
            agent.current_state = "LOGIN"
            all_agents.append(agent)

    # Thêm Hacker vào
    hacker = MaliciousAgent(999, db_state)
    all_agents.append(hacker)

    translator = SQLTranslator(db_state)
    executor = SQLExecutor()
    v_clock = VirtualClock(START_DATE, SIMULATION_SPEED_UP)
    
    stop_event = threading.Event()
    
    # ThreadPool
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
        futures = []
        # Phân phối Agent vào các Thread
        # Vì số Agent > số Thread, ta chia sẻ hoặc chọn ngẫu nhiên
        # Ở đây ta chạy vòng lặp, mỗi thread phụ trách liên tục random agent hoặc cố định
        
        # Cách đơn giản: Submit N tác vụ dài hạn, mỗi tác vụ pick random agent để hành động
        for i in range(NUM_THREADS):
            # Chọn random 1 agent cho luồng này (hoặc có thể xoay vòng trong luồng)
            target_agent = random.choice(all_agents)
            futures.append(pool.submit(user_worker, target_agent, translator, executor, v_clock, stop_event))
            
        try:
            start_run = time.time()
            while (time.time() - start_run) < TOTAL_REAL_SECONDS:
                time.sleep(1)
                # In trạng thái thời gian ảo
                curr_sim = v_clock.get_current_sim_time()
                sys.stdout.write(f"\r⏳ Sim Time: {curr_sim.strftime('%Y-%m-%d %H:%M')} (Real elapsed: {int(time.time() - start_run)}s)   ")
                sys.stdout.flush()
        except KeyboardInterrupt:
            print("\n🛑 Force stopping...")
        finally:
            stop_event.set()
            print("\n✅ Simulation finished.")

if __name__ == "__main__":
    main()