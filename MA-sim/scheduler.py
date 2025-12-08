# simulation_v2/scheduler.py
import random
from datetime import datetime, timedelta

class SimulationScheduler:
    def __init__(self, start_time, agents, translator):
        self.current_time = start_time
        self.agents = agents
        self.translator = translator
        
        # Trạng thái thời gian chờ của từng agent (Agent ID -> Thời điểm được phép hành động tiếp)
        # Ví dụ: { "agent_1": datetime(2025,1,1, 9, 5) }
        self.agent_cooldowns = {} 

    def tick(self, minutes=1):
        """
        Tiến thời gian thêm X phút và xử lý hành động của các Agent
        """
        self.current_time += timedelta(minutes=minutes)
        logs = []
        
        # Giờ hiện tại trong ngày (0-23)
        current_hour = self.current_time.hour
        is_weekend = self.current_time.weekday() >= 5
        
        for agent in self.agents:
            # 1. Kiểm tra Lịch làm việc (Work Schedule Logic)
            # Đây là logic VĨ MÔ (Macro Layer)
            is_working_hours = False
            
            if agent.role == "SALES":
                # Sales làm việc 8h-18h, thỉnh thoảng OT đến 20h
                if 8 <= current_hour < 18: is_working_hours = True
                elif 18 <= current_hour < 20 and random.random() < 0.1: is_working_hours = True # 10% OT
                
            elif agent.role == "DEV":
                # Dev làm muộn hơn 9h-19h, hay OT đêm
                if 9 <= current_hour < 19: is_working_hours = True
                elif (19 <= current_hour < 23) and random.random() < 0.2: is_working_hours = True # 20% OT
                
            elif agent.role == "HR":
                # HR làm hành chính chuẩn chỉ
                if 8 <= current_hour < 17: is_working_hours = True

            # Cuối tuần nghỉ (trừ khi có kịch bản đặc biệt - sẽ thêm sau)
            if is_weekend: is_working_hours = False

            # Nếu không phải giờ làm việc -> Bỏ qua
            if not is_working_hours:
                # Nếu đang có session dở, force logout (để hôm sau login lại)
                if agent.current_state != "START":
                    agent.current_state = "START"
                    agent.session_context = {}
                continue

            # 2. Kiểm tra Cooldown (Think Time)
            # Đây là logic VI MÔ (Micro Layer) - Mô phỏng tốc độ con người
            if agent.agent_id in self.agent_cooldowns:
                if self.current_time < self.agent_cooldowns[agent.agent_id]:
                    continue # Vẫn đang "suy nghĩ", chưa làm gì

            # 3. Thực hiện hành động
            intent = agent.step()
            
            # Nếu hành động là START (đang chờ Login), bỏ qua không log
            if intent['action'] == "START":
                continue

            # 4. Dịch sang SQL
            sql = self.translator.translate(intent)
            
            # 5. Ghi log kết quả
            log_entry = {
                "timestamp": self.current_time.isoformat(),
                "user": agent.username,
                "role": agent.role,
                "action": intent['action'],
                "database": "sales_db", # Tạm thời default, Translator nên trả về DB đích
                "query": sql
            }
            
            # Fix database name based on query content (Logic đơn giản)
            if "hr_db" in sql: log_entry["database"] = "hr_db"
            elif "sales_db" in sql: log_entry["database"] = "sales_db"
            
            logs.append(log_entry)
            
            # 6. Thiết lập thời gian chờ cho hành động tiếp theo (Poisson-like)
            # Hành động phức tạp nghỉ lâu hơn
            wait_minutes = random.randint(1, 5) 
            if intent['action'] in ["LOGIN", "SEARCH_ORDER"]:
                wait_minutes = random.randint(1, 3) # Nhanh
            elif intent['action'] in ["EXPORT_REPORT", "DEBUG_QUERY"]:
                wait_minutes = random.randint(5, 15) # Lâu
                
            self.agent_cooldowns[agent.agent_id] = self.current_time + timedelta(minutes=wait_minutes)

        return logs