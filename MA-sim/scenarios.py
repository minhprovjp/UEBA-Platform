# simulation_v2/scenarios.py
import random

class ScenarioManager:
    def __init__(self, db_state):
        self.db_state = db_state

    def get_scenario(self, scenario_name, target_user=None):
        """Trả về một danh sách các Intent (Ý định) để thực thi tuần tự"""
        
        scenarios = []
        
        # --- KỊCH BẢN 1: DAVE ĂN CẮP LƯƠNG (Insider Threat) ---
        # Mô tả: Dave (Dev) tò mò vào xem bảng lương và dump ra file
        if scenario_name == "INSIDER_SALARY_THEFT":
            user = target_user or "dave.insider"
            scenarios = [
                # 1. Login bình thường
                {"user": user, "role": "DEV", "action": "LOGIN", "params": {}, "is_anomaly": 0},
                # 2. Làm việc giả vờ (Check logs)
                {"user": user, "role": "DEV", "action": "CHECK_LOGS", "params": {}, "is_anomaly": 0},
                # 3. Tò mò sang HR (Bất thường: Dev query bảng lương)
                {"user": user, "role": "HR", "action": "VIEW_PAYROLL", "params": {}, "is_anomaly": 1},
                # 4. Dump dữ liệu (Rất bất thường)
                {"user": user, "role": "ATTACKER", "action": "DUMP_DATA", "params": {}, "is_anomaly": 1},
                # 5. Logout
                {"user": user, "role": "DEV", "action": "LOGOUT", "params": {}, "is_anomaly": 0}
            ]

        # --- KỊCH BẢN 2: HACKER TẤN CÔNG BRUTE-FORCE & SQLi (External Attack) ---
        # Mô tả: Hacker thử đăng nhập nhiều lần, sau đó tiêm SQLi
        elif scenario_name == "EXTERNAL_HACK_ATTEMPT":
            user = target_user or "unknown_hacker"
            
            # 1. Brute force (5 lần fail) - Giả lập bằng action LOGIN liên tục
            for _ in range(5):
                scenarios.append({"user": user, "role": "ATTACKER", "action": "LOGIN", "params": {}, "is_anomaly": 1})
            
            # 2. Recon (Dò bảng)
            scenarios.append({"user": user, "role": "ATTACKER", "action": "RECON_SCHEMA", "params": {}, "is_anomaly": 1})
            
            # 3. Tấn công SQLi
            scenarios.append({"user": user, "role": "ATTACKER", "action": "SQLI_CLASSIC", "params": {}, "is_anomaly": 1})
            
            # 4. Drop table (Phá hoại)
            scenarios.append({"user": user, "role": "ATTACKER", "action": "DROP_TABLE", "params": {}, "is_anomaly": 1})

        # --- KỊCH BẢN 3: LATERAL MOVEMENT (Sales tò mò) ---
        elif scenario_name == "SALES_SNOOPING":
            # Lấy random 1 sales user
            user = target_user or "nguyen.van.an" 
            scenarios = [
                {"user": user, "role": "SALES", "action": "LOGIN", "params": {}, "is_anomaly": 0},
                {"user": user, "role": "SALES", "action": "SEARCH_CUSTOMER", "params": {}, "is_anomaly": 0},
                # Đột nhiên query bảng HR
                {"user": user, "role": "HR", "action": "SEARCH_EMPLOYEE", "params": {"dept_id": 1}, "is_anomaly": 1},
                {"user": user, "role": "HR", "action": "VIEW_PROFILE", "params": {"employee_id": 1}, "is_anomaly": 1},
                {"user": user, "role": "SALES", "action": "LOGOUT", "params": {}, "is_anomaly": 0},
            ]

        return scenarios