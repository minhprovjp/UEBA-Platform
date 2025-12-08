# simulation_v2/agents.py
import random
from config_markov import MARKOV_TRANSITIONS, ACTION_REQUIREMENTS
from stats_utils import StatisticalGenerator 

class EmployeeAgent:
    def __init__(self, agent_id, username, role, db_data):
        self.agent_id = agent_id
        self.username = username
        self.role = role
        self.db_data = db_data  # Dữ liệu hợp lệ (customer_ids, product_ids...)
        
        # --- TRẠNG THÁI & BỘ NHỚ ---
        self.current_state = "START"
        self.session_context = {} # Trí nhớ ngắn hạn của phiên làm việc
        
        self.is_malicious = False # Cờ đánh dấu Insider Threat
    
    def step(self):
        """
        Thực hiện 1 bước hành động dựa trên trạng thái hiện tại.
        """
        # 1. Xác định hành động tiếp theo dựa trên Markov
        next_state = self._get_next_state()
        
        # 2. Chuẩn bị dữ liệu cho hành động đó (Contextual Data)
        query_params = self._prepare_data_for_action(next_state)
        
        # 3. Cập nhật trạng thái
        self.current_state = next_state
        
        # 4. Trả về "Ý định" (Intent) để lớp khác biến thành SQL
        return {
            "user": self.username,
            "role": self.role,
            "action": next_state,
            "params": query_params,
            "session_id": self.session_context.get("session_id"),
            "is_anomaly": 1 if self.is_malicious else 0  # Gắn nhãn Anomaly
        }

    def _get_next_state(self):
        """Chọn trạng thái tiếp theo dựa trên xác suất"""
        role_transitions = MARKOV_TRANSITIONS.get(self.role, {})
        current_transitions = role_transitions.get(self.current_state)
        
        if not current_transitions:
            return "START" # Reset nếu tắc đường
            
        states = list(current_transitions.keys())
        probabilities = list(current_transitions.values())
        
        # Chọn ngẫu nhiên có trọng số
        next_state = random.choices(states, weights=probabilities, k=1)[0]
        
        # Logic Reset phiên làm việc
        if next_state == "LOGIN":
            self.session_context = {"session_id": random.randint(1000, 9999)} # Tạo phiên mới
        if next_state == "LOGOUT":
            self.session_context = {} # Xóa bộ nhớ phiên
            
        return next_state

    def _prepare_data_for_action(self, action):
        """
        Điền tham số vào hành động.
        QUAN TRỌNG: Dùng lại ID đã nhớ nếu có, thay vì random.
        """
        params = {}
        
        # Nếu hành động cần ID (ví dụ VIEW_ORDER cần order_id)
        if action in ACTION_REQUIREMENTS:
            required_keys = ACTION_REQUIREMENTS[action]
            
            for key in required_keys:
                # 1. Kiểm tra xem trong Context đã có ID này chưa (Tính liên kết)
                if key in self.session_context:
                    params[key] = self.session_context[key]
                else:
                    # 2. Nếu chưa có (vd: bước SEARCH), lấy random từ DB và NHỚ LẠI
                    val = self._pick_random_entity(key)
                    params[key] = val
                    self.session_context[key] = val # <--- LƯU VÀO BỘ NHỚ
                    
        # Xử lý logic Reset Context (Ví dụ: Search mới thì quên ID cũ đi)
        if action.startswith("SEARCH_"):
            # Clear context liên quan để bắt đầu tìm cái mới
            if "CUSTOMER" in action: self.session_context.pop("customer_id", None)
            if "ORDER" in action: self.session_context.pop("order_id", None)
            if "EMPLOYEE" in action: self.session_context.pop("employee_id", None)
            
        return params

    def _pick_random_entity(self, key):
        """Lấy dữ liệu hợp lệ từ file db_state.json theo luật Zipf"""
        
        mapping = {
            "customer_id": "customer_ids",
            "order_id": "order_ids",
            "product_id": "product_ids",
            "employee_id": "employee_ids",
            "campaign_id": "campaign_ids"
        }
        
        if key in mapping and mapping[key] in self.db_data:
            data_list = self.db_data[mapping[key]]
                
            # [UPDATE] Thay random.choice bằng Zipfian selection
            # Điều này mô phỏng việc 80% truy vấn dồn vào 20% khách hàng/sản phẩm "hot"
            return StatisticalGenerator.pick_zipfian_item(data_list)
            
        return 1 # Fallback
    
    
# --- [NEW] CLASS KẺ TẤN CÔNG BÊN NGOÀI ---
class MaliciousAgent(EmployeeAgent):
    def __init__(self, agent_id, db_data):
        # Hacker dùng user lạ hoặc user bị lộ
        super().__init__(agent_id, "unknown_hacker", "ATTACKER", db_data)
        self.attack_chain = ["LOGIN", "RECON_SCHEMA", "SQLI_CLASSIC", "DUMP_DATA", "LOGOUT"]
        self.step_index = 0

    def step(self):
        """Hacker hành động theo kịch bản cứng (Kill Chain) hoặc Random"""
        if self.step_index >= len(self.attack_chain):
            self.step_index = 0 # Reset chain
            
        action = self.attack_chain[self.step_index]
        self.step_index += 1
        
        # Params giả cho hacker
        params = {"random_id": random.randint(1000,9999)}
        
        return {
            "user": "script_kiddie", # User DB giả định
            "role": "ATTACKER",
            "action": action,
            "params": params,
            "is_anomaly": 1 # Luôn là bất thường
        }