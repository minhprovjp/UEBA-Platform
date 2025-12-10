# simulation_v2/executor.py
import mysql.connector
import logging
import uuid
import random
import time # [NEW]

# Cấu hình kết nối
DB_CONFIG = {
    "port": 3306,
    "password": "password",
    "connection_timeout": 5
}

class SQLExecutor:
    def __init__(self):
        pass

    def get_connection(self, username, client_profile, database=None):
        """
        Tạo connection.
        [FIX] Đã loại bỏ connection_attributes để tránh lỗi thư viện.
        [FIX] Dùng host='localhost' để kết nối ổn định hơn.
        [FIX] Added optional database parameter to set default database
        """
        # Hackers map về intern_temp hoặc user quyền thấp để kết nối được
        target_user = "intern_temp" if username in ["script_kiddie", "unknown_hacker"] else username
        
        # Nếu user là insider (ví dụ dave.insider) thì dùng chính user đó
        if "insider" in username:
            target_user = username

        connection_params = {
            "host": "127.0.0.1", # [FIX] Dùng localhost thay vì 127.0.0.1
            "user": target_user, 
            "password": "password",
            "autocommit": True, 
            "connection_timeout": 3
        }
        
        # [FIX] Set default database if provided
        if database:
            connection_params["database"] = database

        try:
            return mysql.connector.connect(**connection_params)
        except Exception as e:
            # logging.error(f"Connection failed for {target_user}: {e}")
            return None

    def execute(self, intent, sql, sim_timestamp=None, client_profile=None):
        user = intent['user']
        is_anomaly = intent['is_anomaly']
        behavior = intent['action']
        db_target = intent.get('database', '') 
        # [NEW] Lấy Port từ Agent
        agent_port = intent.get('client_port', 0)
        
        if not client_profile:
            client_profile = {}

        # Fallback DB target
        if not db_target:
            if "sales_db" in sql: db_target = "sales_db"
            elif "hr_db" in sql: db_target = "hr_db"
            
        # [NEW] Latency Injection
        # Hacker/Remote: 50ms - 200ms
        # Local: 1ms - 5ms
        if is_anomaly:
            latency = random.uniform(0.05, 0.2)
        else:
            latency = random.uniform(0.001, 0.005)
        
        # Sleep để giả lập độ trễ mạng TRƯỚC KHI gửi query
        time.sleep(latency)
        
        # --- TAGGING (Quan trọng nhất) ---
        sim_id = uuid.uuid4().hex[:6]
        
        # Lấy thông tin giả lập từ Profile
        fake_ip = client_profile.get("source_host", "192.168.1.100")
        fake_prog = client_profile.get("program_name", "Unknown")
        fake_os = client_profile.get("client_os", "Windows")
        fake_conn = client_profile.get("connector_name", "mysql-connector")
        fake_host = client_profile.get("source_host", "pc")
        
        ts_tag = f"|TS:{sim_timestamp}" if sim_timestamp else ""
        
        # [UPDATE] Thêm Port vào Tag
        tag = f"/* SIM_META:{user}|{fake_ip}|{agent_port}|ID:{sim_id}|BEH:{behavior}|ANO:{is_anomaly}|PROG:{fake_prog}|OS:{fake_os}|CONN:{fake_conn}|HOST:{fake_host}{ts_tag} */"
        
        tagged_sql = f"{tag} {sql}"

        # [FIX] Determine database from SQL content
        target_database = None
        databases_to_check = ["sales_db", "hr_db", "information_schema", "mysql"]
        for db_name in databases_to_check:
            if f"{db_name}." in sql:
                target_database = db_name
                break  # Use the first database found
        
        # Thực thi
        conn = self.get_connection(user, client_profile, target_database)
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute(tagged_sql)
                
                # Đọc hết kết quả để giải phóng cursor
                if cursor.with_rows: 
                    cursor.fetchall()
                
                cursor.close()
                conn.close()
                return True
            except Exception as e:
                # Lỗi SQL (ví dụ hacker chạy lệnh sai) vẫn là thành công về mặt mô phỏng
                if conn: conn.close()
                return False
        return False