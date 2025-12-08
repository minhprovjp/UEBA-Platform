# simulation_v2/executor.py
import mysql.connector
import logging
import uuid
import random

# Cấu hình kết nối
DB_CONFIG = {
    "port": 3306,
    "password": "password",
    "connection_timeout": 5
}

class SQLExecutor:
    def __init__(self):
        pass

    def get_connection(self, username, client_profile):
        """
        Tạo connection.
        [FIX] Đã loại bỏ connection_attributes để tránh lỗi thư viện.
        [FIX] Dùng host='localhost' để kết nối ổn định hơn.
        """
        # Hackers map về intern_temp hoặc user quyền thấp để kết nối được
        target_user = "intern_temp" if username in ["script_kiddie", "unknown_hacker"] else username
        
        # Nếu user là insider (ví dụ dave.insider) thì dùng chính user đó
        if "insider" in username:
            target_user = username

        try:
            return mysql.connector.connect(
                host="127.0.0.1", # [FIX] Dùng localhost thay vì 127.0.0.1
                user=target_user, 
                password="password",
                autocommit=True, 
                connection_timeout=3
                # [FIX] Đã xóa connection_attributes
            )
        except Exception as e:
            # logging.error(f"Connection failed for {target_user}: {e}")
            return None

    def execute(self, intent, sql, sim_timestamp=None, client_profile=None):
        user = intent['user']
        is_anomaly = intent['is_anomaly']
        behavior = intent['action']
        db_target = intent.get('database', '') 
        
        if not client_profile:
            client_profile = {}

        # Fallback DB target
        if not db_target:
            if "sales_db" in sql: db_target = "sales_db"
            elif "hr_db" in sql: db_target = "hr_db"
        
        # --- TAGGING (Quan trọng nhất) ---
        sim_id = uuid.uuid4().hex[:6]
        
        # Lấy thông tin giả lập từ Profile
        fake_ip = client_profile.get("source_host", "192.168.1.100")
        fake_prog = client_profile.get("program_name", "Unknown")
        fake_os = client_profile.get("client_os", "Windows")
        
        fake_conn = client_profile.get("connector_name", "mysql-connector-python")
        fake_host = client_profile.get("source_host", "unknown-pc")
        
        ts_tag = f"|TS:{sim_timestamp}" if sim_timestamp else ""
        
        # Tag format mở rộng: thêm CONN và HOST
        # /* SIM_META:User|IP|0|ID:..|BEH:..|ANO:..|PROG:..|OS:..|CONN:..|HOST:..|TS:.. */
        tag = f"/* SIM_META:{user}|{fake_ip}|0|ID:{sim_id}|BEH:{behavior}|ANO:{is_anomaly}|PROG:{fake_prog}|OS:{fake_os}|CONN:{fake_conn}|HOST:{fake_host}{ts_tag} */"
        
        tagged_sql = f"{tag} {sql}"

        # Thực thi
        conn = self.get_connection(user, client_profile)
        if conn:
            try:
                cursor = conn.cursor()
                
                # Chọn DB nếu cần
                if db_target:
                    try: cursor.execute(f"USE {db_target}")
                    except: pass 
                
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