# simulation/setup_full_environment.py
import mysql.connector
from faker import Faker
import random
import json
import os
import unicodedata

# CẤU HÌNH
DB_CONFIG = {"host": "localhost", "port": 3306, "user": "root", "password": "root"}
USERS_CONFIG_FILE = "simulation/users_config.json"
fake = Faker('vi_VN')

def remove_accents(input_str):
    """Chuyển 'Nguyễn Văn Nam' -> 'nguyen.van.nam'"""
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    s = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    return ".".join(s.lower().split())

def get_conn():
    return mysql.connector.connect(**DB_CONFIG, autocommit=True)

def setup_real_users():
    print("👤 ĐANG TẠO USER TÊN THẬT & PHÂN QUYỀN...")
    conn = get_conn()
    cur = conn.cursor()
    
    # 1. Xóa user cũ (dev_user, sale_user...)
    cur.execute("SELECT User, Host FROM mysql.user WHERE User LIKE '%_user%'")
    for u, h in cur.fetchall(): 
        if u not in ['root', 'mysql.session', 'mysql.sys', 'mysql.infoschema', 'uba_user']:
            try: cur.execute(f"DROP USER '{u}'@'{h}'")
            except: pass

    # 2. Định nghĩa số lượng nhân sự
    # Format: (Role, Count, DB_Access)
    teams = [
        ("SALES", 20, ["sales_db"]),
        ("HR", 5, ["hr_db", "sales_db"]), # HR xem được sales để tính lương
        ("DEV", 10, ["sales_db", "hr_db", "mysql"]),
        ("ADMIN", 2, ["*"])
    ]

    user_map = {} # username -> role
    
    for role, count, dbs in teams:
        for _ in range(count):
            # Tạo tên thật: nguyen.van.a
            full_name = fake.name()
            username = remove_accents(full_name)[:30] # Limit 32 chars
            
            # Tránh trùng
            while username in user_map:
                username += str(random.randint(1,9))
            
            user_map[username] = role
            
            # Tạo MySQL User
            try:
                cur.execute(f"CREATE USER '{username}'@'%' IDENTIFIED BY 'password'")
                
                # Cấp quyền
                if role == "ADMIN":
                    cur.execute(f"GRANT ALL PRIVILEGES ON *.* TO '{username}'@'%'")
                else:
                    for db in dbs:
                        if db == "*": cur.execute(f"GRANT SELECT ON *.* TO '{username}'@'%'")
                        elif db == "mysql": cur.execute(f"GRANT SELECT ON mysql.* TO '{username}'@'%'")
                        else: cur.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {db}.* TO '{username}'@'%'")
            except Exception as e:
                print(f"⚠️ Lỗi tạo {username}: {e}")

    # Tạo User Hacker/Insider cụ thể để kịch bản dùng
    bad_actors = {
        "dave.insider": "BAD_ACTOR",     # Insider Threat
        "guest.temp": "VULNERABLE",      # User yếu để hacker chiếm
        "script.kiddie": "EXTERNAL"      # Hacker bên ngoài
    }
    
    for u, role in bad_actors.items():
        try:
            cur.execute(f"CREATE USER '{u}'@'%' IDENTIFIED BY 'password'")
            cur.execute(f"GRANT SELECT ON sales_db.* TO '{u}'@'%'")
            user_map[u] = role
        except: pass

    cur.execute("FLUSH PRIVILEGES")
    conn.close()
    
    # Lưu config
    # ROLE_PERMISSIONS mapping giữ nguyên hoặc cập nhật tùy ý
    config_data = {"users": user_map}
    with open(USERS_CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config_data, f, indent=2)
    print(f"✅ Đã tạo {len(user_map)} users tên thật. Config saved.")

if __name__ == "__main__":
    setup_real_users()