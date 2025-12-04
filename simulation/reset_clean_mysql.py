# simulation/reset_clean_mysql.py
import mysql.connector
import os
from redis import Redis

# --- CẤU HÌNH ROOT ---
# Phải dùng quyền ROOT để xóa sạch mọi thứ
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root" # <-- Thay password root của bạn vào đây
}
REDIS_URL = "redis://localhost:6379/0"
FILES_TO_DELETE = [
    "logs/.mysql_perf_creator.state",
]

def get_conn():
    try:
        return mysql.connector.connect(**DB_CONFIG, autocommit=True)
    except Exception as e:
        print(f"❌ Không kết nối được MySQL: {e}")
        return None

def clean_databases(cursor):
    print("🗑️  1. Đang xóa các Database cũ (sales_db, hr_db)...")
    dbs = ['sales_db', 'hr_db', 'admin_db', 'hack_db'] # Thêm hack_db phòng hờ
    for db in dbs:
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {db}")
            print(f"   - Đã xóa: {db}")
        except Exception as e:
            print(f"   - Lỗi xóa {db}: {e}")

def clean_users(cursor):
    print("👤 2. Đang xóa sạch các User (Sale, HR, Dave, Hacker)...")
    
    # Lấy danh sách user hiện tại để lọc
    cursor.execute("SELECT user, host FROM mysql.user")
    users = cursor.fetchall()
    
    users_to_drop = []
    for u, h in users:
        # Chỉ xóa các user liên quan đến mô phỏng, KHÔNG xóa root hay mysql.sys
        if any(x in u for x in ['sale_user', 'hr_user', 'dev_user', 'dave', 'hacker', 'backdoor', 'intern_temp', 'script_kiddie', 'unknown']):
            users_to_drop.append((u, h))
    
    for u, h in users_to_drop:
        try:
            cursor.execute(f"DROP USER '{u}'@'{h}'")
            print(f"   - Đã xóa user: '{u}'@'{h}'")
        except Exception as e:
            print(f"   ⚠️ Không xóa được {u}: {e}")
            
    cursor.execute("FLUSH PRIVILEGES")

def reset_global_settings(cursor):
    print("⚙️  3. Reset cấu hình MySQL (Khôi phục hậu quả tấn công)...")
    try:
        # Nếu hacker đã bật set global read_only, tắt nó đi
        cursor.execute("SET GLOBAL read_only = 0")
        cursor.execute("SET GLOBAL offline_mode = 0")
        # Xóa các Event/Process lạ nếu có (nhưng Drop DB ở bước 1 đã lo việc này rồi)
        print("   - Global variables reset: OK")
    except Exception as e:
        print(f"   - Lỗi reset settings: {e}")

def clean_performance_schema(cursor):
    print("🧹 4. Dọn dẹp lịch sử Performance Schema (Forensics data)...")
    tables = [
        "events_statements_history_long",
        "events_statements_history",
        "events_stages_history_long"
    ]
    for t in tables:
        try:
            cursor.execute(f"TRUNCATE TABLE performance_schema.{t}")
            print(f"   - Đã truncate: {t}")
        except Exception as e:
            print(f"   ⚠️ Không truncate được {t} (Có thể do chưa bật): {e}")

def clean_files():
    print("📂 5. Xóa các file 'rác' do tấn công Data Exfiltration tạo ra...")
    # Hacker thường xuất file ra /tmp/ hoặc thư mục upload
    # Lưu ý: Code này chạy trên máy đang chạy Python. 
    # Nếu MySQL nằm trên server khác, bạn phải vào server đó xóa tay.
    
    temp_dir = "C:/Windows/Temp" if os.name == 'nt' else "/tmp"
    
    try:
        count = 0
        for filename in os.listdir(temp_dir):
            if filename.startswith("leak_") and filename.endswith(".csv"):
                file_path = os.path.join(temp_dir, filename)
                try:
                    os.remove(file_path)
                    count += 1
                except: pass
        print(f"   - Đã xóa {count} file rác trong {temp_dir}")
    except:
        print("   - Không truy cập được thư mục Temp (Bỏ qua)")

def reset_system():
    print("🚀 BẮT ĐẦU RESET TOÀN BỘ HỆ THỐNG...")
    
    # 1. Xóa file tạm và dataset cũ
    for f_path in FILES_TO_DELETE:
        if os.path.exists(f_path):
            try:
                os.remove(f_path)
                print(f"✅ Đã xóa file: {f_path}")
            except Exception as e:
                print(f"❌ Lỗi xóa {f_path}: {e}")
        else:
            print(f"ℹ️  File {f_path} không tồn tại (Sạch).")

    # 2. Xóa dữ liệu Redis
    try:
        r = Redis.from_url(REDIS_URL)
        r.flushall()
        print("✅ Đã xóa sạch Redis (FLUSHALL).")
    except Exception as e:
        print(f"❌ Lỗi kết nối Redis: {e}")

    # 3. Xóa sạch bộ nhớ đệm MySQL (Quan trọng nhất)
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Tắt consumer để truncate an toàn
        cur.execute("UPDATE performance_schema.setup_consumers SET ENABLED = 'NO' WHERE NAME = 'events_statements_history_long'")
        
        # Xóa sạch bảng
        cur.execute("TRUNCATE TABLE performance_schema.events_statements_history_long")
        
        # Bật lại consumer ngay lập tức
        cur.execute("UPDATE performance_schema.setup_consumers SET ENABLED = 'YES' WHERE NAME = 'events_statements_history_long'")
        
        print("✅ Đã TRUNCATE bảng log MySQL (Bộ nhớ về 0 dòng).")
        conn.close()
    except Exception as e:
        print(f"❌ Lỗi MySQL: {e}")


def main():
    conn = get_conn()
    if not conn: return
    
    cursor = conn.cursor()
    
    clean_databases(cursor)
    clean_users(cursor)
    reset_global_settings(cursor)
    clean_performance_schema(cursor)
    
    conn.close()
    clean_files()
    reset_system()
    
    print("\n✨ MYSQL ĐÃ SẠCH BÓNG! BẠN CÓ THỂ CHẠY LẠI SETUP.")

if __name__ == "__main__":
    main()