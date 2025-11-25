import mysql.connector
import sys

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root", 
    "password": "root" # <--- Sửa password
}

def clean_slate():
    print("🧹 ĐANG DỌN DẸP HỆ THỐNG (CLEAN SLATE)...")
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Tắt kiểm tra khóa ngoại để drop thoải mái
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        databases = ['sales_db', 'hr_db', 'admin_db']
        for db in databases:
            cursor.execute(f"DROP DATABASE IF EXISTS {db}")
            print(f"   - Đã xóa Database: {db}")
            
        # Xóa user rác nếu cần (Optional - để tránh lỗi Duplicate User khi tạo lại)
        # cursor.execute("DROP USER IF EXISTS 'sale_user_0'@'%'") ... (Có thể bỏ qua nếu setup có IF NOT EXISTS)
            
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        print("✅ Đã dọn dẹp sạch sẽ. Hệ thống sẵn sàng để Setup lại.")
        conn.close()
    except Exception as e:
        print(f"❌ Lỗi khi dọn dẹp: {e}")

if __name__ == "__main__":
    clean_slate()