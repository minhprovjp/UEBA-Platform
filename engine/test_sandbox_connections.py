import mysql.connector
import sys

# ==============================================================================
# SCRIPT KIỂM TRA KẾT NỐI SANDBOX
# - Không cần file .env, tất cả thông tin được ghi trực tiếp tại đây.
# - Mật khẩu mặc định cho tất cả user là "password".
# ==============================================================================

# Cấu hình Host và Port của Sandbox
SANDBOX_HOST = "localhost"
SANDBOX_PORT = 3306

# Danh sách tất cả các user cần kiểm tra
USER_CONFIGS = {
    # --- Sales Team ---
    "Anh (Sales)":      {"user": "anh_sales",   "database": "sales_db"},
    "Linh (Sales)":     {"user": "linh_sales",  "database": "sales_db"},
    "Quang (Sales)":    {"user": "quang_sales", "database": "sales_db"},
    "Trang (Sales)":    {"user": "trang_sales", "database": "sales_db"},
    # --- Marketing Team ---
    "Binh (Marketing)": {"user": "binh_mkt",    "database": "sales_db"},
    "Mai (Marketing)":  {"user": "mai_mkt",     "database": "sales_db"},
    "Vy (Marketing)":   {"user": "vy_mkt",      "database": "sales_db"},
    # --- HR Team ---
    "Chi (HR)":         {"user": "chi_hr",      "database": "hr_db"},
    "Hoa (HR)":         {"user": "hoa_hr",      "database": "hr_db"},
    # --- Support Team ---
    "Dung (Support)":   {"user": "dung_support","database": "sales_db"},
    "Loan (Support)":   {"user": "loan_support","database": "sales_db"},
    "Khang (Support)":  {"user": "khang_support","database": "sales_db"},
    # --- Engineering/Dev Team ---
    "Em (Dev)":         {"user": "em_dev",      "database": "sales_db"},
    "Tam (Dev)":        {"user": "tam_dev",     "database": "sales_db"},
    "Ly (Data)":        {"user": "ly_data",     "database": "sales_db"},
    "Quoc (App)":       {"user": "quoc_app",    "database": "sales_db"},
    "Dave (Dev)":       {"user": "dave_dev",    "database": "sales_db"},
    # --- IT Admin ---
    "Thanh (Admin)":    {"user": "thanh_admin", "database": "mysql"},
}

def run_connection_test():
    """Lặp qua tất cả user và kiểm tra kết nối của họ."""
    print("--- BẮT ĐẦU KIỂM TRA KẾT NỐI SANDBOX ---")
    all_successful = True
    
    for name, config in USER_CONFIGS.items():
        conn = None
        # In ra thông báo đang kiểm tra, căn lề để dễ nhìn
        print(f"[*] Đang kiểm tra {name:<20}...", end="")
        
        try:
            # Cố gắng kết nối
            conn = mysql.connector.connect(
                host=SANDBOX_HOST,
                port=SANDBOX_PORT,
                user=config["user"],
                password="password", # Mật khẩu được ghi cứng
                database=config["database"]
            )
            
            # Kiểm tra xem kết nối có thực sự hoạt động không
            if conn.is_connected():
                print(" ✅ THÀNH CÔNG")
            else:
                # Trường hợp hiếm gặp: kết nối được nhưng không active
                print(" ❌ THẤT BẠI (Kết nối không hoạt động)")
                all_successful = False

        except mysql.connector.Error as err:
            # Bắt lỗi và in ra thông báo chi tiết
            print(f" ❌ THẤT BẠI")
            print(f"    └──> Lỗi: {err}")
            all_successful = False
            
        finally:
            # Đảm bảo kết nối luôn được đóng
            if conn and conn.is_connected():
                conn.close()

    print("\n--- KIỂM TRA HOÀN TẤT ---")
    if all_successful:
        print("🎉 Tuyệt vời! Tất cả các tài khoản đã kết nối thành công.")
    else:
        print("⚠️ Có lỗi xảy ra. Vui lòng kiểm tra các tài khoản báo THẤT BẠI.")
        print("   Gợi ý: Kiểm tra xem user đã được tạo chưa, mật khẩu có đúng là 'password' không, và đã được cấp quyền (GRANT) vào database tương ứng chưa.")

if __name__ == "__main__":
    # Kiểm tra xem thư viện đã được cài đặt chưa
    if 'mysql.connector' not in sys.modules:
        print("Lỗi: Thư viện 'mysql-connector-python' chưa được cài đặt.")
        print("Vui lòng chạy: pip install mysql-connector-python")
        sys.exit(1)
        
    run_connection_test()