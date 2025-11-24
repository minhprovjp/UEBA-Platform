import mysql.connector
from faker import Faker
import random

# --- CẤU HÌNH ADMIN (Dùng root để có quyền tạo DB/User) ---
ADMIN_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",      # Bắt buộc dùng root hoặc user có quyền GRANT/CREATE
    "password": "root" 
}

fake = Faker()

def get_conn(db=None):
    cfg = ADMIN_CONFIG.copy()
    if db: cfg["database"] = db
    return mysql.connector.connect(**cfg)

def setup_databases_and_tables():
    print("--- 1. TẠO DATABASE & TABLES ---")
    conn = get_conn()
    cursor = conn.cursor()

    # 1.1 Tạo Databases
    dbs = ["sales_db", "hr_db", "admin_db"]
    for db in dbs:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        print(f"✅ Database '{db}' đã sẵn sàng.")
    conn.close()

    # 1.2 Tạo Tables cho Sales DB
    conn = get_conn("sales_db")
    cursor = conn.cursor()
    
    # Bảng Products
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(100),
            price DECIMAL(10, 2),
            stock INT
        )
    """)
    
    # Bảng Customers
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            email VARCHAR(255),
            city VARCHAR(100),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Bảng Orders
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INT AUTO_INCREMENT PRIMARY KEY,
            customer_id INT,
            amount DECIMAL(10, 2),
            status VARCHAR(50),
            order_date DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✅ Tables trong 'sales_db' đã tạo xong.")
    conn.close()

    # 1.3 Tạo Tables cho HR DB
    conn = get_conn("hr_db")
    cursor = conn.cursor()
    
    # Bảng Employees
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            position VARCHAR(100),
            department VARCHAR(100),
            joined_date DATE
        )
    """)
    
    # Bảng Salaries (Nhạy cảm)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS salaries (
            id INT AUTO_INCREMENT PRIMARY KEY,
            employee_id INT,
            amount DECIMAL(15, 2),
            bonus DECIMAL(15, 2),
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✅ Tables trong 'hr_db' đã tạo xong.")
    conn.close()

def seed_initial_data():
    print("\n--- 2. CHÈN DỮ LIỆU MỒI (SEED DATA) ---")
    # Seed Products
    conn = get_conn("sales_db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    if cursor.fetchone()[0] == 0:
        print("   -> Đang tạo 100 sản phẩm mẫu...")
        for _ in range(100):
            sql = "INSERT INTO products (name, category, price, stock) VALUES (%s, %s, %s, %s)"
            val = (fake.word().title(), random.choice(['Electronics', 'Books', 'Home']), 
                   random.uniform(10, 500), random.randint(0, 100))
            cursor.execute(sql, val)
        conn.commit()
    
    # Seed Customers
    cursor.execute("SELECT COUNT(*) FROM customers")
    if cursor.fetchone()[0] == 0:
        print("   -> Đang tạo 200 khách hàng mẫu...")
        for _ in range(200):
            sql = "INSERT INTO customers (name, email, city) VALUES (%s, %s, %s)"
            val = (fake.name(), fake.email(), fake.city())
            cursor.execute(sql, val)
        conn.commit()
    conn.close()

    # Seed HR
    conn = get_conn("hr_db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM employees")
    if cursor.fetchone()[0] == 0:
        print("   -> Đang tạo 50 hồ sơ nhân viên...")
        for i in range(50):
            # Employee
            sql_emp = "INSERT INTO employees (name, position, department, joined_date) VALUES (%s, %s, %s, %s)"
            val_emp = (fake.name(), fake.job(), random.choice(['Sales', 'Dev', 'HR']), fake.date_this_decade())
            cursor.execute(sql_emp, val_emp)
            emp_id = cursor.lastrowid
            
            # Salary
            sql_sal = "INSERT INTO salaries (employee_id, amount, bonus) VALUES (%s, %s, %s)"
            val_sal = (emp_id, random.uniform(1000, 5000), random.uniform(0, 1000))
            cursor.execute(sql_sal, val_sal)
        conn.commit()
    conn.close()
    print("✅ Dữ liệu mẫu đã sẵn sàng.")

def create_enterprise_users():
    print("\n--- 3. TẠO USER DOANH NGHIỆP (MYSQL USERS) ---")
    conn = get_conn()
    cursor = conn.cursor()
    
    # Danh sách user cần tạo (Khớp với logic trong turbo_traffic_gen.py)
    # 20 Sales, 10 Dev, 5 HR
    users_to_create = []
    for i in range(20): users_to_create.append( (f"sale_user_{i}", "sales_db") )
    for i in range(10): users_to_create.append( (f"dev_user_{i}", "sales_db") )
    for i in range(5):  users_to_create.append( (f"hr_user_{i}", "hr_db") )
    
    # Insider Threat
    users_to_create.append( ("dave_insider", "sales_db") )

    created_count = 0
    for username, default_db in users_to_create:
        try:
            # Tạo User
            cursor.execute(f"CREATE USER IF NOT EXISTS '{username}'@'%' IDENTIFIED BY 'password';")
            
            # Cấp quyền (Grant)
            if "sale" in username or "dev" in username or "dave" in username:
                cursor.execute(f"GRANT SELECT, INSERT, UPDATE ON sales_db.* TO '{username}'@'%';")
            
            if "hr" in username:
                cursor.execute(f"GRANT SELECT, INSERT, UPDATE ON hr_db.* TO '{username}'@'%';")
                # HR được quyền xem user hệ thống (để test privilege)
                cursor.execute(f"GRANT SELECT ON mysql.user TO '{username}'@'%';")

            created_count += 1
        except Exception as e:
            print(f"⚠️ Lỗi tạo user {username}: {e}")
            
    cursor.execute("FLUSH PRIVILEGES;")
    conn.close()
    print(f"✅ Đã cấu hình xong {created_count} users.")

if __name__ == "__main__":
    try:
        setup_databases_and_tables()
        seed_initial_data()
        create_enterprise_users()
        print("\n🎉 MÔI TRƯỜNG ĐÃ SẴN SÀNG CHO TURBO GENERATOR!")
    except Exception as e:
        print(f"\n❌ CÓ LỖI XẢY RA: {e}")
        print("Gợi ý: Kiểm tra xem mật khẩu root trong biến ADMIN_CONFIG đã đúng chưa?")