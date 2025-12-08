# simulation/setup_full_environment.py
import mysql.connector
from faker import Faker
import random
import json
import os
import unicodedata
import re

# --- CẤU HÌNH ---
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root", 
    "password": "root", 
    "auth_plugin": "mysql_native_password"
}
COMMON_USER_PASSWORD = "password"
USERS_CONFIG_FILE = "simulation/users_config.json"
DB_STATE_FILE = "simulation/db_state.json"

# Sử dụng Locale tiếng Việt
fake = Faker('vi_VN')

# --- HELPER FUNCTIONS ---

def remove_accents(input_str):
    """Chuyển 'Nguyễn Văn Nam' -> 'nguyen.van.nam'"""
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    s = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    return ".".join(s.lower().split())

def generate_corporate_email(name, company_domain="uba-corp.com"):
    # Chuyển "Nguyễn Văn An" -> "nguyen van an"
    clean_name = remove_accents(name).replace('.', ' ')
    parts = clean_name.split()
    
    # Logic: an.nguyen@...
    if len(parts) >= 2:
        email_prefix = f"{parts[-1]}.{parts[0]}"
        if len(parts) > 2:
            middle = "".join([p[0] for p in parts[1:-1]])
            email_prefix += f".{middle}"
    else:
        email_prefix = parts[0]
        
    if random.random() < 0.3:
        email_prefix += str(random.randint(1, 99))
        
    return f"{email_prefix}@{company_domain}"

def get_conn(db=None):
    cfg = DB_CONFIG.copy()
    if db: cfg["database"] = db
    return mysql.connector.connect(**cfg)

# --- 1. CẤU TRÚC DATABASE ---
def setup_database_structure():
    print("🚀 1. KHỞI TẠO CẤU TRÚC DATABASE...")
    conn = get_conn()
    cursor = conn.cursor()
    for db in ['sales_db', 'hr_db', 'admin_db']:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db} CHARACTER SET utf8mb4")
    
    # --- SALES DB ---
    conn.close(); conn = get_conn("sales_db"); cursor = conn.cursor()
    tables = ["reviews", "order_items", "orders", "inventory", "products", "marketing_campaigns", "customers"]
    for t in tables: cursor.execute(f"DROP TABLE IF EXISTS {t}")

    cursor.execute("""
        CREATE TABLE customers (
            customer_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100), email VARCHAR(100) UNIQUE,
            phone VARCHAR(50), address TEXT, city VARCHAR(50),
            segment VARCHAR(20), created_at DATETIME
        )
    """)
    cursor.execute("""
        CREATE TABLE products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(150), category VARCHAR(50),
            price DECIMAL(10,2), sku VARCHAR(50) UNIQUE,
            supplier VARCHAR(100), created_at DATETIME
        )
    """)
    cursor.execute("""
        CREATE TABLE inventory (
            product_id INT PRIMARY KEY,
            stock_quantity INT, warehouse_location VARCHAR(50),
            last_restock_date DATETIME,
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    """)
    cursor.execute("""
        CREATE TABLE marketing_campaigns (
            campaign_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100), type VARCHAR(50),
            status VARCHAR(20) DEFAULT 'Planned',
            budget DECIMAL(12,2), start_date DATE, end_date DATE
        )
    """)
    cursor.execute("""
        CREATE TABLE orders (
            order_id INT AUTO_INCREMENT PRIMARY KEY,
            customer_id INT, order_date DATETIME,
            total_amount DECIMAL(12,2), status VARCHAR(20),
            payment_method VARCHAR(20),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE order_items (
            item_id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT, product_id INT,
            quantity INT, unit_price DECIMAL(10,2),
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    """)
    cursor.execute("""
        CREATE TABLE reviews (
            review_id INT AUTO_INCREMENT PRIMARY KEY,
            product_id INT, customer_id INT,
            rating INT, comment TEXT, review_date DATE,
            FOREIGN KEY (product_id) REFERENCES products(id),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    conn.close()

    # --- HR DB ---
    conn = get_conn("hr_db"); cursor = conn.cursor()
    tables = ["salaries", "attendance", "employees", "departments"]
    for t in tables: cursor.execute(f"DROP TABLE IF EXISTS {t}")

    cursor.execute("""
        CREATE TABLE departments (
            dept_id INT AUTO_INCREMENT PRIMARY KEY,
            dept_name VARCHAR(50), location VARCHAR(50)
        )
    """)
    cursor.execute("""
        CREATE TABLE employees (
            employee_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100), email VARCHAR(100),
            position VARCHAR(100), dept_id INT,
            hire_date DATE, salary DECIMAL(10,2),
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE salaries (
            salary_id INT AUTO_INCREMENT PRIMARY KEY,
            employee_id INT, amount DECIMAL(12,2),
            bonus DECIMAL(10,2), payment_date DATE,
            FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE attendance (
            record_id INT AUTO_INCREMENT PRIMARY KEY,
            employee_id INT, date DATE,
            check_in TIME, check_out TIME, status VARCHAR(20),
            FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
        )
    """)
    conn.close()
    print("✅ Cấu trúc Database đã sẵn sàng.")

# --- 2. DỮ LIỆU GIẢ LẬP ---
def populate_data_and_export():
    print("\n🌱 2. ĐANG ĐỔ DỮ LIỆU MẪU (Dữ liệu Việt Nam hóa)...")
    
    db_state = {
        "customer_ids": [], "product_ids": [], "product_skus": [],
        "product_categories": [], "cities": [],
        "employee_ids": [], "dept_ids": [], "campaign_ids": [],
        "order_ids": [] 
    }

    # Seed Sales
    conn = get_conn("sales_db"); cursor = conn.cursor()

    # --- 2.1 Customers (FIX LỖI DUPLICATE EMAIL) ---
    print("   -> Seeding 2000 Customers (VN)...")
    cust_data = []
    vn_cities = ["Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ", "Nha Trang", "Vũng Tàu", "Bình Dương"]
    
    generated_emails = set()

    for _ in range(2000):
        name = fake.name()
        city = random.choice(vn_cities)
        db_state["cities"].append(city)
        
        base_email = generate_corporate_email(name, random.choice(["gmail.com", "yahoo.com", "outlook.com"]))
        email = base_email
        counter = 1
        while email in generated_emails:
            user_part, domain_part = base_email.split('@')
            email = f"{user_part}{counter}@{domain_part}"
            counter += 1
        generated_emails.add(email)

        cust_data.append((
            name, email, fake.phone_number(), fake.address(), city, 
            random.choice(['Retail','Wholesale','VIP']), fake.date_this_year()
        ))
    
    cursor.executemany("INSERT INTO customers (name, email, phone, address, city, segment, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)", cust_data)
    conn.commit()
    
    cursor.execute("SELECT customer_id FROM customers")
    db_state["customer_ids"] = [row[0] for row in cursor.fetchall()]

    # --- 2.2 Products ---
    print("   -> Seeding 500 Products...")
    prod_data = []
    categories = ['Điện tử', 'Nội thất', 'Thời trang', 'Đồ chơi', 'Sách']
    db_state["product_categories"] = categories
    for _ in range(500):
        adjectives = ["Cao cấp", "Giá rẻ", "Thông minh", "Nhập khẩu", "Thế hệ mới"]
        nouns = ["Laptop", "Ghế sofa", "Áo thun", "Robot", "Tiểu thuyết"]
        prod_name = f"{random.choice(nouns)} {fake.word()} {random.choice(adjectives)}"
        sku = f"VN-{fake.unique.ean8()}"
        prod_data.append((prod_name, random.choice(categories), random.uniform(50000, 50000000), sku, fake.company(), fake.date_this_year()))
        
    cursor.executemany("INSERT INTO products (name, category, price, sku, supplier, created_at) VALUES (%s,%s,%s,%s,%s,%s)", prod_data)
    conn.commit()
    cursor.execute("SELECT id, sku FROM products")
    products = cursor.fetchall()
    db_state["product_ids"] = [p[0] for p in products]
    db_state["product_skus"] = [p[1] for p in products]

    # Inventory
    inv_data = [(pid, random.randint(0, 1000), f"Kho-{random.choice(['A','B','C'])}-{random.randint(1,10)}", fake.date_this_month()) for pid in db_state["product_ids"]]
    cursor.executemany("INSERT INTO inventory (product_id, stock_quantity, warehouse_location, last_restock_date) VALUES (%s,%s,%s,%s)", inv_data)

    # --- 2.3 Campaigns ---
    print("   -> Seeding 50 Campaigns...")
    camp_data = []
    camp_types = ['Facebook Ads', 'Google Ads', 'TVC', 'Email Marketing', 'KOL Booking']
    for _ in range(50):
        camp_name = f"Chiến dịch {fake.catch_phrase()}"
        camp_data.append((camp_name, random.choice(camp_types), random.uniform(10000000, 500000000), '2025-01-01', '2025-12-31'))
    cursor.executemany("INSERT INTO marketing_campaigns (name, type, budget, start_date, end_date) VALUES (%s,%s,%s,%s,%s)", camp_data)
    conn.commit()
    cursor.execute("SELECT campaign_id FROM marketing_campaigns")
    db_state["campaign_ids"] = [row[0] for row in cursor.fetchall()]
    
    # --- 2.4 Orders ---
    print("   -> Seeding 20,000 Orders...")
    order_batch = []
    for _ in range(20000):
        cid = random.choice(db_state["customer_ids"])
        order_batch.append((cid, fake.date_this_year(), 0, random.choice(['Completed', 'Pending', 'Cancelled']), random.choice(['Momo', 'ZaloPay', 'COD', 'Bank Transfer'])))
    cursor.executemany("INSERT INTO orders (customer_id, order_date, total_amount, status, payment_method) VALUES (%s,%s,%s,%s,%s)", order_batch)
    conn.commit()
    
    cursor.execute("SELECT order_id FROM orders")
    db_state["order_ids"] = [row[0] for row in cursor.fetchall()]
    
    # --- 2.5 Order Items (Logic tối ưu) ---
    print("   -> Seeding Order Details (Realistic & Consistent)...")
    cursor.execute("SELECT id, price FROM products")
    product_price_map = {row[0]: float(row[1]) for row in cursor.fetchall()}
    
    item_batch = []
    all_order_ids = db_state["order_ids"]
    for oid in all_order_ids:
        num_items = random.randint(1, 5)
        selected_pids = random.sample(db_state["product_ids"], k=min(len(db_state["product_ids"]), num_items))
        for pid in selected_pids:
            quantity = random.randint(1, 10)
            real_price = product_price_map.get(pid, 100000.0)
            final_price = real_price * random.uniform(0.9, 1.0)
            item_batch.append((oid, pid, quantity, final_price))

    batch_size = 5000
    total_items = len(item_batch)
    print(f"      - Tổng số items: {total_items}")
    for i in range(0, total_items, batch_size):
        batch = item_batch[i:i + batch_size]
        cursor.executemany("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s,%s,%s,%s)", batch)
        conn.commit()
        print(f"      - Đã insert: {min(i + batch_size, total_items)}/{total_items}", end='\r')
    
    print("\n   -> Updating Orders Total Amount (Data Integrity)...")
    cursor.execute("""
        UPDATE orders o
        JOIN (
            SELECT order_id, SUM(quantity * unit_price) as real_total
            FROM order_items
            GROUP BY order_id
        ) items ON o.order_id = items.order_id
        SET o.total_amount = items.real_total;
    """)
    conn.commit()
    conn.close()

    # --- 2.6 HR DB ---
    conn = get_conn("hr_db"); cursor = conn.cursor()
    depts = ['Kinh doanh', 'Marketing', 'Kỹ thuật', 'Nhân sự', 'Tài chính']
    for d in depts: cursor.execute("INSERT INTO departments (dept_name, location) VALUES (%s, %s)", (d, 'Trụ sở chính'))
    conn.commit()
    cursor.execute("SELECT dept_id FROM departments")
    db_state["dept_ids"] = [row[0] for row in cursor.fetchall()]

    # Employees
    print("   -> Seeding 200 Employees (VN)...")
    emp_data = []
    emp_emails = set()
    positions = ['Nhân viên', 'Trưởng nhóm', 'Giám đốc', 'Thực tập sinh']
    
    for _ in range(200):
        name = fake.name()
        base_email = generate_corporate_email(name, "uba-corp.com.vn")
        email = base_email
        counter = 1
        while email in emp_emails:
            user_part, domain_part = base_email.split('@')
            email = f"{user_part}{counter}@{domain_part}"
            counter += 1
        emp_emails.add(email)

        emp_data.append((name, email, random.choice(positions), random.choice(db_state["dept_ids"]), fake.date_this_decade(), random.uniform(10000000, 50000000)))
    
    cursor.executemany("INSERT INTO employees (name, email, position, dept_id, hire_date, salary) VALUES (%s,%s,%s,%s,%s,%s)", emp_data)
    conn.commit()
    cursor.execute("SELECT employee_id FROM employees")
    db_state["employee_ids"] = [row[0] for row in cursor.fetchall()]

    # Salaries
    sal_data = [(eid, random.uniform(10000000, 50000000), random.uniform(0, 5000000), fake.date_this_month()) for eid in db_state["employee_ids"]]
    cursor.executemany("INSERT INTO salaries (employee_id, amount, bonus, payment_date) VALUES (%s,%s,%s,%s)", sal_data)
    conn.commit()
    conn.close()

    # Lưu db_state
    db_state["cities"] = list(set(db_state["cities"]))
    os.makedirs("simulation", exist_ok=True)
    with open(DB_STATE_FILE, "w", encoding='utf-8') as f: json.dump(db_state, f, indent=2)
    print(f"💾 Đã xuất file '{DB_STATE_FILE}'")

# --- 3. TẠO USER HỆ THỐNG & PHÂN QUYỀN (MERGED LOGIC) ---
def setup_users_and_permissions():
    print("\n👤 3. ĐANG TẠO USER TÊN THẬT & PHÂN QUYỀN...")
    conn = get_conn()
    cur = conn.cursor()
    
    # 1. Xóa user cũ
    cur.execute("SELECT User, Host FROM mysql.user WHERE User LIKE '%_user%' OR User LIKE '%.%' OR User IN ('dave_insider', 'guest_temp', 'script_kiddie', 'intern_temp')")
    for u, h in cur.fetchall(): 
        if u not in ['root', 'mysql.session', 'mysql.sys', 'mysql.infoschema', 'uba_user']:
            try: cur.execute(f"DROP USER '{u}'@'{h}'")
            except: pass

    # 2. Định nghĩa Role & Permissions
    # Cấu trúc: Role -> { DB: [Actions] }
    # Dùng để lưu vào config file cho hệ thống mô phỏng
    ROLE_PERMISSIONS = {
        "SALES": { "sales_db": ["SELECT", "INSERT", "UPDATE"], "hr_db": [] },
        "HR": { "sales_db": ["SELECT"], "hr_db": ["SELECT", "INSERT", "UPDATE"] },
        "DEV": { "sales_db": ["SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "ALTER"], "hr_db": ["SELECT", "INSERT", "UPDATE"], "mysql": ["SELECT"] },
        "ADMIN": { "*": ["ALL"] },
        "BAD_ACTOR": { "sales_db": ["SELECT"] },
        "VULNERABLE": {}
    }

    # 3. Tạo User thật
    teams = [
        ("SALES", 20),
        ("HR", 5),
        ("DEV", 10),
        ("ADMIN", 2)
    ]

    user_map = {} # username -> role

    for role, count in teams:
        for _ in range(count):
            full_name = fake.name()
            username = remove_accents(full_name)[:30]
            
            # Tránh trùng username
            while username in user_map:
                username += str(random.randint(1,9))
            
            user_map[username] = role
            
            try:
                cur.execute(f"CREATE USER '{username}'@'%' IDENTIFIED BY '{COMMON_USER_PASSWORD}'")
                
                # Cấp quyền dựa trên Role
                if role == "SALES":
                    cur.execute(f"GRANT SELECT, INSERT, UPDATE ON sales_db.* TO '{username}'@'%'")
                elif role == "HR":
                    cur.execute(f"GRANT SELECT ON sales_db.* TO '{username}'@'%'")
                    cur.execute(f"GRANT SELECT, INSERT, UPDATE ON hr_db.* TO '{username}'@'%'")
                elif role == "DEV":
                    cur.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE, DROP, ALTER ON sales_db.* TO '{username}'@'%'")
                    cur.execute(f"GRANT SELECT, INSERT, UPDATE ON hr_db.* TO '{username}'@'%'")
                    cur.execute(f"GRANT SELECT ON mysql.* TO '{username}'@'%'")
                elif role == "ADMIN":
                    cur.execute(f"GRANT ALL PRIVILEGES ON *.* TO '{username}'@'%'")
                    
                cur.execute(f"GRANT USAGE ON *.* TO '{username}'@'%'")
            except Exception as e:
                print(f"⚠️ Lỗi tạo {username}: {e}")

    # 4. Tạo Bad Actors (Cố định để kịch bản tấn công dùng)
    special_users = {
        "dave.insider": "BAD_ACTOR",
        "intern_temp": "VULNERABLE",
        "script.kiddie": "EXTERNAL_HACKER" # Dùng làm nguồn tấn công từ ngoài
    }

    for u, r in special_users.items():
        try:
            cur.execute(f"CREATE USER '{u}'@'%' IDENTIFIED BY '{COMMON_USER_PASSWORD}'")
            if r == "BAD_ACTOR":
                cur.execute(f"GRANT SELECT ON sales_db.* TO '{u}'@'%'")
            cur.execute(f"GRANT USAGE ON *.* TO '{u}'@'%'")
            user_map[u] = r
        except: pass

    cur.execute("FLUSH PRIVILEGES")
    conn.close()
    
    # Lưu config
    config_data = {
        "roles": ROLE_PERMISSIONS,
        "users": user_map
    }
    with open(USERS_CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config_data, f, indent=2)
    print(f"✅ Đã lưu file phân quyền: {USERS_CONFIG_FILE}")

# --- 4. CẤU HÌNH HỆ THỐNG ---
def setup_system_config():
    print("\n⚙️ 4. CẤU HÌNH PERFORMANCE SCHEMA...")
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE performance_schema.setup_consumers SET ENABLED = 'YES' WHERE NAME = 'events_statements_history_long'")
        cursor.execute("UPDATE performance_schema.setup_instruments SET ENABLED = 'YES', TIMED = 'YES' WHERE NAME LIKE 'statement/%'")
        
        cursor.execute("DROP USER IF EXISTS 'uba_user'@'localhost'")
        cursor.execute("CREATE USER 'uba_user'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password'")
        cursor.execute("GRANT SELECT ON *.* TO 'uba_user'@'localhost'")
        cursor.execute("FLUSH PRIVILEGES")
        print("✅ Cấu hình hệ thống hoàn tất.")
    except Exception as e: print(f"⚠️ Lỗi cấu hình hệ thống: {e}")
    finally: conn.close()

if __name__ == "__main__":
    setup_database_structure()
    populate_data_and_export()
    setup_users_and_permissions()
    setup_system_config()
    print("\n🎉 MÔI TRƯỜNG FINAL (DATASET VIỆT NAM) ĐÃ SẴN SÀNG!")