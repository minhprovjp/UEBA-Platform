# simulation/setup_full_environment.py
import mysql.connector
from faker import Faker
import random
import json
import os

# --- CẤU HÌNH ---
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root", 
    "password": "root", # <--- PASSWORD ROOT
    "auth_plugin": "mysql_native_password"
}
COMMON_USER_PASSWORD = "password"
USERS_CONFIG_FILE = "simulation/users_config.json" # File cấu hình user

fake = Faker()

def get_conn(db=None):
    cfg = DB_CONFIG.copy()
    if db: cfg["database"] = db
    return mysql.connector.connect(**cfg)

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

def populate_data_and_export():
    print("\n🌱 2. ĐANG ĐỔ DỮ LIỆU MẪU (SEEDING)...")
    db_state = {
        "customer_ids": [], "product_ids": [], "product_skus": [],
        "product_categories": [], "cities": [],
        "employee_ids": [], "dept_ids": [], "campaign_ids": []
    }

    # Seed Sales
    conn = get_conn("sales_db"); cursor = conn.cursor()

    # 1. Customers
    print("   -> Seeding 2000 Customers...")
    cust_data = []
    for _ in range(2000):
        city = fake.city()
        db_state["cities"].append(city)
        cust_data.append((fake.name(), fake.unique.email(), fake.phone_number()[:50], fake.address(), city, random.choice(['Retail','Wholesale','VIP']), fake.date_this_year()))
    cursor.executemany("INSERT INTO customers (name, email, phone, address, city, segment, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)", cust_data)
    conn.commit()
    # Lấy ID thật ra
    cursor.execute("SELECT customer_id FROM customers")
    db_state["customer_ids"] = [row[0] for row in cursor.fetchall()]

    # 2. Products 
    print("   -> Seeding 500 Products...")
    prod_data = []
    categories = ['Electronics', 'Furniture', 'Clothing', 'Toys', 'Books']
    db_state["product_categories"] = categories
    for _ in range(500):
        sku = f"SKU-{fake.unique.ean8()}"
        prod_data.append((fake.word().title(), random.choice(categories), random.uniform(10, 2000), sku, fake.company(), fake.date_this_year()))
    cursor.executemany("INSERT INTO products (name, category, price, sku, supplier, created_at) VALUES (%s,%s,%s,%s,%s,%s)", prod_data)
    conn.commit()
    cursor.execute("SELECT id, sku, price FROM products")
    products = cursor.fetchall()
    db_state["product_ids"] = [p[0] for p in products]
    db_state["product_skus"] = [p[1] for p in products]

    # Inventory
    inv_data = [(pid, random.randint(0, 1000), f"Zone-{random.choice('ABC')}", fake.date_this_month()) for pid in db_state["product_ids"]]
    cursor.executemany("INSERT INTO inventory (product_id, stock_quantity, warehouse_location, last_restock_date) VALUES (%s,%s,%s,%s)", inv_data)

    # 3. Campaigns
    print("   -> Seeding 50 Campaigns...")
    camp_data = []
    for _ in range(20):
        status = random.choice(['Running', 'Ended', 'Planned', 'Paused'])
        camp_data.append((fake.catch_phrase(), 'Social Media', random.uniform(1000, 50000), '2025-01-01', '2025-12-31'))
    cursor.executemany("INSERT INTO marketing_campaigns (name, type, budget, start_date, end_date) VALUES (%s,%s,%s,%s,%s)", camp_data)
    conn.commit()
    cursor.execute("SELECT campaign_id FROM marketing_campaigns")
    db_state["campaign_ids"] = [row[0] for row in cursor.fetchall()]
    
    # 4. Orders
    print("   -> Seeding 20,000 Orders...")
    order_batch = []
    for _ in range(20000):
        cid = random.choice(db_state["customer_ids"])
        order_batch.append((cid, fake.date_this_year(), random.uniform(50, 5000), random.choice(['Completed', 'Pending', 'Cancelled']), random.choice(['Credit Card', 'PayPal'])))
    cursor.executemany("INSERT INTO orders (customer_id, order_date, total_amount, status, payment_method) VALUES (%s,%s,%s,%s,%s)", order_batch)
    conn.commit()
    
    print("   -> Create Order Details...")
    cursor.execute("SELECT order_id FROM orders")
    order_ids = [r[0] for r in cursor.fetchall()]
    item_batch = []
    for oid in order_ids:
        for _ in range(random.randint(1, 3)):
            prod = random.choice(products)
            item_batch.append((oid, prod[0], random.randint(1, 5), prod[2]))
    
    batch_size = 5000
    for i in range(0, len(item_batch), batch_size):
        cursor.executemany("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s,%s,%s,%s)", item_batch[i:i+batch_size])
    conn.commit()
    conn.close()

    # Seed HR
    conn = get_conn("hr_db"); cursor = conn.cursor()
    depts = ['Sales', 'Marketing', 'IT', 'HR', 'Finance']
    for d in depts: cursor.execute("INSERT INTO departments (dept_name, location) VALUES (%s, %s)", (d, 'HQ'))
    conn.commit()
    cursor.execute("SELECT dept_id FROM departments")
    db_state["dept_ids"] = [row[0] for row in cursor.fetchall()]

    # 6. Employees
    print("   -> Seeding 200 Employees...")
    emp_data = []
    for _ in range(200):
        emp_data.append((fake.name(), fake.email(), fake.job(), random.choice(db_state["dept_ids"]), fake.date_this_decade(), random.uniform(3000, 15000)))
    cursor.executemany("INSERT INTO employees (name, email, position, dept_id, hire_date, salary) VALUES (%s,%s,%s,%s,%s,%s)", emp_data)
    conn.commit()
    cursor.execute("SELECT employee_id FROM employees")
    db_state["employee_ids"] = [row[0] for row in cursor.fetchall()]

    # 7. Salaries & Attendance
    sal_data = [(eid, random.uniform(3000, 20000), random.uniform(0, 5000), fake.date_this_month()) for eid in db_state["employee_ids"]]
    cursor.executemany("INSERT INTO salaries (employee_id, amount, bonus, payment_date) VALUES (%s,%s,%s,%s)", sal_data)
    conn.commit()
    conn.close()

    # Làm sạch list set
    db_state["cities"] = list(set(db_state["cities"]))
    os.makedirs("simulation", exist_ok=True)
    with open("simulation/db_state.json", "w") as f: json.dump(db_state, f, indent=2)
    print("💾 Đã xuất file 'simulation/db_state.json'.")

# --- HÀM TẠO USER & XUẤT CONFIG (QUAN TRỌNG) ---
def setup_users():
    print("\n👤 3. TẠO USER VÀ XUẤT CONFIG...")
    conn = get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT User, Host FROM mysql.user WHERE User LIKE '%_user%' OR User IN ('dave_insider', 'intern_temp')")
    for u, h in cursor.fetchall(): cursor.execute(f"DROP USER '{u}'@'{h}'")
    
    # Cấu trúc lưu file config
    exported_users = {
        "SALES": [],
        "HR": [],
        "DEV": [],
        "BAD_ACTOR": [],
        "VULNERABLE": []
    }

    # Định nghĩa số lượng và quyền
    user_definitions = [
        ("sale_user", "SALES", 6, [
        	("sales_db", ["SELECT", "INSERT", "UPDATE"]),
        	# KHÔNG CÓ QUYỀN HR_DB -> Để test lỗi Access Denied
        ]),
        ("hr_user", "HR", 2, [
        	("hr_db", ["SELECT", "INSERT", "UPDATE"])
        ]),
        ("dev_user", "DEV", 3, [
        	("sales_db", ["SELECT", "INSERT", "UPDATE", "DELETE"]), ("hr_db", ["SELECT", "INSERT", "UPDATE", "DELETE"])
        ]),
        # Dave Insider: Giả vờ là user thường, quyền rất thấp
        ("dave_insider", "BAD_ACTOR", 1, [
        	("sales_db", ["SELECT"])
        ]),
        # User 'intern_temp': Mật khẩu yếu, bị hacker chiếm dụng để login.
        # Quyền hạn: Rỗng (Chỉ login được, chạy lệnh gì cũng lỗi)
        ("intern_temp", "VULNERABLE", 1, [])
    ]

    for prefix, category, count, perms in user_definitions:
        for i in range(count):
            if count == 1: username = prefix
            else: username = f"{prefix}_{i}"
            
            # Lưu vào danh sách
            if category in exported_users:
                exported_users[category].append(username)

            try:
                cursor.execute(f"CREATE USER '{username}'@'%' IDENTIFIED BY '{COMMON_USER_PASSWORD}'")
            except: pass

            for db, rights in perms:
                privs = ", ".join(rights)
                cursor.execute(f"GRANT {privs} ON {db}.* TO '{username}'@'%'")
            
            cursor.execute(f"GRANT USAGE ON *.* TO '{username}'@'%'")

    cursor.execute("FLUSH PRIVILEGES")
    conn.close()
  	
    with open(USERS_CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(exported_users, f, indent=2)
    
    print(f"✅ Đã lưu danh sách user vào: {USERS_CONFIG_FILE}")

if __name__ == "__main__":
    setup_database_structure()
    populate_data_and_export()
    setup_users()
    print("\n🎉 MÔI TRƯỜNG FINAL ĐÃ SẴN SÀNG!")