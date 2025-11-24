import mysql.connector
import random
import logging
from datetime import datetime, timedelta

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configuration
DB_HOST = "localhost"
DB_PORT = 3306
DB_ROOT_USER = "root"
DB_ROOT_PASS = "root"  # CHANGE THIS to your actual root password
DEFAULT_USER_PASS = "password"

# List of users exactly as per your SQL file
USERS_CONFIG = {
    "sales_team": ["anh_sales", "linh_sales", "quang_sales", "trang_sales"],
    "marketing_team": ["binh_mkt", "mai_mkt", "vy_mkt"],
    "hr_team": ["chi_hr", "hoa_hr"],
    "support_team": ["dung_support", "loan_support", "khang_support"],
    "dev_team": ["em_dev", "tam_dev", "ly_data", "quoc_app", "dave_dev"],
    "admin": ["thanh_admin"],
    "monitoring": ["uba_user"]
}

def get_connection():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_ROOT_USER, password=DB_ROOT_PASS
    )

def setup_database():
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        logging.info("--- 1. CLEANING OLD DATABASES & USERS ---")
        cursor.execute("DROP DATABASE IF EXISTS sales_db")
        cursor.execute("DROP DATABASE IF EXISTS hr_db")
        cursor.execute("DROP DATABASE IF EXISTS admin_db")

        # Collect all users to drop them
        all_users = [u for team in USERS_CONFIG.values() for u in team]
        for user in all_users:
            cursor.execute(f"DROP USER IF EXISTS '{user}'@'%'")
            cursor.execute(f"DROP USER IF EXISTS '{user}'@'localhost'")

        logging.info("--- 2. CREATING SCHEMAS ---")
        
        # --- SALES_DB ---
        cursor.execute("CREATE DATABASE sales_db")
        cursor.execute("USE sales_db")
        
        cursor.execute("""
            CREATE TABLE customers (
                customer_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                city VARCHAR(50),
                registration_date DATETIME
            )
        """)
        cursor.execute("""
            CREATE TABLE products (
                sku VARCHAR(20) PRIMARY KEY,
                name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10, 2),
                stock_quantity INT DEFAULT 100
            )
        """)
        cursor.execute("""
            CREATE TABLE orders (
                order_id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT,
                order_date DATETIME,
                status VARCHAR(20),
                total_amount DECIMAL(10, 2),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE order_items (
                item_id INT AUTO_INCREMENT PRIMARY KEY,
                order_id INT,
                product_sku VARCHAR(20),
                quantity INT,
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
                FOREIGN KEY (product_sku) REFERENCES products(sku)
            )
        """)
        cursor.execute("""
            CREATE TABLE marketing_campaigns (
                campaign_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                start_date DATE,
                end_date DATE
            )
        """)

        # --- HR_DB ---
        cursor.execute("CREATE DATABASE hr_db")
        cursor.execute("USE hr_db")
        cursor.execute("""
            CREATE TABLE employees (
                employee_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                position VARCHAR(50),
                department VARCHAR(50),
                start_date DATE
            )
        """)
        cursor.execute("""
            CREATE TABLE salaries (
                salary_id INT AUTO_INCREMENT PRIMARY KEY,
                employee_id INT,
                base_salary DECIMAL(10, 2),
                bonus DECIMAL(10, 2),
                pay_date DATE,
                FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
            )
        """)

        # --- ADMIN_DB ---
        cursor.execute("CREATE DATABASE admin_db")
        cursor.execute("USE admin_db")
        cursor.execute("""
            CREATE TABLE system_logs (
                log_id INT AUTO_INCREMENT PRIMARY KEY,
                event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                service_name VARCHAR(50),
                log_level VARCHAR(10),
                message TEXT
            )
        """)

        logging.info("--- 3. CREATING USERS & GRANTING PERMISSIONS ---")
        
        # Helper to create user
        def create_user(username):
            cursor.execute(f"CREATE USER '{username}'@'%' IDENTIFIED BY '{DEFAULT_USER_PASS}'")
            cursor.execute(f"CREATE USER '{username}'@'localhost' IDENTIFIED BY '{DEFAULT_USER_PASS}'")

        # Create all users
        for team in USERS_CONFIG.values():
            for user in team:
                create_user(user)

        # --- Permissions (Matching your SQL File) ---
        
        # Sales Team
        for user in USERS_CONFIG["sales_team"]:
            cursor.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON sales_db.* TO '{user}'@'%'")

        # Marketing Team
        for user in USERS_CONFIG["marketing_team"]:
            cursor.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON sales_db.* TO '{user}'@'%'")

        # Support Team (Limited)
        for user in USERS_CONFIG["support_team"]:
            cursor.execute(f"GRANT SELECT, INSERT, UPDATE ON sales_db.* TO '{user}'@'%'")

        # Dev Team (All Privs on Sales)
        for user in USERS_CONFIG["dev_team"]:
            cursor.execute(f"GRANT ALL PRIVILEGES ON sales_db.* TO '{user}'@'%'")

        # HR Team
        for user in USERS_CONFIG["hr_team"]:
            cursor.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON hr_db.* TO '{user}'@'%'")

        # Special Curiosity Grants (Privilege Abuse Scenarios)
        cursor.execute("GRANT SELECT ON hr_db.employees TO 'dave_dev'@'%'")
        cursor.execute("GRANT SELECT ON hr_db.employees TO 'anh_sales'@'%'")

        # Admin
        cursor.execute("GRANT ALL PRIVILEGES ON *.* TO 'thanh_admin'@'%' WITH GRANT OPTION")

        # Monitoring (Publisher)
        cursor.execute("GRANT SELECT ON performance_schema.* TO 'uba_user'@'%'")
        cursor.execute("GRANT PROCESS ON *.* TO 'uba_user'@'%'")

        cursor.execute("FLUSH PRIVILEGES")

        logging.info("--- 4. POPULATING MASSIVE DATA ---")
        
        # --- Populate Sales DB ---
        cursor.execute("USE sales_db")
        
        # Products (100 rows)
        prod_data = []
        for i in range(1, 101):
            sku = f"SKU{i:03d}"
            cat = random.choice(["Electronics", "Books", "Home", "Clothing"])
            prod_data.append((sku, f"Product {i}", cat, random.uniform(10, 2000), random.randint(0, 500)))
        cursor.executemany("INSERT INTO products (sku, name, category, price, stock_quantity) VALUES (%s, %s, %s, %s, %s)", prod_data)

        # Customers (500 rows)
        cust_data = []
        cities = ["Hanoi", "Ho Chi Minh", "Da Nang", "Can Tho", "Hai Phong"]
        for i in range(1, 501):
            cust_data.append((f"Customer {i}", f"cust{i}@example.com", random.choice(cities), datetime.now() - timedelta(days=random.randint(0, 365))))
        cursor.executemany("INSERT INTO customers (name, email, city, registration_date) VALUES (%s, %s, %s, %s)", cust_data)

        # Orders (2000 rows)
        order_data = []
        for i in range(1, 2001):
            cust_id = random.randint(1, 500)
            order_data.append((cust_id, datetime.now() - timedelta(days=random.randint(0, 60)), random.choice(['Pending', 'Completed', 'Cancelled']), random.uniform(20, 1000)))
        cursor.executemany("INSERT INTO orders (customer_id, order_date, status, total_amount) VALUES (%s, %s, %s, %s)", order_data)

        # Order Items (Generate loosely linked to orders)
        # (Skipping complex linking for speed, just populating dummy items implies existence)
        item_data = []
        for i in range(1, 4001):
            order_id = random.randint(1, 2000)
            sku = f"SKU{random.randint(1,100):03d}"
            item_data.append((order_id, sku, random.randint(1, 5)))
        cursor.executemany("INSERT INTO order_items (order_id, product_sku, quantity) VALUES (%s, %s, %s)", item_data)

        # Marketing Campaigns
        camp_data = [
            ("Summer Sale", "2025-06-01", "2025-06-30"),
            ("Black Friday", "2025-11-20", "2025-11-30"),
            ("Tet Holiday", "2025-01-15", "2025-02-15")
        ]
        cursor.executemany("INSERT INTO marketing_campaigns (name, start_date, end_date) VALUES (%s, %s, %s)", camp_data)

        # --- Populate HR DB ---
        cursor.execute("USE hr_db")
        
        emp_data = []
        depts = ["Sales", "Marketing", "HR", "Engineering", "Support"]
        for i in range(1, 101):
            emp_data.append((f"Employee {i}", "Staff", random.choice(depts), datetime.now() - timedelta(days=random.randint(0, 1000))))
        cursor.executemany("INSERT INTO employees (name, position, department, start_date) VALUES (%s, %s, %s, %s)", emp_data)

        sal_data = []
        for i in range(1, 101):
            sal_data.append((i, random.uniform(1000, 5000), random.uniform(0, 500), datetime.now()))
        cursor.executemany("INSERT INTO salaries (employee_id, base_salary, bonus, pay_date) VALUES (%s, %s, %s, %s)", sal_data)

        conn.commit()
        logging.info("✅ Database Setup Complete: 3 DBs created, 20+ users created, 2000+ rows inserted.")

    except mysql.connector.Error as err:
        logging.error(f"Setup Failed: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    setup_database()