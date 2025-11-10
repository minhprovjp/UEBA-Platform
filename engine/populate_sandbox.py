# populate_sandbox.py
import mysql.connector
import os
import logging
from datetime import datetime, timedelta
import random

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [Populator] - %(message)s")
log = logging.getLogger("Populator")

# (Lấy thông tin kết nối từ .env)
SANDBOX_HOST = os.getenv("SANDBOX_DB_HOST", "localhost")
SANDBOX_PORT = os.getenv("SANDBOX_DB_PORT", "3306")
# (Sử dụng user 'root' hoặc user admin của Sandbox để có quyền INSERT)
DB_USER = "root" 
DB_PASS = os.getenv("MYSQL_ROOT_PASSWORD", "root") # (Thay bằng pass root của bạn)

def populate_data():
    try:
        conn = mysql.connector.connect(
            host=SANDBOX_HOST,
            port=SANDBOX_PORT,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()
        log.info("Kết nối MySQL (Sandbox) thành công.")
    except mysql.connector.Error as err:
        log.error(f"Lỗi kết nối MySQL: {err}")
        return

    try:
        # 1. Bơm dữ liệu vào HR
        cursor.execute("USE hr_db")
        log.info("Bơm dữ liệu 'employees' và 'salaries'...")
        emp_data = []
        sal_data = []
        for i in range(1, 101): # Tạo 100 nhân viên
            emp_data.append((i, f"Employee {i}", "Sales" if i % 2 == 0 else "Dev"))
            sal_data.append((i, i, random.randint(50000, 100000)))
            
        cursor.executemany("INSERT INTO employees (id, name, position) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE name=name", emp_data)
        cursor.executemany("INSERT INTO salaries (id, employee_id, salary) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE salary=salary", sal_data)
        
        # 2. Bơm dữ liệu vào Sales
        cursor.execute("USE sales_db")
        log.info("Bơm dữ liệu 'customers', 'products', và 'orders'...")
        cust_data = []
        for i in range(1, 501): # 500 khách hàng
            cust_data.append((i, f"Customer {i}", f"cust{i}@example.com"))
        cursor.executemany("INSERT INTO customers (id, name, email) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE name=name", cust_data)

        prod_data = []
        for i in range(1, 101): # 100 sản phẩm
            prod_data.append((f"SKU{i:03d}", f"Product {i}"))
        cursor.executemany("INSERT INTO products (sku, name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name=name", prod_data)

        order_data = []
        for i in range(1, 10001): # 10,000 đơn hàng (để test phá hoại)
             order_data.append((i, random.randint(1, 500), "Completed"))
        cursor.executemany("INSERT INTO orders (id, customer_id, status) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE status=status", order_data)

        conn.commit()
        log.info(f"✅ Hoàn tất: Đã bơm {len(order_data)} đơn hàng và {len(emp_data)} nhân viên vào Sandbox.")

    except mysql.connector.Error as err:
        log.error(f"Lỗi khi bơm dữ liệu: {err}")
    finally:
        if cursor and conn:
            try:
                log.warning("Đang dọn dẹp (TRUNCATE) log 'nhiễu' từ Performance Schema...")
                
                # 1. Xóa lịch sử câu lệnh
                cursor.execute("TRUNCATE TABLE performance_schema.events_statements_history")
                
                # 2. Xóa lịch sử câu lệnh (dài)
                cursor.execute("TRUNCATE TABLE performance_schema.events_statements_history_long")
                
                # (Tùy chọn: Xóa cả General Log Table nếu bạn bật nó)
                cursor.execute("TRUNCATE TABLE mysql.general_log")
                
                conn.commit()
                log.info("✅ Dọn dẹp log 'nhiễu' thành công. Sandbox đã sẵn sàng.")
            except mysql.connector.Error as err:
                log.error(f"Lỗi khi dọn dẹp log: {err}. (User của bạn có thể thiếu quyền TRUNCATE).")
                
        # Đóng kết nối
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    populate_data()