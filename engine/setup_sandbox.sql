-- ==============================================================================
-- SCRIPT SETUP SANDBOX DATABASE CHO DỰ ÁN UBA
-- ==============================================================================

-- Xóa DB cũ nếu tồn tại để làm sạch môi trường
DROP DATABASE IF EXISTS sales_db;
DROP DATABASE IF EXISTS hr_db;
DROP DATABASE IF EXISTS admin_db;

-- Tạo các database mới
CREATE DATABASE sales_db;
CREATE DATABASE hr_db;
CREATE DATABASE admin_db;

-- ==============================================================================
-- CẤU TRÚC BẢNG VÀ DỮ LIỆU MẪU
-- ==============================================================================

-- --- Bảng cho SALES_DB ---
USE sales_db;

CREATE TABLE customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    registration_date DATE
);

CREATE TABLE products (
    sku VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2)
);

CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    order_date DATETIME,
    status VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
    item_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    product_sku VARCHAR(20),
    quantity INT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_sku) REFERENCES products(sku)
);

CREATE TABLE marketing_campaigns (
    campaign_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    start_date DATE,
    end_date DATE
);

-- Thêm dữ liệu mẫu cho sales_db
INSERT INTO customers (name, email, city, registration_date) VALUES
('Nguyen Van A', 'a.nguyen@example.com', 'Hanoi', '2024-01-15'),
('Tran Thi B', 'b.tran@example.com', 'HCMC', '2024-02-20');

INSERT INTO products (sku, name, category, price) VALUES
('PROD001', 'Laptop Pro', 'Electronics', 2500.00),
('BOOK001', 'Data Science Intro', 'Books', 55.00);

INSERT INTO orders (customer_id, order_date, status) VALUES
(1, NOW(), 'Pending'),
(2, NOW(), 'Shipped');

INSERT INTO order_items (order_id, product_sku, quantity) VALUES
(1, 'PROD001', 1),
(2, 'BOOK001', 2);


-- --- Bảng cho HR_DB ---
USE hr_db;

CREATE TABLE employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    position VARCHAR(50),
    start_date DATE
);

CREATE TABLE salaries (
    salary_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id INT,
    base_salary DECIMAL(10, 2),
    bonus DECIMAL(10, 2),
    pay_date DATE,
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

-- Thêm dữ liệu mẫu cho hr_db
INSERT INTO employees (name, position, start_date) VALUES
('Le Van C', 'Developer', '2023-05-10'),
('Pham Thi D', 'Sales Manager', '2022-08-01');

INSERT INTO salaries (employee_id, base_salary, bonus, pay_date) VALUES
(1, 60000.00, 5000.00, '2025-10-30'),
(2, 75000.00, 10000.00, '2025-10-30');


-- --- Bảng cho ADMIN_DB ---
USE admin_db;

CREATE TABLE system_logs (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    service_name VARCHAR(50),
    log_level VARCHAR(10),
    message TEXT
);

-- ==============================================================================
-- TẠO NGƯỜI DÙNG VÀ CẤP QUYỀN
-- ==============================================================================

-- Xóa user cũ nếu tồn tại
DROP USER IF EXISTS 'anh_sales'@'%'; DROP USER IF EXISTS 'linh_sales'@'%'; DROP USER IF EXISTS 'quang_sales'@'%'; DROP USER IF EXISTS 'trang_sales'@'%';
DROP USER IF EXISTS 'binh_mkt'@'%'; DROP USER IF EXISTS 'mai_mkt'@'%'; DROP USER IF EXISTS 'vy_mkt'@'%';
DROP USER IF EXISTS 'chi_hr'@'%'; DROP USER IF EXISTS 'hoa_hr'@'%';
DROP USER IF EXISTS 'dung_support'@'%'; DROP USER IF EXISTS 'loan_support'@'%'; DROP USER IF EXISTS 'khang_support'@'%';
DROP USER IF EXISTS 'em_dev'@'%'; DROP USER IF EXISTS 'tam_dev'@'%'; DROP USER IF EXISTS 'ly_data'@'%'; DROP USER IF EXISTS 'quoc_app'@'%'; DROP USER IF EXISTS 'dave_dev'@'%';
DROP USER IF EXISTS 'thanh_admin'@'%';
DROP USER IF EXISTS 'uba_user'@'%'; -- User cho publisher


-- --- Tạo user với mật khẩu 'password' ---
-- Sales Team
CREATE USER 'anh_sales'@'%' IDENTIFIED BY 'password';
CREATE USER 'linh_sales'@'%' IDENTIFIED BY 'password';
CREATE USER 'quang_sales'@'%' IDENTIFIED BY 'password';
CREATE USER 'trang_sales'@'%' IDENTIFIED BY 'password';
-- Marketing Team
CREATE USER 'binh_mkt'@'%' IDENTIFIED BY 'password';
CREATE USER 'mai_mkt'@'%' IDENTIFIED BY 'password';
CREATE USER 'vy_mkt'@'%' IDENTIFIED BY 'password';
-- HR Team
CREATE USER 'chi_hr'@'%' IDENTIFIED BY 'password';
CREATE USER 'hoa_hr'@'%' IDENTIFIED BY 'password';
-- Support Team
CREATE USER 'dung_support'@'%' IDENTIFIED BY 'password';
CREATE USER 'loan_support'@'%' IDENTIFIED BY 'password';
CREATE USER 'khang_support'@'%' IDENTIFIED BY 'password';
-- Engineering/Dev Team
CREATE USER 'em_dev'@'%' IDENTIFIED BY 'password';
CREATE USER 'tam_dev'@'%' IDENTIFIED BY 'password';
CREATE USER 'ly_data'@'%' IDENTIFIED BY 'password';
CREATE USER 'quoc_app'@'%' IDENTIFIED BY 'password';
CREATE USER 'dave_dev'@'%' IDENTIFIED BY 'password';
-- IT Admin
CREATE USER 'thanh_admin'@'%' IDENTIFIED BY 'password';

-- User cho publisher (cần quyền SELECT trên performance_schema)
CREATE USER 'uba_user'@'%' IDENTIFIED BY 'password';


-- --- Cấp quyền (GRANT) ---
-- Sales, Marketing, Support, Dev có quyền trên sales_db
GRANT SELECT, INSERT, UPDATE, DELETE ON sales_db.* TO 'anh_sales'@'%', 'linh_sales'@'%', 'quang_sales'@'%', 'trang_sales'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON sales_db.* TO 'binh_mkt'@'%', 'mai_mkt'@'%', 'vy_mkt'@'%';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'dung_support'@'%', 'loan_support'@'%', 'khang_support'@'%';
GRANT ALL PRIVILEGES ON sales_db.* TO 'em_dev'@'%', 'tam_dev'@'%', 'ly_data'@'%', 'quoc_app'@'%', 'dave_dev'@'%';

-- HR có quyền trên hr_db
GRANT SELECT, INSERT, UPDATE, DELETE ON hr_db.* TO 'chi_hr'@'%', 'hoa_hr'@'%';

-- Một số user tò mò có thể xem (SELECT) bảng employees
GRANT SELECT ON hr_db.employees TO 'dave_dev'@'%';
GRANT SELECT ON hr_db.employees TO 'anh_sales'@'%';


-- IT Admin có quyền gần như root
GRANT ALL PRIVILEGES ON *.* TO 'thanh_admin'@'%' WITH GRANT OPTION;

-- Cấp quyền cho Publisher
GRANT SELECT ON performance_schema.* TO 'uba_user'@'%';
GRANT PROCESS ON *.* TO 'uba_user'@'%'; -- Cần quyền này để đọc bảng threads

FLUSH PRIVILEGES;

SELECT '*** SETUP HOÀN TẤT ***' AS message;