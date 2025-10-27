-- 1. Tạo database và dùng nó
CREATE DATABASE IF NOT EXISTS company_db;
USE company_db;

-- 2. Bảng khách hàng
CREATE TABLE customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    city VARCHAR(50),
    registration_date DATE
);

-- 3. Bảng sản phẩm
CREATE TABLE products (
    sku VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2)
);

-- 4. Bảng đơn hàng
CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    order_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- 5. Bảng chi tiết đơn hàng
CREATE TABLE order_items (
    item_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    product_sku VARCHAR(50),
    quantity INT,
    item_price DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_sku) REFERENCES products(sku)
);

-- 6. Bảng nhân viên
CREATE TABLE employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    position VARCHAR(100),
    dept VARCHAR(50),
    start_date DATE
);

-- 7. Bảng lương (nhạy cảm)
CREATE TABLE salaries (
    employee_id INT PRIMARY KEY,
    base_salary DECIMAL(10,2),
    bonus DECIMAL(10,2),
    bank_account VARCHAR(34),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

-- 8. Log hệ thống (IT/DevOps hay xem)
CREATE TABLE system_logs (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    ts DATETIME,
    level ENUM('INFO','WARN','ERROR'),
    message TEXT
);

-- 9. Chiến dịch marketing
CREATE TABLE marketing_campaigns (
    campaign_id INT AUTO_INCREMENT PRIMARY KEY,
    campaign_name VARCHAR(100),
    channel ENUM('Email','Social','Ads','Event'),
    budget DECIMAL(12,2),
    start_date DATE,
    end_date DATE
);

-- 10. View an toàn cho Marketing (không cho full salaries / bank / v.v.)
-- Marketing chỉ nên thấy thông tin khách hàng cơ bản để phân khúc,
-- KHÔNG full dump mọi thứ riêng tư nhạy cảm nội bộ.
CREATE OR REPLACE VIEW marketing_customer_view AS
SELECT
    customer_id,
    name,
    email,
    city,
    registration_date
FROM customers;

-- 11. Dữ liệu giả (ít thôi để chạy thử)
INSERT INTO customers (name, email, phone, city, registration_date) VALUES
('Alice Nguyen', 'alice@demo.example', '0905123456', 'Da Nang', '2024-01-15'),
('Bob Tran', 'bob@demo.example', '0912345678', 'Hue', '2024-02-20'),
('Charlie Le', 'charlie@demo.example', '0988777666', 'Quang Nam', '2024-03-10'),
('Daisy Pham', 'daisy@demo.example', '0933111222', 'Da Nang', '2024-05-01'),
('Ethan Vo', 'ethan@demo.example', '0944556677', 'Hoi An', '2024-06-12');

INSERT INTO products (sku, product_name, category, price) VALUES
('PROD001', 'Laptop Pro X', 'Electronics', 1200.00),
('PROD002', 'Wireless Mouse G', 'Accessories', 25.50),
('PROD003', 'Mechanical Keyboard K', 'Accessories', 75.00),
('BOOK001', 'The Art of SQL', 'Books', 30.00),
('BOOK002', 'Clean Architecture', 'Books', 42.00);

INSERT INTO employees (name, position, dept, start_date) VALUES
('Anh Sales', 'Account Executive', 'Sales', '2023-09-10'),
('Binh Marketing', 'Growth Specialist', 'Marketing', '2022-11-01'),
('Chi HR', 'HR Manager', 'HR', '2021-03-15'),
('Dung Support', 'Customer Support', 'Support', '2024-04-01'),
('Em Dev', 'Backend Developer', 'Engineering', '2022-06-20'),
('Thanh Admin', 'IT Admin', 'IT', '2020-01-05'),
('Dave Dev', 'Backend Developer', 'Engineering', '2022-12-01');

INSERT INTO salaries (employee_id, base_salary, bonus, bank_account) VALUES
(1, 1500.00, 200.00, 'VCB-123-xxx'),
(2, 1400.00, 150.00, 'ACB-456-xxx'),
(3, 2000.00, 400.00, 'TCB-789-xxx'),
(4, 1300.00, 100.00, 'VCB-321-xxx'),
(5, 2500.00, 800.00, 'ACB-654-xxx'),
(6, 3000.00, 1000.00, 'TCB-987-xxx'),
(7, 2600.00, 500.00, 'VCB-777-xxx');

INSERT INTO orders (customer_id, status) VALUES
(1, 'Pending'),
(2, 'Shipped'),
(1, 'Delivered'),
(3, 'Pending'),
(5, 'Pending');

INSERT INTO order_items (order_id, product_sku, quantity, item_price) VALUES
(1, 'PROD001', 1, 1200.00),
(1, 'PROD002', 2, 25.50),
(2, 'BOOK001', 1, 30.00),
(3, 'PROD003', 1, 75.00),
(4, 'PROD001', 1, 1200.00),
(5, 'BOOK002', 1, 42.00);

INSERT INTO marketing_campaigns (campaign_name, channel, budget, start_date, end_date) VALUES
('Fall Promo', 'Email', 5000.00, '2025-10-01', '2025-10-15'),
('11.11 Sale', 'Ads', 12000.00, '2025-10-20', '2025-11-12');

INSERT INTO system_logs (ts, level, message) VALUES
(NOW(), 'INFO', 'Daily ETL job complete'),
(NOW(), 'WARN', 'High response time on checkout API'),
(NOW(), 'ERROR', 'Payment gateway timeout for order_id=42');

-- Done bootstrap
