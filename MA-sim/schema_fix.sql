-- Schema Fix Script for Enhanced Vietnamese Enterprise Simulation
-- Addresses missing tables and schema mismatches

-- Ensure all databases exist
CREATE DATABASE IF NOT EXISTS sales_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS inventory_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS finance_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS marketing_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS support_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS hr_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS admin_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Fix HR database - customers table should NOT exist here
-- HR database should have employee-related tables only
USE hr_db;
CREATE TABLE IF NOT EXISTS employees (
    employee_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    position VARCHAR(50),
    dept_id INT,
    hire_date DATE,
    salary DECIMAL(12,2),
    status ENUM('active', 'inactive') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS departments (
    dept_id INT PRIMARY KEY AUTO_INCREMENT,
    dept_name VARCHAR(50) NOT NULL,
    manager_id INT,
    budget DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS salaries (
    salary_id INT PRIMARY KEY AUTO_INCREMENT,
    employee_id INT,
    amount DECIMAL(12,2),
    bonus DECIMAL(12,2) DEFAULT 0,
    payment_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

CREATE TABLE IF NOT EXISTS attendance (
    attendance_id INT PRIMARY KEY AUTO_INCREMENT,
    employee_id INT,
    date DATE,
    status ENUM('present', 'absent', 'late', 'sick') DEFAULT 'present',
    check_in_time TIME,
    check_out_time TIME,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

-- Fix Admin database - customers table should NOT exist here
-- Admin database should have system administration tables
USE admin_db;
CREATE TABLE IF NOT EXISTS system_logs (
    log_id INT PRIMARY KEY AUTO_INCREMENT,
    log_level ENUM('info', 'warning', 'error', 'debug') DEFAULT 'info',
    module VARCHAR(50),
    message TEXT,
    user_id VARCHAR(50),
    ip_address VARCHAR(45),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_sessions (
    session_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id VARCHAR(50),
    login_time TIMESTAMP,
    last_activity TIMESTAMP,
    ip_address VARCHAR(45),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS report_schedules (
    schedule_id INT PRIMARY KEY AUTO_INCREMENT,
    report_name VARCHAR(100),
    schedule_type ENUM('daily', 'weekly', 'monthly') DEFAULT 'daily',
    last_run TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ensure customers table exists ONLY in sales_db
USE sales_db;
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_code VARCHAR(20) UNIQUE,
    company_name VARCHAR(200) NOT NULL,
    contact_person VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    city VARCHAR(50),
    customer_type ENUM('individual', 'business') DEFAULT 'business',
    status ENUM('active', 'inactive') DEFAULT 'active',
    credit_limit DECIMAL(15,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data if tables are empty
INSERT IGNORE INTO sales_db.customers (customer_code, company_name, contact_person, city, customer_type) VALUES
('KH000001', 'Công ty TNHH ABC', 'Nguyễn Văn A', 'Hà Nội', 'business'),
('KH000002', 'Công ty CP XYZ', 'Trần Thị B', 'Hồ Chí Minh', 'business'),
('KH000003', 'Doanh nghiệp DEF', 'Lê Văn C', 'Đà Nẵng', 'business');

INSERT IGNORE INTO hr_db.departments (dept_name, budget) VALUES
('Phòng Kinh Doanh', 5000000000),
('Phòng Marketing', 3000000000),
('Phòng Nhân Sự', 2000000000),
('Phòng Tài Chính', 2500000000),
('Phòng IT', 4000000000);

INSERT IGNORE INTO hr_db.employees (name, email, position, dept_id, salary) VALUES
('Nguyễn Văn Nam', 'nguyen_van_nam@company.vn', 'Nhân viên kinh doanh', 1, 15000000),
('Trần Thị Lan', 'tran_thi_lan@company.vn', 'Chuyên viên marketing', 2, 12000000),
('Lê Minh Đức', 'le_minh_duc@company.vn', 'Lập trình viên', 5, 18000000);

-- Grant proper permissions
GRANT SELECT ON sales_db.* TO 'nguyen_van_nam'@'%';
GRANT SELECT ON hr_db.* TO 'le_thanh_khang'@'%';
GRANT SELECT ON admin_db.* TO 'duong_van_phong'@'%';

FLUSH PRIVILEGES;
