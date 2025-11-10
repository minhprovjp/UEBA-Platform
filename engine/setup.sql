-- =====================================================================
-- SCRIPT CÀI ĐẶT MÔI TRƯỜNG TOÀN DIỆN CHO PROJECT UBA
-- BAO GỒM: PostgreSQL (Đích) và MySQL (Nguồn/Sandbox)
-- =====================================================================


-- =====================================================================
-- PHẦN A: CÀI ĐẶT POSTGRESQL (CSDL LƯU TRỮ KẾT QUẢ)
-- Chạy các lệnh này bằng user 'postgres' (hoặc superuser)
-- =====================================================================

-- 1. Tạo User và CSDL cho ứng dụng
CREATE USER uba_user WITH PASSWORD 'password'; -- (Thay 'password' bằng mật khẩu của bạn)
CREATE DATABASE uba_db;

-- 2. Cấp quyền cho user trên CSDL đó
GRANT ALL PRIVILEGES ON DATABASE uba_db TO uba_user;

-- 3. CHUYỂN KẾT NỐI sang CSDL uba_db TRƯỚC KHI CHẠY CÁC LỆNH TIẾP THEO
-- Trong psql, gõ: \c uba_db
-- 4. Cấp quyền cho user trên schema 'public' (rất quan trọng)
GRANT ALL PRIVILEGES ON SCHEMA public TO uba_user;


-- 5. Tạo các bảng (Đây là các lệnh mà 'init_db.py' sẽ chạy)
-- (Lưu ý: Bạn vẫn nên chạy 'init_db.py' vì nó an toàn hơn,
-- nhưng đây là các lệnh SQL thô nếu bạn muốn tự chạy)

-- Xóa các bảng cũ trước nếu chúng tồn tại (dọn dẹp)
DROP TABLE IF EXISTS all_logs CASCADE;
DROP TABLE IF EXISTS anomalies CASCADE;

-- Tạo bảng Anomalies (Bất thường)
CREATE TABLE anomalies (
    id SERIAL NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "user" VARCHAR,
    client_ip VARCHAR,
    database VARCHAR,
    query TEXT NOT NULL,
    anomaly_type VARCHAR NOT NULL,
    score FLOAT,
    reason VARCHAR,
    status VARCHAR,
    execution_time_ms FLOAT DEFAULT 0,
    rows_returned BIGINT DEFAULT 0,
    rows_affected BIGINT DEFAULT 0,
    PRIMARY KEY (id)
);

-- Tạo bảng AllLogs (Tất cả log)
CREATE TABLE all_logs (
    id SERIAL NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    "user" VARCHAR,
    client_ip VARCHAR,
    database VARCHAR,
    query TEXT NOT NULL,
    is_anomaly BOOLEAN DEFAULT false,
    analysis_type VARCHAR,
    execution_time_ms FLOAT DEFAULT 0,
    rows_returned BIGINT DEFAULT 0,
    rows_affected BIGINT DEFAULT 0,
    PRIMARY KEY (id)
);

-- Tạo các chỉ mục (Indexes) để tăng tốc độ truy vấn
CREATE INDEX IF NOT EXISTS ix_anomalies_timestamp ON anomalies (timestamp);
CREATE INDEX IF NOT EXISTS ix_anomalies_user ON anomalies ("user");
CREATE INDEX IF NOT EXISTS ix_anomalies_anomaly_type ON anomalies (anomaly_type);
CREATE INDEX IF NOT EXISTS ix_anomalies_status ON anomalies (status);

CREATE INDEX IF NOT EXISTS ix_all_logs_timestamp ON all_logs (timestamp);
CREATE INDEX IF NOT EXISTS ix_all_logs_user ON all_logs ("user");

-- Cấp quyền sở hữu các bảng này cho 'uba_user' (nếu bạn đang chạy với 'postgres')
ALTER TABLE anomalies OWNER TO uba_user;
ALTER TABLE all_logs OWNER TO uba_user;


-- =====================================================================
-- PHẦN B: CÀI ĐẶT MYSQL (CSDL NGUỒN LOG & SANDBOX)
-- Chạy các lệnh này bằng user 'root' của MySQL
-- =====================================================================

-- 1. Tạo CSDL "Sandbox" (nơi simulator chạy truy vấn)
CREATE DATABASE IF NOT EXISTS sales_db;
CREATE DATABASE IF NOT EXISTS hr_db;

-- 2. Tạo các bảng rỗng (schema) cho Sandbox
USE sales_db;
CREATE TABLE IF NOT EXISTS customers (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100));
CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY, customer_id INT, status VARCHAR(50));
CREATE TABLE IF NOT EXISTS products (sku VARCHAR(50) PRIMARY KEY, name VARCHAR(100));
CREATE TABLE IF NOT EXISTS order_items (id INT PRIMARY KEY, order_id INT, product_sku VARCHAR(50));

USE hr_db;
CREATE TABLE IF NOT EXISTS employees (id INT PRIMARY KEY, name VARCHAR(100), position VARCHAR(100));
CREATE TABLE IF NOT EXISTS salaries (id INT PRIMARY KEY, employee_id INT, salary FLOAT);

-- 3. Tạo các User "Persona" (cho Simulator)
CREATE USER IF NOT EXISTS 'anh_sales'@'localhost' IDENTIFIED BY 'password123';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'anh_sales'@'localhost';

CREATE USER IF NOT EXISTS 'chi_hr'@'localhost' IDENTIFIED BY 'password100';
GRANT SELECT, INSERT ON hr_db.* TO 'chi_hr'@'localhost';

CREATE USER IF NOT EXISTS 'dave_dev'@'localhost' IDENTIFIED BY 'password789';
GRANT SELECT, UPDATE ON sales_db.* TO 'dave_dev'@'localhost';

-- 4. Tạo User "Log Reader" (cho Publisher)
CREATE USER IF NOT EXISTS 'uba_user'@'localhost' IDENTIFIED BY 'password'; -- (Thay 'password' cho đúng)

-- 5. Cấp quyền cho "Log Reader" (Cấp cho cả 2 phương án)

-- Quyền cho Performance Schema (Khuyến nghị)
GRANT SELECT ON performance_schema.events_statements_history TO 'uba_user'@'localhost';
GRANT SELECT ON performance_schema.threads TO 'uba_user'@'localhost';

-- Quyền cho General Log Table (Dự phòng)
GRANT SELECT ON mysql.general_log TO 'uba_user'@'localhost';

-- 6. Áp dụng tất cả quyền
FLUSH PRIVILEGES;


-- =====================================================================
-- PHẦN C: CÁC LỆNH TIỆN ÍCH BẬT/TẮT VÀ XÓA LOG (MYSQL)
-- Chạy các lệnh này bằng user 'root' của MySQL khi cần
-- =====================================================================

-- ---------------------------------
-- 1. BẬT LOGS (Chọn 1 trong 2)
-- ---------------------------------

-- LỰA CHỌN A: Bật PERFORMANCE SCHEMA (Khuyến nghị)
-- (Chỉ cần chạy 1 lần duy nhất)
UPDATE performance_schema.setup_instruments 
SET ENABLED = 'YES', TIMED = 'YES' 
WHERE NAME LIKE 'statement/%';

UPDATE performance_schema.setup_consumers 
SET ENABLED = 'YES' 
WHERE NAME LIKE '%events_statements_history%';


-- LỰA CHỌN B: Bật GENERAL LOG (FILE)
SET GLOBAL general_log = 'ON';
SET GLOBAL log_output = 'FILE';


-- LỰA CHỌN C: Bật GENERAL LOG (TABLE)
SET GLOBAL general_log = 'ON';
SET GLOBAL log_output = 'TABLE';


-- ---------------------------------
-- 2. TẮT LOGS
-- ---------------------------------

-- Tắt General Log (an toàn để chạy)
SET GLOBAL general_log = 'OFF';

-- Tắt Performance Schema (Không khuyến nghị trừ khi gỡ lỗi)
-- UPDATE performance_schema.setup_instruments SET ENABLED = 'NO' WHERE NAME LIKE 'statement/%';
-- UPDATE performance_schema.setup_consumers SET ENABLED = 'NO' WHERE NAME LIKE '%events_statements_history%';


-- ---------------------------------
-- 3. XÓA (CLEAR) LOGS
-- ---------------------------------

-- Xóa log trong BẢNG General Log
TRUNCATE TABLE mysql.general_log;

-- Xóa log trong Performance Schema
TRUNCATE TABLE performance_schema.events_statements_history;
TRUNCATE TABLE performance_schema.events_statements_history_long;