-- =====================================================================
-- SCRIPT SỬA LỖI TOÀN DIỆN CHO SANDBOX MYSQL
-- 1. Tạo tất cả user (nếu chưa tồn tại).
-- 2. Đặt lại mật khẩu của họ về một giá trị chuẩn.
-- 3. Cấp quyền (GRANT) chính xác cho từng vai trò.
-- =====================================================================

-- ---
-- BƯỚC 1: TẠO VÀ ĐẶT LẠI MẬT KHẨU USER
-- Chúng ta sẽ đặt TẤT CẢ mật khẩu thành 'password'
-- Bạn PHẢI cập nhật file .env của mình để khớp với mật khẩu này.
-- ---
CREATE USER IF NOT EXISTS 'anh_sales'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'linh_sales'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'quang_sales'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'trang_sales'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'binh_mkt'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'mai_mkt'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'vy_mkt'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'chi_hr'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'hoa_hr'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'dung_support'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'loan_support'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'khang_support'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'em_dev'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'tam_dev'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'ly_data'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'quoc_app'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'dave_dev'@'localhost' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'thanh_admin'@'localhost' IDENTIFIED BY 'password';
-- DROP USER 'loan_support'@'localhost';
-- DROP USER 'tam_dev'@'localhost';
-- DROP USER 'ly_data'@'localhost';
-- DROP USER 'quoc_app'@'localhost';
-- DROP USER 'loan_support'@'localhost';
-- DROP USER 'khang_support'@'localhost';
-- ---
-- BƯỚC 2: CẤP QUYỀN (SỬA LỖI 1044)
-- ---

-- Quyền cho vai trò Sales, Marketing, Support, Dev (truy cập 'sales_db')
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'anh_sales'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'linh_sales'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'quang_sales'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'trang_sales'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'binh_mkt'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'mai_mkt'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'vy_mkt'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'dung_support'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'loan_support'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'khang_support'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'em_dev'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'tam_dev'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'ly_data'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'quoc_app'@'localhost';
GRANT SELECT, INSERT, UPDATE ON sales_db.* TO 'dave_dev'@'localhost';

-- Quyền cho vai trò HR (truy cập 'hr_db')
GRANT SELECT, INSERT, UPDATE ON hr_db.* TO 'chi_hr'@'localhost';
GRANT SELECT, INSERT, UPDATE ON hr_db.* TO 'hoa_hr'@'localhost';

-- Quyền cho IT Admin (truy cập 'mysql' và mọi thứ)
-- (Log [cite: 279-281] cho thấy 'it_thanh' cần truy cập 'mysql', nhưng vai trò Admin cũng cần quyền cao hơn để mô phỏng)
GRANT ALL PRIVILEGES ON *.* TO 'thanh_admin'@'localhost' WITH GRANT OPTION;
GRANT SELECT, INSERT, UPDATE ON mysql.* TO 'thanh_admin'@'localhost';

-- ---
-- BƯỚC 3: ÁP DỤNG THAY ĐỔI
-- ---
FLUSH PRIVILEGES;

SELECT user, host FROM mysql.user WHERE user LIKE '%uba%';
-- (Lệnh trên chỉ để xác nhận các user đã được tạo)