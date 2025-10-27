USE company_db;

-- Xoá user cũ nếu đang test lại
DROP USER IF EXISTS 'anh_sales'@'%';
DROP USER IF EXISTS 'binh_mkt'@'%';
DROP USER IF EXISTS 'chi_hr'@'%';
DROP USER IF EXISTS 'dung_support'@'%';
DROP USER IF EXISTS 'em_dev'@'%';
DROP USER IF EXISTS 'dave_dev'@'%';
DROP USER IF EXISTS 'thanh_admin'@'%';

-- Tạo user
CREATE USER 'anh_sales'@'%' IDENTIFIED BY 'password';
CREATE USER 'binh_mkt'@'%' IDENTIFIED BY 'password';
CREATE USER 'chi_hr'@'%' IDENTIFIED BY 'password';
CREATE USER 'dung_support'@'%' IDENTIFIED BY 'password';
CREATE USER 'em_dev'@'%' IDENTIFIED BY 'password';
CREATE USER 'dave_dev'@'%' IDENTIFIED BY 'password';
CREATE USER 'thanh_admin'@'%' IDENTIFIED BY 'password';

-- SALES (chỉ làm việc với khách hàng, đơn hàng)
GRANT SELECT (customer_id, name, email, registration_date, city)
    ON company_db.customers TO 'anh_sales'@'%';
GRANT SELECT ON company_db.products TO 'anh_sales'@'%';
GRANT SELECT, INSERT, UPDATE
    ON company_db.orders TO 'anh_sales'@'%';
GRANT SELECT, INSERT, UPDATE
    ON company_db.order_items TO 'anh_sales'@'%';

-- MARKETING (chỉ xem phân khúc KH, không nên thấy full bảng gốc)
GRANT SELECT ON company_db.marketing_customer_view TO 'binh_mkt'@'%';
GRANT SELECT ON company_db.marketing_campaigns TO 'binh_mkt'@'%';

-- HR (nhân sự: xem & chỉnh employee, xem lương)
GRANT SELECT, INSERT, UPDATE
    ON company_db.employees TO 'chi_hr'@'%';
GRANT SELECT
    ON company_db.salaries TO 'chi_hr'@'%';

-- SUPPORT (hỗ trợ khách hàng: xem orders, customer info để trả lời ticket)
GRANT SELECT
    ON company_db.orders TO 'dung_support'@'%';
GRANT SELECT
    ON company_db.order_items TO 'dung_support'@'%';
GRANT SELECT (customer_id, name, email, phone, city)
    ON company_db.customers TO 'dung_support'@'%';
GRANT UPDATE (status)
    ON company_db.orders TO 'dung_support'@'%';

-- DEVELOPER (dev backend: xem orders để debug, xem products, đôi lúc xem khách hàng;
--            KHÔNG được xem bảng lương, KHÔNG được tạo user)
GRANT SELECT
    ON company_db.orders TO 'em_dev'@'%';
GRANT SELECT
    ON company_db.order_items TO 'em_dev'@'%';
GRANT SELECT
    ON company_db.products TO 'em_dev'@'%';
GRANT SELECT (customer_id, name, email, city)
    ON company_db.customers TO 'em_dev'@'%';
GRANT UPDATE (status)
    ON company_db.orders TO 'em_dev'@'%';

-- DAVE_DEV (giống dev thường, để giả lập insider)
GRANT SELECT
    ON company_db.orders TO 'dave_dev'@'%';
GRANT SELECT
    ON company_db.order_items TO 'dave_dev'@'%';
GRANT SELECT
    ON company_db.products TO 'dave_dev'@'%';
GRANT SELECT (customer_id, name, email, city)
    ON company_db.customers TO 'dave_dev'@'%';
GRANT UPDATE (status)
    ON company_db.orders TO 'dave_dev'@'%';

-- IT ADMIN (quản trị hệ thống DB - quyền nguy hiểm)
-- Quyền rộng trong company_db
GRANT ALL PRIVILEGES ON company_db.* TO 'thanh_admin'@'%' WITH GRANT OPTION;

-- Quyền cấp hệ thống/global để leo thang, tạo user khác, xem processlist
GRANT CREATE USER, PROCESS, SHOW DATABASES, SUPER, RELOAD
    ON *.* TO 'thanh_admin'@'%' WITH GRANT OPTION;

-- Cập nhật
FLUSH PRIVILEGES;
