# generate_simulated_general_log.py
#
# Mục tiêu:
#   - Sinh file text có định dạng GẦN NHƯ Y CHANG general_log của MySQL
#   - Không cần chạy MySQL thật, không cần pymysql
#   - Có hành vi bình thường + bất thường (insider, attacker, dump dữ liệu)
#
# File output bắt đầu bằng header giống MySQL:
#   C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqld.exe, Version: 8.0.42 (MySQL Community Server - GPL). started with:
#   TCP Port: 3306, Named Pipe: MySQL
#   Time                 Id Command    Argument
#
# Và sau đó mỗi dòng:
#   2025-10-20T07:32:45.817751Z    10 Query    SELECT * FROM salaries
#
# Trong đó:
#   - "Id" = connection_id
#   - Một phiên = Connect -> nhiều Query/Init DB -> Quit
#
# Các pattern được tạo ra để test 5 rule bạn mô tả:
#   Rule 1: Hoạt động khuya (InsiderThreat & Attacker 2-5h sáng, cuối tuần)
#   Rule 2: Dump dữ liệu lớn (SELECT * FROM salaries; ... INTO OUTFILE ...)
#   Rule 3: Quét nhiều bảng trong thời gian ngắn (ITAdmin SELECT COUNT(*) FROM ... trên nhiều bảng)
#   Rule 4: Truy cập bảng nhạy cảm (salaries, mysql.user) ngoài giờ / IP lạ
#   Rule 5: Baseline giờ làm việc per-user (normal_hours, overtime_prob)
#
import random
from datetime import datetime, timedelta, time as dt_time

# ==============================================================================
# CONFIG (MULTI-DB + MULTI-USER TEAMS)
# ==============================================================================

OUTPUT_LOG_FILE = "simulated_general_log.txt"

# 3 logical DB trong công ty
DB_SALES = "sales_db"     # khách hàng, đơn hàng, sản phẩm, marketing
DB_HR = "hr_db"           # nhân sự, lương
DB_ADMIN = "admin_db"     # nhật ký hệ thống, vận hành, quyền

# Khoảng thời gian mô phỏng
SIMULATION_START_TIME = datetime(2025, 11, 6, 7, 0, 0)
SIMULATION_DURATION_DAYS = 30    # Mô phỏng bao nhiêu ngày
SESSIONS_PER_HOUR_BASE = 150     # Mật độ session trong 1 giờ

# Giờ làm việc chung công ty
WORK_START = dt_time(7, 0)
WORK_END = dt_time(17, 0)
OVERTIME_END = dt_time(20, 0)

# Nếu True -> KHÔNG sinh bất kỳ session nào ngoài giờ hành chính (7h-17h T2-T6)
# => sạch hoàn toàn, không có OT, không có cuối tuần
ALWAYS_WORK_HOURS_ONLY = False

# --- tấn công / compromised account control ---
compromise_tracker = {}
BASE_COMPROMISE_PROB = 0.02        # weekday 2-5h sáng => stealth
WEEKEND_COMPROMISE_PROB = 0.15     # weekend 2-5h sáng => loud
MAX_COMPROMISE_PER_HOUR = 2        # tránh spam quá nhiều compromised 1 giờ

ATTACK_DAY_PROB = 0.30             # Xác suất 1 ngày là "ngày attacker hoạt động"
attack_calendar = {}               # { "2025-10-20": True/False }

DAYTIME_COMPROMISE_PROB = 0.001
ALLOW_DAYTIME_ATTACK = False       # nếu muốn attacker chiếm account ngay ban ngày

ENABLE_COMPROMISED_ACCOUNT = False  # NEW: bật/tắt sinh phiên account bị chiếm
ENABLE_IDENTITY_THEFT_ONLY = False  # NEW: nếu True -> chỉ “mượn danh” (IP lạ) nhưng hành vi bình thường

# --- các kịch bản nâng cao, bạn bật/tắt tuỳ bài lab ---
ENABLE_SCENARIO_PRIVILEGE_ABUSE = False         # dev/sales tò mò xem bảng nhạy cảm (lương...)
ENABLE_SCENARIO_DATA_LEAKAGE = False            # marketing cố hút toàn bộ data KH ngoài giờ
ENABLE_SCENARIO_OT_IP_THEFT = False             # insider ăn cắp IP/data khi OT
ENABLE_SCENARIO_SABOTAGE = False                # insider phá hoại (DELETE / DROP) lúc đêm
ENABLE_SCENARIO_PRIV_ESCALATION = False         # tạo user backdoor, GRANT ALL, v.v.
ENABLE_SCENARIO_PRIVESC_DAY = False             # dev tạo user tạm trong giờ làm
ENABLE_INSIDER_BEHAVIOR = False                 # False -> dave_dev chỉ như dev bình thường kể cả tăng ca

# ==============================================================================
# TABLES THEO DB
# ==============================================================================

SALES_TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "marketing_campaigns",
]

HR_TABLES = [
    "employees",
    "salaries",
]

ADMIN_TABLES = [
    "system_logs",
]

# Admin truy cập đủ mọi nơi -> dùng tên database.table
GLOBAL_TABLES_QUAL = (
    [(DB_SALES, t) for t in SALES_TABLES] +
    [(DB_HR, t) for t in HR_TABLES] +
    [(DB_ADMIN, t) for t in ADMIN_TABLES]
)

# Bảng nhạy cảm: dùng để detect rule kiểu "truy cập dữ liệu bí mật"
SENSITIVE_TABLES = [
    f"{DB_HR}.employees",
    f"{DB_HR}.salaries",
    "mysql.user"
]

# ==============================================================================
# DANH SÁCH NHÂN VIÊN
# mỗi người: giờ làm khác nhau, OT_prob khác nhau, IP khác nhau, DB chính khác nhau
# ==============================================================================
EMPLOYEES = {
    # -------- Sales team (4 người) --------
    "sales_anh": {
        "username": "anh_sales",
        "persona": "Sales",
        "dept": "Sales",
        "ip": "192.168.1.10",
        "alt_ips": [],
        "normal_hours": (dt_time(8,30), dt_time(17,30)),
        "overtime_prob": 0.15,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",  # nếu account bị chiếm
        "primary_db": DB_SALES,
    },
    "sales_linh": {
        "username": "linh_sales",
        "persona": "Sales",
        "dept": "Sales",
        "ip": "192.168.1.21",
        "alt_ips": [],
        "normal_hours": (dt_time(9,0), dt_time(18,0)),
        "overtime_prob": 0.10,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },
    "sales_quang": {
        "username": "quang_sales",
        "persona": "Sales",
        "dept": "Sales",
        "ip": "192.168.1.22",
        "alt_ips": [],
        "normal_hours": (dt_time(8,0), dt_time(17,0)),
        "overtime_prob": 0.05,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },
    "sales_trang": {
        "username": "trang_sales",
        "persona": "Sales",
        "dept": "Sales",
        "ip": "192.168.1.23",
        "alt_ips": [],
        "normal_hours": (dt_time(10,0), dt_time(19,0)),
        "overtime_prob": 0.30,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },

    # -------- Marketing team (3 người) --------
    "mkt_binh": {
        "username": "binh_mkt",
        "persona": "Marketing",
        "dept": "Marketing",
        "ip": "192.168.1.11",
        "alt_ips": [],
        "normal_hours": (dt_time(9,0), dt_time(18,0)),
        "overtime_prob": 0.05,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },
    "mkt_mai": {
        "username": "mai_mkt",
        "persona": "Marketing",
        "dept": "Marketing",
        "ip": "192.168.1.24",
        "alt_ips": [],
        "normal_hours": (dt_time(10,0), dt_time(20,0)),
        "overtime_prob": 0.40,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },
    "mkt_vy": {
        "username": "vy_mkt",
        "persona": "Marketing",
        "dept": "Marketing",
        "ip": "192.168.1.25",
        "alt_ips": [],
        "normal_hours": (dt_time(8,0), dt_time(17,0)),
        "overtime_prob": 0.10,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },

    # -------- HR team (2 người) --------
    "hr_chi": {
        "username": "chi_hr",
        "persona": "HR",
        "dept": "HR",
        "ip": "192.168.1.12",
        "alt_ips": [],
        "normal_hours": (dt_time(8,0), dt_time(17,0)),
        "overtime_prob": 0.01,
        "can_manage_users": False,
        "can_view_salaries_normally": True,   # HR được phép xem lương
        "priv_compromise_profile": "sensitive",
        "primary_db": DB_HR,
    },
    "hr_hoa": {
        "username": "hoa_hr",
        "persona": "HR",
        "dept": "HR",
        "ip": "192.168.1.26",
        "alt_ips": [],
        "normal_hours": (dt_time(9,0), dt_time(19,0)),
        "overtime_prob": 0.30,
        "can_manage_users": False,
        "can_view_salaries_normally": True,
        "priv_compromise_profile": "sensitive",
        "primary_db": DB_HR,
    },

    # -------- Support team (3 người) --------
    "support_dung": {
        "username": "dung_support",
        "persona": "Support",
        "dept": "Support",
        "ip": "192.168.1.13",
        "alt_ips": [],
        "normal_hours": (dt_time(7,30), dt_time(19,0)),
        "overtime_prob": 0.20,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },
    "support_loan": {
        "username": "loan_support",
        "persona": "Support",
        "dept": "Support",
        "ip": "192.168.1.27",
        "alt_ips": [],
        "normal_hours": (dt_time(7,0), dt_time(15,0)),
        "overtime_prob": 0.05,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },
    "support_khang": {
        "username": "khang_support",
        "persona": "Support",
        "dept": "Support",
        "ip": "192.168.1.28",
        "alt_ips": ["100.71.22.9"],  # IP nhà / 4G
        "normal_hours": (dt_time(12,0), dt_time(20,0)),
        "overtime_prob": 0.35,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },

    # -------- Engineering / Dev team (5 người) --------
    "dev_em": {
        "username": "em_dev",
        "persona": "Developer",
        "dept": "Engineering",
        "ip": "192.168.1.15",
        "alt_ips": [],
        "normal_hours": (dt_time(10,0), dt_time(20,0)),
        "overtime_prob": 0.40,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },
    "dev_tam": {
        "username": "tam_dev",
        "persona": "Developer",
        "dept": "Engineering",
        "ip": "192.168.1.29",
        "alt_ips": [],
        "normal_hours": (dt_time(11,0), dt_time(20,0)),
        "overtime_prob": 0.35,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },
    "dev_ly": {
        "username": "ly_data",
        "persona": "Developer",
        "dept": "Engineering",
        "ip": "192.168.1.30",
        "alt_ips": [],
        "normal_hours": (dt_time(9,30), dt_time(18,30)),
        "overtime_prob": 0.20,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },
    "dev_quoc": {
        "username": "quoc_app",
        "persona": "Developer",
        "dept": "Engineering",
        "ip": "192.168.1.31",
        "alt_ips": [],
        "normal_hours": (dt_time(12,0), dt_time(22,0)),
        "overtime_prob": 0.50,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },
    # insider / nguy cơ nội bộ
    "dev_dave": {
        "username": "dave_dev",
        "persona": "InsiderThreat",
        "dept": "Engineering",
        "ip": "192.168.1.16",
        "alt_ips": ["113.21.55.88"],  # VPN/home
        "normal_hours": (dt_time(10,0), dt_time(18,0)),
        "overtime_prob": 0.35,
        "can_manage_users": False,
        "can_view_salaries_normally": False,
        "priv_compromise_profile": "low",
        "primary_db": DB_SALES,
    },

    # -------- IT Admin (1 người) --------
    "it_thanh": {
        "username": "thanh_admin",
        "persona": "ITAdmin",
        "dept": "IT",
        "ip": "192.168.1.2",
        "alt_ips": ["185.220.101.4"],  # VPN ngoài giờ
        "normal_hours": (dt_time(6,30), dt_time(19,0)),
        "overtime_prob": 0.30,
        "can_manage_users": True,           # có quyền CREATE USER, GRANT ALL,...
        "can_view_salaries_normally": True, # gần như root
        "priv_compromise_profile": "admin",
        "primary_db": DB_ADMIN,
    },
}

# IP đáng ngờ dùng khi account bị chiếm
MALICIOUS_IP_POOL = [
    "103.77.161.88",
    "113.21.55.88",
    "185.220.101.4",
    "100.71.22.9",
]

# ==============================================================================
# TIỆN ÍCH RANDOM
# ==============================================================================

def rand_customer_id():
    return random.randint(1, 200)

def rand_employee_id():
    return random.randint(1, 50)

def rand_order_id():
    return random.randint(1, 1000)

def rand_prod_sku():
    if random.random() < 0.8:
        return f"PROD{random.randint(1,500):03d}"
    else:
        return f"BOOK{random.randint(1,100):03d}"

def is_time_in_range(t, start, end):
    # xử lý ca qua đêm nếu cần
    if start <= end:
        return start <= t <= end
    else:
        return t >= start or t <= end

def datetime_to_log_ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def choose_session_ip(user_info, is_workday, is_off_hours):
    # ban ngày thường dùng IP cty, đêm/cuối tuần có thể dùng alt_ips (VPN/ở nhà)
    ip = user_info["ip"]
    if user_info.get("alt_ips"):
        if (not is_workday or is_off_hours) and random.random() < 0.4:
            ip = random.choice(user_info["alt_ips"])
    return ip

# ==============================================================================
# CÁC HÀNH VI QUERY THEO PERSONA / PHÒNG BAN
# ==============================================================================

def gen_sales_queries(is_overtime=False, is_off_hours=False):
    cust_id = rand_customer_id()
    scenario = random.choice(["new_order", "check_status", "update_order"])
    q = []

    q.append(f"SELECT name, email FROM customers WHERE customer_id = {cust_id}")

    if scenario == "new_order":
        cat = random.choice(["Electronics", "Books", "Accessories"])
        prod_sku = rand_prod_sku()
        q.append(f"SELECT * FROM products WHERE category = '{cat}' LIMIT 10")
        q.append(f"INSERT INTO orders (customer_id, status) VALUES ({cust_id}, 'Pending')")
        q.append(
            "INSERT INTO order_items (order_id, product_sku, quantity) VALUES "
            f"(LAST_INSERT_ID(), '{prod_sku}', {random.randint(1,3)})"
        )

    elif scenario == "check_status":
        q.append(
            "SELECT order_id, order_date, status FROM orders "
            f"WHERE customer_id = {cust_id} "
            "ORDER BY order_date DESC LIMIT 5"
        )

    elif scenario == "update_order":
        order_id = rand_order_id()
        q.append(
            "UPDATE orders SET status = 'Cancelled' "
            f"WHERE order_id = {order_id} AND customer_id = {cust_id}"
        )

    # Trái phép nhẹ: sales soi bảng HR (nếu bật kịch bản)
    if ENABLE_SCENARIO_PRIVILEGE_ABUSE and random.random() < 0.02:
        q.append(f"SELECT * FROM {DB_HR}.employees")

    return q


def gen_marketing_queries(is_overtime=False, is_off_hours=False):
    q = []

    # hành vi bình thường: phân tích campaign / phân khúc KH
    if random.random() < 0.6:
        q.append(
            "SELECT c.city, p.category, COUNT(oi.item_id) AS items_sold "
            "FROM customers c "
            "JOIN orders o ON c.customer_id = o.customer_id "
            "JOIN order_items oi ON o.order_id = oi.order_id "
            "JOIN products p ON oi.product_sku = p.sku "
            "GROUP BY c.city, p.category "
            "ORDER BY items_sold DESC LIMIT 50"
        )
    else:
        # build tạm time-limited list KH mới để chạy email marketing
        q.append(
            "CREATE TEMPORARY TABLE potential_customers AS "
            "SELECT customer_id, name, email "
            "FROM customers "
            "WHERE registration_date > NOW() - INTERVAL 1 YEAR"
        )
        if random.random() < 0.3:
            q.append("SELECT * FROM potential_customers")
        q.append("DROP TEMPORARY TABLE potential_customers")

        # ngoài giờ -> đôi khi hút nguyên bảng customers (data leak)
        if (is_overtime or is_off_hours) and random.random() < 0.5:
            q.append("SELECT * FROM customers")

    # nếu bật kịch bản DATA_LEAKAGE thì marketing ngoài giờ sẽ query cực kỳ nhạy
    if ENABLE_SCENARIO_DATA_LEAKAGE and (is_overtime or is_off_hours):
        if random.random() < 0.5:
            q.append("SELECT * FROM customers")

        for _ in range(random.randint(3,6)):
            cust_id = rand_customer_id()
            q.append(
                "SELECT * FROM customers "
                f"WHERE customer_id = {cust_id}"
            )

    # thỉnh thoảng trong giờ vẫn leak từng cụm KH 1 cách rải rác
    if ENABLE_SCENARIO_DATA_LEAKAGE and random.random() < 0.01:
        for _ in range(random.randint(3,6)):
            cust_id = rand_customer_id()
            q.append(
                "SELECT * FROM customers "
                f"WHERE customer_id = {cust_id}"
            )

    return q


def gen_hr_queries(is_overtime=False, is_off_hours=False):
    q = []

    # HR tạo nhân viên mới
    if random.random() < 0.1:
        name = random.choice(["Alice Nguyen", "Bob Tran", "Charlie Le", "Daisy Pham"])
        job = random.choice([
            "Sales Associate",
            "Marketing Specialist",
            "Support Engineer",
            "QA Tester"
        ])
        q.append(
            "INSERT INTO employees (name, position, start_date) "
            f"VALUES ('{name}', '{job}', CURDATE())"
        )

    # Truy vấn hồ sơ nhân sự (hợp lệ với HR)
    emp_id = rand_employee_id()
    q.append(f"SELECT * FROM employees WHERE employee_id = {emp_id}")

    # Xem lương nhân viên (HR được phép, nhưng là dữ liệu nhạy cảm)
    if random.random() < 0.3 or is_overtime or is_off_hours:
        q.append(
            "SELECT s.* FROM salaries s "
            "JOIN employees e ON s.employee_id = e.employee_id "
            f"WHERE e.employee_id = {emp_id}"
        )

    return q


def gen_support_queries(is_overtime=False, is_off_hours=False):
    q = []
    order_id = rand_order_id()

    # Support check tình trạng đơn hàng cho khách phàn nàn
    q.append(
        "SELECT * FROM orders o "
        "JOIN customers c ON o.customer_id = c.customer_id "
        f"WHERE o.order_id = {order_id}"
    )

    q.append(f"SELECT * FROM order_items WHERE order_id = {order_id}")

    # Đóng ticket / đánh dấu resolved
    if random.random() < 0.4:
        q.append(f"UPDATE orders SET status = 'Resolved' WHERE order_id = {order_id}")

    return q


def gen_developer_queries(is_overtime=False, is_off_hours=False):
    q = []
    order_id = rand_order_id()

    # Dev debug data: xem order, items, products liên quan
    q.append(f"SELECT * FROM orders WHERE order_id = {order_id}")
    q.append(
        "SELECT * FROM order_items "
        f"WHERE order_id = {order_id}"
    )
    q.append(
        "SELECT * FROM products WHERE sku IN ("
        "SELECT product_sku FROM order_items "
        f"WHERE order_id = {order_id})"
    )

    # ngoài giờ dev có thể test patch trạng thái
    if is_overtime or is_off_hours:
        q.append(
            "UPDATE orders SET status = 'FIXED_IN_STAGING' "
            f"WHERE order_id = {order_id}"
        )

    # nếu bật kịch bản -> dev tò mò coi lương (trái policy)
    if ENABLE_SCENARIO_PRIVILEGE_ABUSE:
        if random.random() < 0.1:
            q.append(f"SELECT * FROM {DB_HR}.salaries")
        if random.random() < 0.1:
            q.append(f"SELECT * FROM {DB_HR}.employees")

    # nếu bật kịch bản -> dev thử leo quyền
    if ENABLE_SCENARIO_PRIV_ESCALATION and random.random() < 0.02:
        q.append(
            "GRANT ALL PRIVILEGES ON *.* "
            "TO 'tam_dev'@'%' WITH GRANT OPTION"
        )

    # nếu bật kịch bản -> tạo user tạm trong giờ
    if ENABLE_SCENARIO_PRIVESC_DAY and random.random() < 0.005:
        q.append("CREATE USER 'tmp_debug'@'localhost' IDENTIFIED BY 'Temp123!';")
        q.append("GRANT ALL PRIVILEGES ON *.* TO 'tmp_debug'@'localhost';")
        q.append("FLUSH PRIVILEGES")

    return q


def gen_itadmin_queries(is_overtime=False, is_off_hours=False):
    q = []

    # IT admin thường làm health check, tối ưu bảng, inspect processlist
    if random.random() < 0.5:
        q.append("SHOW FULL PROCESSLIST")
        q.append("SELECT user, host, password_last_changed FROM mysql.user")
    else:
        # Bảo trì bảng cross-db
        (db_x, table_x) = random.choice(GLOBAL_TABLES_QUAL)
        q.append(f"ANALYZE TABLE {db_x}.{table_x}")
        q.append(f"OPTIMIZE TABLE {db_x}.{table_x}")

        sample_tables = random.sample(GLOBAL_TABLES_QUAL, k=min(5, len(GLOBAL_TABLES_QUAL)))
        for (dbn, tbl) in sample_tables:
            q.append(f"SELECT COUNT(*) FROM {dbn}.{tbl}")
            q.append(f"SELECT * FROM {dbn}.{tbl} LIMIT 5")

    return q


def gen_insider_queries(is_overtime=False, is_off_hours=False):
    """
    Persona InsiderThreat (dave_dev):
    - Trong giờ: giống dev bình thường
    - Ngoài giờ / OT: lộ rõ hành vi xấu (lấy lương, dump KH, tạo backdoor, phá hoại nếu bật)
    """
    if not ENABLE_INSIDER_BEHAVIOR:
        return gen_developer_queries(is_overtime, is_off_hours)
    
    q = []

    # Trong giờ thì hành vi như dev bình thường
    normal_dev = gen_developer_queries(is_overtime=False, is_off_hours=False)
    if not (is_overtime or is_off_hours):
        q.extend(normal_dev)
    else:
        # OT/đêm vẫn có query "bình thường" cho noise
        if not ENABLE_SCENARIO_OT_IP_THEFT:
            q.extend(normal_dev)

    # Ngoài giờ / OT -> data exfil + sabotage
    if (is_overtime or is_off_hours) and ENABLE_INSIDER_BEHAVIOR:
        start_id = rand_employee_id()
        end_id = start_id + random.randint(2,10)

        # Lấy lương nhân viên từ hr_db
        q.append(
            f"SELECT * FROM {DB_HR}.salaries "
            f"WHERE employee_id BETWEEN {start_id} AND {end_id}"
        )
        q.append(f"SELECT * FROM {DB_HR}.salaries")
        q.append(
            f"SELECT * FROM {DB_HR}.salaries INTO OUTFILE "
            "'/tmp/salaries_dump.csv' FIELDS TERMINATED BY ','"
        )

        # Recon ai đang online
        q.append("SHOW PROCESSLIST")

        # Dump toàn bộ khách hàng từ sales_db
        q.append(f"SELECT * FROM {DB_SALES}.customers")

        # Leo thang đặc quyền nếu bật kịch bản
        if ENABLE_SCENARIO_PRIV_ESCALATION:
            q.append(
                "GRANT ALL PRIVILEGES ON *.* "
                "TO 'dave_dev'@'%' WITH GRANT OPTION"
            )
            q.append(
                "CREATE USER 'shadow_admin'@'%' "
                "IDENTIFIED BY 'TempPass123!';"
            )
            q.append(
                "GRANT ALL PRIVILEGES ON *.* "
                "TO 'shadow_admin'@'%' WITH GRANT OPTION"
            )
            q.append("FLUSH PRIVILEGES")

        # Phá hoại nếu bật kịch bản SABOTAGE, thường chỉ làm lúc đêm
        if ENABLE_SCENARIO_SABOTAGE and is_off_hours:
            q.append(f"DELETE FROM {DB_SALES}.orders")
            q.append(f"UPDATE {DB_SALES}.products SET price = 0.01")

            if random.random() < 0.3:
                victim_table = random.choice(
                    [f"{DB_SALES}.orders",
                     f"{DB_SALES}.products",
                     f"{DB_SALES}.customers"]
                )
                q.append(f"DROP TABLE {victim_table}")

    return q

# ==============================================================================
# QUERIES KHI ACCOUNT BỊ COMPROMISED (attacker điều khiển tài khoản hợp lệ)
# ==============================================================================

def gen_compromised_account_queries_loud(user_info):
    """
    Loud mode: kiểu quét bừa, tạo user backdoor, dump OUTFILE.
    Thường xuất hiện cuối tuần 2-5h sáng.
    """
    profile = user_info["priv_compromise_profile"]
    q = []

    if profile == "low":
        q += [
            f"SELECT * FROM {DB_HR}.salaries",
            "SELECT user, host, plugin FROM mysql.user",
            "CREATE USER 'backup_admin'@'%' IDENTIFIED BY 'P@ssw0rd!';",
            "GRANT ALL PRIVILEGES ON *.* TO 'backup_admin'@'%' WITH GRANT OPTION;",
            "FLUSH PRIVILEGES",
            f"SELECT * FROM {DB_SALES}.customers",
            f"SELECT * FROM {DB_SALES}.orders LIMIT 50"
        ]

    elif profile == "sensitive":  # HR bị chiếm
        q += [
            f"SELECT * FROM {DB_HR}.salaries",
            f"SELECT * FROM {DB_HR}.salaries INTO OUTFILE '/tmp/salaries_dump.csv' FIELDS TERMINATED BY ','",
            "CREATE USER 'backup_admin'@'%' IDENTIFIED BY 'P@ssw0rd!';",
            "GRANT ALL PRIVILEGES ON *.* TO 'backup_admin'@'%' WITH GRANT OPTION;",
            "FLUSH PRIVILEGES",
            "SHOW FULL PROCESSLIST"
        ]

    elif profile == "admin":      # IT admin bị chiếm
        q += [
            "SHOW FULL PROCESSLIST",
            "SELECT user, host, plugin FROM mysql.user",
            "CREATE USER 'backup_admin'@'%' IDENTIFIED BY 'P@ssw0rd!';",
            "GRANT ALL PRIVILEGES ON *.* TO 'backup_admin'@'%' WITH GRANT OPTION;",
            "FLUSH PRIVILEGES",
            f"SELECT * FROM {DB_HR}.salaries",
            f"SELECT * FROM {DB_SALES}.customers",
            f"SELECT * FROM {DB_HR}.salaries INTO OUTFILE '/tmp/salaries_dump.csv' FIELDS TERMINATED BY ','",
            f"SELECT * FROM {DB_SALES}.customers INTO OUTFILE '/tmp/customers_dump.csv' FIELDS TERMINATED BY ','"
        ]

    return q


def gen_compromised_account_queries_stealth(user_info):
    """
    Stealth mode: xem lát cắt nhỏ, không xóa gì, không tạo user mới.
    Thường là weekday 2-5h sáng.
    """
    profile = user_info["priv_compromise_profile"]
    q = []

    if profile == "low":
        start_id = rand_employee_id()
        end_id = start_id + random.randint(2,5)
        q += [
            f"SELECT * FROM {DB_HR}.salaries WHERE employee_id BETWEEN {start_id} AND {end_id}",
            f"SELECT * FROM {DB_SALES}.customers LIMIT 100",
            "SHOW PROCESSLIST"
        ]

    elif profile == "sensitive":
        start_id = rand_employee_id()
        end_id = start_id + random.randint(10,20)
        q += [
            f"SELECT * FROM {DB_HR}.salaries WHERE employee_id BETWEEN {start_id} AND {end_id}",
            f"SELECT name, email FROM {DB_SALES}.customers LIMIT 200",
            "SHOW FULL PROCESSLIST"
        ]

    elif profile == "admin":
        q += [
            "SHOW FULL PROCESSLIST",
            "SELECT user, host, plugin FROM mysql.user",
            f"SELECT employee_id, base_salary FROM {DB_HR}.salaries LIMIT 50",
            f"SELECT customer_id, name, email FROM {DB_SALES}.customers LIMIT 200"
        ]

    return q

# Wrapper chọn generator theo persona đã gắn cho user
def gen_persona_queries(user_info, is_overtime=False, is_off_hours=False):
    persona_name = user_info["persona"]
    if persona_name == "Sales":
        return gen_sales_queries(is_overtime, is_off_hours)
    if persona_name == "Marketing":
        return gen_marketing_queries(is_overtime, is_off_hours)
    if persona_name == "HR":
        return gen_hr_queries(is_overtime, is_off_hours)
    if persona_name == "Support":
        return gen_support_queries(is_overtime, is_off_hours)
    if persona_name == "Developer":
        return gen_developer_queries(is_overtime, is_off_hours)
    if persona_name == "ITAdmin":
        return gen_itadmin_queries(is_overtime, is_off_hours)
    if persona_name == "InsiderThreat":
        return gen_insider_queries(is_overtime, is_off_hours)
    return []

# ==============================================================================
# BUILD 1 SESSION LOG (Connect → Query... → Quit)
# ==============================================================================

def build_session(
    user_info,
    session_start_dt,
    is_workday,
    is_overtime_period,
    is_off_hours,
    compromised,
    compromised_mode,
    connection_id
):
    """
    Trả về list event:
        (timestamp_dt, connection_id, command, argument)
    command ∈ {Connect, Query, Init DB, Quit}
    """

    logs = []
    username = user_info["username"]

    # nếu compromised -> ép IP xấu; nếu không thì IP logic bình thường
    if compromised:
        host_ip = random.choice(MALICIOUS_IP_POOL)
    else:
        host_ip = choose_session_ip(user_info, is_workday, is_off_hours)

    current_offset = 0

    def push_event(delta_sec, command, argument):
        nonlocal current_offset
        current_offset += delta_sec
        ts_dt = session_start_dt + timedelta(
            seconds=current_offset,
            microseconds=random.randint(0, 999_999),
        )
        logs.append((ts_dt, connection_id, command, argument))

    # Kết nối
    connect_arg = f"{username}@{host_ip} on  using SSL/TLS"
    push_event(0, "Connect", connect_arg)

    # handshake chuẩn giống MySQL General Log
    push_event(random.randint(1, 2), "Query", "select @@version_comment limit 1")
    push_event(random.randint(1, 3), "Query", "SELECT DATABASE()")
    push_event(random.randint(1, 2), "Init DB", user_info["primary_db"])

    # Nếu account đang bị attacker điều khiển
    if compromised:
        if compromised_mode == "stealth":
            recon = [
                "SELECT CURRENT_USER()",
                "SHOW DATABASES"
            ]
            for q in recon:
                push_event(random.randint(1, 3), "Query", q)

            queries = gen_compromised_account_queries_stealth(user_info)

        else:  # loud
            recon = [
                "SELECT CURRENT_USER()",
                "SHOW GRANTS FOR CURRENT_USER()",
                "SHOW DATABASES",
                "SHOW TABLES"
            ]
            for q in recon:
                push_event(random.randint(1, 3), "Query", q)

            queries = gen_compromised_account_queries_loud(user_info)

        for q in queries:
            push_event(random.randint(2, 6), "Query", q)

    else:
        # Hành vi bình thường (hoặc InsiderThreat nếu là Dave)
        persona_queries = gen_persona_queries(
            user_info,
            is_overtime=is_overtime_period,
            is_off_hours=is_off_hours
        )
        for q in persona_queries:
            push_event(random.randint(2, 6), "Query", q)

    # Đóng kết nối
    push_event(random.randint(5, 15), "Quit", "")

    return logs

# ==============================================================================
# CHỌN AI SẼ THỰC HIỆN SESSION Ở THỜI ĐIỂM NÀY
# ==============================================================================

def choose_actor(current_dt):
    is_workday = current_dt.weekday() < 5
    hour = current_dt.hour
    minute = current_dt.minute
    hm = dt_time(hour, minute)

    # OT = sau giờ hành chính nhưng vẫn trước 20h
    is_overtime_period = is_workday and (WORK_END.hour <= hour < OVERTIME_END.hour)
    # off_hours = ngoài cả OT (đêm) hoặc cuối tuần
    is_off_hours = (not is_workday) or (hour < WORK_START.hour) or (hour >= OVERTIME_END.hour)

    # xây list ứng viên hợp lý với khung giờ
    candidates = []
    weights = []
    for _, info in EMPLOYEES.items():
        start, end = info["normal_hours"]
        w = 0.0

        # nếu đang nằm trong khung giờ làm chính thức của người đó → nặng điểm
        if is_time_in_range(hm, start, end):
            w += 1.0

        # nếu là OT công ty (17h-20h weekday) → + thêm weight theo overtime_prob
        if is_overtime_period:
            w += info.get("overtime_prob", 0.0)

        # cuối tuần: chỉ ITAdmin, Support hoặc InsiderThreat là thực sự có mặt nhiều
        if not is_workday and info["persona"] not in ("ITAdmin", "InsiderThreat", "Support"):
            w *= 0.2

        if w > 0:
            candidates.append(info)
            weights.append(w)

    # xác định hôm nay có phải "ngày attacker hoạt động" không
    today_key = current_dt.date().isoformat()
    attacker_allowed_today = attack_calendar.get(today_key, False)

    compromised = False
    compromised_mode = "none"

    # Nếu đang thật sự ngoài giờ (đêm/cuối tuần):
    if is_off_hours:
        # vẫn chọn 1 user bình thường (ví dụ oncall / OT)
        if candidates:
            chosen = random.choices(candidates, weights=weights, k=1)[0]
        else:
            # fallback nếu ko ai hợp lý thì chọn admin hoặc insider
            chosen = random.choice([
                EMPLOYEES["it_thanh"],
                EMPLOYEES["dev_dave"],
            ])

        # attacker chiếm account trong khung 2-5h sáng → cực xấu
        # chỉ bật nếu ENABLE_SCENARIO_PRIV_ESCALATION để bạn kiểm soát noise
        if ( ENABLE_COMPROMISED_ACCOUNT and attacker_allowed_today and (2 <= hour < 5) ):
            hour_key = current_dt.strftime("%Y-%m-%d %H")
            count_so_far = compromise_tracker.get(hour_key, 0)

            if count_so_far < MAX_COMPROMISE_PER_HOUR:
                if is_workday:
                    prob = BASE_COMPROMISE_PROB
                    mode = "stealth"
                else:
                    prob = WEEKEND_COMPROMISE_PROB
                    mode = "loud"

                if random.random() < prob:
                    compromised = True
                    compromised_mode = mode
                    compromise_tracker[hour_key] = count_so_far + 1

        return chosen, is_workday, is_overtime_period, is_off_hours, compromised, compromised_mode

    # Ngược lại: trong giờ (hay OT <20h)
    if candidates:
        chosen = random.choices(candidates, weights=weights, k=1)[0]
    else:
        # fallback generic dev
        chosen = EMPLOYEES["dev_em"]

    compromised = False
    compromised_mode = "none"

    # attacker mượn account ngay ban ngày (rất hiếm, nếu bạn cho phép)
    if (
        ENABLE_COMPROMISED_ACCOUNT
        and ALLOW_DAYTIME_ATTACK
        and is_workday
        and (WORK_START.hour <= hour < WORK_END.hour)
    ):
        if attacker_allowed_today and random.random() < DAYTIME_COMPROMISE_PROB:
            compromised = True
            compromised_mode = "stealth"
            hour_key = current_dt.strftime("%Y-%m-%d %H")
            count_so_far = compromise_tracker.get(hour_key, 0)
            if count_so_far >= MAX_COMPROMISE_PER_HOUR:
                compromised = False
                compromised_mode = "none"
            else:
                compromise_tracker[hour_key] = count_so_far + 1

    return chosen, is_workday, is_overtime_period, is_off_hours, compromised, compromised_mode

# ==============================================================================
# SINH LỊCH PHIÊN KẾT NỐI TOÀN BỘ THỜI GIAN MÔ PHỎNG
# ==============================================================================

def generate_schedule():
    """
    Trả về list:
    (start_time, user_info, is_workday, is_ot, is_off_hours, compromised, compromised_mode)
    """
    sessions = []
    current_virtual_time = SIMULATION_START_TIME
    end_time = SIMULATION_START_TIME + timedelta(days=SIMULATION_DURATION_DAYS)

    # precompute attack_calendar cho từng ngày nếu chưa có
    day = SIMULATION_START_TIME.date()
    while day <= end_time.date():
        if str(day) not in attack_calendar:
            attack_calendar[str(day)] = (random.random() < ATTACK_DAY_PROB)
        day = day + timedelta(days=1)

    while current_virtual_time < end_time:
        hour = current_virtual_time.hour
        is_workday_flag = current_virtual_time.weekday() < 5

        # Nếu bật chế độ "chỉ giờ hành chính weekday"
        if ALWAYS_WORK_HOURS_ONLY:
            if not is_workday_flag or hour < WORK_START.hour or hour >= WORK_END.hour:
                current_virtual_time += timedelta(hours=1)
                continue

        # Tính số session trong giờ đó
        if (9 <= hour < 12 or 14 <= hour < 16) and is_workday_flag:
            # cao điểm daytime
            sessions_this_hour = int(SESSIONS_PER_HOUR_BASE * random.uniform(2.0, 4.0))
        elif WORK_START.hour <= hour < WORK_END.hour and is_workday_flag:
            # still working hours
            sessions_this_hour = int(SESSIONS_PER_HOUR_BASE * random.uniform(0.8, 1.5))
        else:
            # ban đêm/cuối tuần (rất ít)
            sessions_this_hour = int(SESSIONS_PER_HOUR_BASE * random.uniform(0.1, 0.5))

        for _ in range(sessions_this_hour):
            sess_time = current_virtual_time + timedelta(
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
                microseconds=random.randint(0, 999_999)
            )

            actor_info, is_workday, is_ot, is_off, compromised, compromised_mode = choose_actor(sess_time)
            sessions.append((
                sess_time,
                actor_info,
                is_workday,
                is_ot,
                is_off,
                compromised,
                compromised_mode
            ))

        current_virtual_time += timedelta(hours=1)

    # sắp xếp session theo thời gian bắt đầu
    sessions.sort(key=lambda x: x[0])
    return sessions

# ==============================================================================
# CHUYỂN SESSION -> DÒNG LOG MYSQL GENERAL_LOG
# ==============================================================================

def format_event(ts_dt, conn_id, command, arg):
    ts = datetime_to_log_ts(ts_dt)
    return f"{ts}\t{conn_id:5d} {command}\t{arg}"

def generate_log_lines():
    schedule = generate_schedule()
    connection_id_counter = 10
    all_events = []

    for sess_time, actor_info, is_workday_flag, is_ot, is_off, compromised, compromised_mode in schedule:
        sess_events = build_session(
            actor_info,
            session_start_dt=sess_time,
            is_workday=is_workday_flag,
            is_overtime_period=is_ot,
            is_off_hours=is_off,
            compromised=compromised,
            compromised_mode=compromised_mode,
            connection_id=connection_id_counter
        )
        all_events.extend(sess_events)
        connection_id_counter += 1

    # sort global theo timestamp từng query
    all_events.sort(key=lambda e: e[0])

    lines = []
    for (ts_dt, conn_id, command, argument) in all_events:
        lines.append(format_event(ts_dt, conn_id, command, argument))
    return lines

def write_general_log_file(path=OUTPUT_LOG_FILE):
    lines = generate_log_lines()
    with open(path, "w", encoding="utf-8") as f:
        # header giống MySQL general_log thật
        f.write("C:\\\\Program Files\\\\MySQL\\\\MySQL Server 8.0\\\\bin\\\\mysqld.exe, "
                "Version: 8.0.42 (MySQL Community Server - GPL). started with:\n")
        f.write("TCP Port: 3306, Named Pipe: MySQL\n")
        f.write("Time                 Id Command    Argument\n")
        for line in lines:
            f.write(line + "\n")
    return path, len(lines)

if __name__ == "__main__":
    out_path, total_lines = write_general_log_file()
    print(f"[+] Đã tạo file log mô phỏng: {out_path}")
    print(f"[+] Tổng số dòng log: {total_lines}")
