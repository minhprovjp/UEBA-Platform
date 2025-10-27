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
# 1. CẤU HÌNH CHUNG
# ==============================================================================
OUTPUT_LOG_FILE = "simulated_general_log.txt"

DB_NAME = "company_db"

SIMULATION_START_TIME = datetime(2025, 10, 20, 7, 0, 0)
SIMULATION_DURATION_DAYS = 30      # Mô phỏng 7 ngày
SESSIONS_PER_HOUR_BASE = 1000       # Tăng số này lên (ví dụ 50) nếu muốn log cực lớn

# Giờ làm việc công ty
WORK_START = dt_time(7, 0)
WORK_END = dt_time(17, 0)
OVERTIME_END = dt_time(20, 0)

COMPANY_IP_RANGE = "192.168.1."

# Các bảng để ITAdmin quét (Rule 3)
TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "employees",
    "salaries",
    "system_logs",
    "marketing_campaigns"
]

# Bảng nhạy cảm (Rule 4)
SENSITIVE_TABLES = ["employees", "salaries", "customers", "mysql.user"]

# Danh sách nhân sự / persona
# normal_hours => khung giờ làm việc bình thường (Rule 5 baseline)
# overtime_prob => xác suất có mặt 17h-20h
# alt_ips => IP ngoài công ty (dùng ban đêm/cuối tuần)
EMPLOYEES = {
    "sales_user_anh": {
        "username": "anh_sales",
        "persona": "Sales",
        "ip": COMPANY_IP_RANGE + "10",
        "alt_ips": [],
        "overtime_prob": 0.15,
        "normal_hours": (dt_time(8, 30), dt_time(17, 30)),
        "allowed_sensitive": []
    },
    "marketing_user_binh": {
        "username": "binh_mkt",
        "persona": "Marketing",
        "ip": COMPANY_IP_RANGE + "11",
        "alt_ips": [],
        "overtime_prob": 0.05,
        "normal_hours": (dt_time(9, 0), dt_time(18, 0)),
        "allowed_sensitive": []
    },
    "hr_user_chi": {
        "username": "chi_hr",
        "persona": "HR",
        "ip": COMPANY_IP_RANGE + "12",
        "alt_ips": [],
        "overtime_prob": 0.01,
        "normal_hours": (dt_time(8, 0), dt_time(17, 0)),
        "allowed_sensitive": ["employees", "salaries"]
    },
    "support_user_dung": {
        "username": "dung_support",
        "persona": "Support",
        "ip": COMPANY_IP_RANGE + "13",
        "alt_ips": [],
        "overtime_prob": 0.20,
        "normal_hours": (dt_time(7, 30), dt_time(19, 0)),
        "allowed_sensitive": []
    },
    "dev_user_em": {
        "username": "em_dev",
        "persona": "Developer",
        "ip": COMPANY_IP_RANGE + "15",
        "alt_ips": [],
        "overtime_prob": 0.40,
        "normal_hours": (dt_time(10, 0), dt_time(20, 0)),
        "allowed_sensitive": []
    },
    "it_admin_thanh": {
        "username": "thanh_admin",
        "persona": "ITAdmin",
        "ip": COMPANY_IP_RANGE + "2",
        "alt_ips": [],
        "overtime_prob": 0.30,
        "normal_hours": (dt_time(6, 30), dt_time(19, 0)),
        "allowed_sensitive": ["mysql.user", "system_logs"]
    },
    "insider_dave": {
        "username": "dave_dev",
        "persona": "InsiderThreat",
        "ip": COMPANY_IP_RANGE + "16",
        "alt_ips": ["113.21.55.88"],  # IP ở nhà hoặc VPN lạ
        "overtime_prob": 0.35,
        "normal_hours": (dt_time(10, 0), dt_time(18, 0)),
        "allowed_sensitive": []
    }
}

EXTERNAL_ATTACKER = {
    "username": "attacker",
    "persona": "Attacker",
    "ip": "103.77.161.88",
    "alt_ips": [],
    "overtime_prob": 0.0,
    "normal_hours": (dt_time(2, 0), dt_time(5, 0)),
    "allowed_sensitive": []
}

# Tình huống (có thể bật/tắt)
ENABLE_SCENARIO_PRIVILEGE_ABUSE = True  # Dev truy cập dữ liệu nhạy cảm không liên quan đến công việc
ENABLE_SCENARIO_DATA_LEAKAGE = True     # Marketing gia tăng đột ngột việc truy cập dữ liệu nhạy cảm
ENABLE_SCENARIO_OT_IP_THEFT = True      # InsiderThreat đánh cắp tài sản trí tuệ bằng cách truy cập ngoài giờ
ENABLE_SCENARIO_SABOTAGE = True         # InsiderThreat cố ý phá hoại tài sản công ty

# ==============================================================================
# 2. TIỆN ÍCH NHỎ
# ==============================================================================

def rand_customer_id():
    return random.randint(1, 200)

def rand_employee_id():
    return random.randint(1, 50)

def rand_order_id():
    return random.randint(1, 1000)

def rand_prod_sku():
    # SKU kiểu PROD### hoặc BOOK###
    if random.random() < 0.8:
        return f"PROD{random.randint(1,500):03d}"
    else:
        return f"BOOK{random.randint(1,100):03d}"

def is_time_in_range(t, start, end):
    # nếu start > end thì coi như qua nửa đêm
    if start <= end:
        return start <= t <= end
    else:
        return t >= start or t <= end

def datetime_to_log_ts(dt):
    # MySQL general_log style: 2025-05-20T14:37:52.279990Z
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def choose_session_ip(user_info, is_workday, is_off_hours):
    # ban ngày dùng IP công ty, ban đêm/cuối tuần đôi khi IP lạ
    ip = user_info["ip"]
    if user_info.get("alt_ips"):
        if (not is_workday or is_off_hours) and random.random() < 0.4:
            ip = random.choice(user_info["alt_ips"])
    return ip

# ==============================================================================
# 3. QUERY PATTERNS CHO TỪNG LOẠI USER
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
    if ENABLE_SCENARIO_PRIVILEGE_ABUSE and random.random() < 0.05:
        q.append("SELECT * FROM employees")
    return q

def gen_marketing_queries(is_overtime=False, is_off_hours=False):
    q = []
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
        q.append(
            "CREATE TEMPORARY TABLE potential_customers AS "
            "SELECT customer_id, name, email "
            "FROM customers "
            "WHERE registration_date > NOW() - INTERVAL 1 YEAR"
        )
        if random.random() < 0.3:
            q.append("SELECT * FROM potential_customers")
        q.append("DROP TEMPORARY TABLE potential_customers")

        # ban đêm có thể chơi bẩn dump full customers
        if (is_overtime or is_off_hours) and random.random() < 0.5:
            q.append("SELECT * FROM customers")
    # === DATA LEAKAGE SCENARIO ===
    # Chỉ kích hoạt ngoài giờ hoặc cuối tuần -> gom data khách hàng chi tiết
    if ENABLE_SCENARIO_DATA_LEAKAGE and (is_overtime or is_off_hours):
        # dump cả bảng
        if random.random() < 0.5:
            q.append("SELECT * FROM customers")

        # query theo từng khách hàng một cách lặp lại (pattern scraping)
        # tạo 3-6 query kiểu SELECT * WHERE customer_id = ...
        for _ in range(random.randint(3,6)):
            cust_id = rand_customer_id()
            q.append(
                "SELECT * FROM customers "
                f"WHERE customer_id = {cust_id}"
            )
    return q

def gen_hr_queries(is_overtime=False, is_off_hours=False):
    q = []
    if random.random() < 0.1:
        name = random.choice(["Alice Nguyen", "Bob Tran", "Charlie Le", "Daisy Pham"])
        job = random.choice(["Sales Associate", "Marketing Specialist", "Support Engineer", "QA Tester"])
        q.append(
            "INSERT INTO employees (name, position, start_date) "
            f"VALUES ('{name}', '{job}', CURDATE())"
        )

    emp_id = rand_employee_id()
    q.append(f"SELECT * FROM employees WHERE employee_id = {emp_id}")

    # xem lương (nhạy cảm). vẫn có thể xảy ra ngoài giờ => noise hợp lệ
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
    q.append(
        "SELECT * FROM orders o "
        "JOIN customers c ON o.customer_id = c.customer_id "
        f"WHERE o.order_id = {order_id}"
    )
    q.append(f"SELECT * FROM order_items WHERE order_id = {order_id}")
    if random.random() < 0.4:
        q.append(f"UPDATE orders SET status = 'Resolved' WHERE order_id = {order_id}")
    return q

def gen_developer_queries(is_overtime=False, is_off_hours=False):
    q = []
    order_id = rand_order_id()
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
    if is_overtime or is_off_hours:
        q.append(
            "UPDATE orders SET status = 'FIXED_IN_STAGING' "
            f"WHERE order_id = {order_id}"
        )
    # === PRIVILEGE ABUSE SCENARIO ===
    # Dev tò mò xem lương dù không cần
    if ENABLE_SCENARIO_PRIVILEGE_ABUSE:
        # Không cần phải là ban đêm, để test rule 4 trong giờ làm
        if random.random() < 0.1:  # hiếm thôi
            q.append("SELECT * FROM salaries")
        if random.random() < 0.1:
            q.append("SELECT * FROM employees")
    return q

def gen_itadmin_queries(is_overtime=False, is_off_hours=False):
    q = []
    if random.random() < 0.5:
        # Health check + đụng tới mysql.user (nhạy cảm nhưng hợp lệ cho IT)
        q.append("SHOW FULL PROCESSLIST")
        q.append("SELECT user, host, password_last_changed FROM mysql.user")
    else:
        # Quét hàng loạt bảng rất nhanh → Rule 3
        table = random.choice(TABLES)
        q.append(f"ANALYZE TABLE {table}")
        q.append(f"OPTIMIZE TABLE {table}")
        for tbl in random.sample(TABLES, k=min(5, len(TABLES))):
            q.append(f"SELECT COUNT(*) FROM {tbl}")
            q.append(f"SELECT * FROM {tbl} LIMIT 5")
    return q

def gen_insider_queries(is_overtime=False, is_off_hours=False):
    q = []

    # Bình thường để nguỵ trang (giống dev)
    normal_dev = gen_developer_queries(is_overtime=False, is_off_hours=False)
    # Nhưng nếu đây là OT (18-20h) hoặc off-hours (đêm, cuối tuần),
    # ta có thể giảm bớt hành vi bình thường để pattern nhìn rõ ràng hơn
    if not (is_overtime or is_off_hours):
        q.extend(normal_dev)
    else:
        if ENABLE_SCENARIO_OT_IP_THEFT:
            # thay vì debug order -> hắn chỉ tập trung vào data nhạy cảm
            pass
        else:
            # nếu tắt scenario này thì cứ behave như dev
            q.extend(normal_dev)

    # === DATA THEFT / IP THEFT SCENARIO ===
    # chỉ khi OT hoặc off-hours mới bùng nổ hành vi xấu
    if (is_overtime or is_off_hours):
        # dump lương nội bộ
        q.append(
            "SELECT * FROM salaries "
            f"WHERE employee_id BETWEEN {rand_employee_id()} AND {rand_employee_id()+5}"
        )

        if ENABLE_SCENARIO_OT_IP_THEFT:
            # Full dump salary
            q.append("SELECT * FROM salaries")

            # xuất file (exfil)
            q.append(
                "SELECT * FROM salaries INTO OUTFILE '/tmp/salaries_dump.csv' "
                "FIELDS TERMINATED BY ','"
            )

            # lộ ý đồ theo dõi ai online
            q.append("SHOW PROCESSLIST")

            # lấy data khách hàng hàng loạt
            q.append("SELECT * FROM customers")
        # === SABOTAGE SCENARIO ===
    if ENABLE_SCENARIO_SABOTAGE and is_off_hours and random.random() < 0.15:
        # xoá toàn bộ orders (không WHERE)
        q.append("DELETE FROM orders")

        # phá giá sản phẩm
        q.append("UPDATE products SET price = 0.01")

        # hành vi cực đoan hơn
        if random.random() < 0.3:
            victim_table = random.choice(["orders", "products", "customers"])
            q.append(f"DROP TABLE {victim_table}")
    return q

def gen_attacker_queries(is_overtime=False, is_off_hours=False):
    q = []
    # Recon
    q.append("SHOW DATABASES")
    q.append("SHOW TABLES")
    for tbl in random.sample(SENSITIVE_TABLES, k=min(2, len(SENSITIVE_TABLES))):
        q.append(f"DESCRIBE `{tbl}`")

    # Khai thác
    if random.random() < 0.5:
        q.append(
            "SELECT * FROM users WHERE id = 1 "
            "AND IF(1=1, SLEEP(5), 0)"
        )
    else:
        q.append(
            "SELECT name, email FROM customers "
            "UNION SELECT user, password FROM mysql.user"
        )

    # Trích xuất
    q.append("SELECT * FROM salaries")
    if random.random() < 0.5:
        q.append(
            "SELECT * INTO OUTFILE '/tmp/products_export.csv' FROM products"
        )
    return q

def gen_persona_queries(persona_name, is_overtime=False, is_off_hours=False):
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
    if persona_name == "Attacker":
        return gen_attacker_queries(is_overtime, is_off_hours)
    return []

# ==============================================================================
# 4. SINH RA 1 PHIÊN LÀM VIỆC → DANH SÁCH EVENT (Connect/Query/...)
# ==============================================================================

def build_session(user_info, session_start_dt, is_workday, is_overtime_period, is_off_hours, connection_id):
    """
    Trả về list event cho 1 connection:
      (timestamp_dt, connection_id, command, argument)
    command ∈ {Connect, Query, Init DB, Quit}
    """
    logs = []

    persona_name = user_info["persona"]
    username = user_info["username"]
    host_ip = choose_session_ip(user_info, is_workday, is_off_hours)

    # offset thời gian trong phiên (giây)
    current_offset = 0

    def push_event(delta_sec, command, argument):
        nonlocal current_offset
        current_offset += delta_sec
        ts_dt = session_start_dt + timedelta(
            seconds=current_offset,
            microseconds=random.randint(0, 999_999)
        )
        logs.append((ts_dt, connection_id, command, argument))

    # Connect
    connect_arg = f"{username}@{host_ip} on  using SSL/TLS"
    push_event(0, "Connect", connect_arg)

    # handshake giống client thật
    push_event(random.randint(1, 2), "Query", "select @@version_comment limit 1")
    push_event(random.randint(1, 4), "Query", "SELECT DATABASE()")

    # chọn DB
    push_event(random.randint(1, 3), "Init DB", DB_NAME)

    # truy vấn thực tế
    persona_queries = gen_persona_queries(
        persona_name,
        is_overtime=is_overtime_period,
        is_off_hours=is_off_hours
    )

    for q in persona_queries:
        push_event(random.randint(2, 6), "Query", q)

    # Quit
    push_event(random.randint(5, 15), "Quit", "")

    return logs

# ==============================================================================
# 5. CHỌN AI SẼ TẠO PHIÊN TẠI THỜI ĐIỂM X
# ==============================================================================

def choose_actor(current_dt):
    """
    Trả về (user_info, is_workday_flag, is_overtime_period, is_off_hours)
    Logic:
      - Trong giờ làm việc -> nhân viên bình thường
      - Tăng ca -> Developer/IT/Insider dày hơn
      - 2-5h sáng -> attacker + insider
      - Cuối tuần -> hầu như chỉ ITAdmin, InsiderThreat, attacker
    """
    is_workday = current_dt.weekday() < 5
    hour = current_dt.hour
    minute = current_dt.minute
    hm = dt_time(hour, minute)

    is_overtime_period = is_workday and (WORK_END.hour <= hour < OVERTIME_END.hour)
    is_off_hours = (not is_workday) or (hour < WORK_START.hour) or (hour >= OVERTIME_END.hour)

    candidates = []
    weights = []

    for _, info in EMPLOYEES.items():
        persona = info["persona"]
        start, end = info["normal_hours"]

        weight = 0.0
        # giờ bình thường
        if is_time_in_range(hm, start, end):
            weight += 1.0

        # tăng ca
        if is_overtime_period:
            weight += info.get("overtime_prob", 0.0)

        # insider được buff nếu ngoài giờ
        if persona == "InsiderThreat" and (is_overtime_period or is_off_hours):
            weight += 2.0

        # cuối tuần giảm mạnh sales/marketing/... nhưng vẫn giữ IT + Insider
        if not is_workday and persona not in ("ITAdmin", "InsiderThreat"):
            weight *= 0.2

        if weight > 0:
            candidates.append(info)
            weights.append(weight)

    # attacker: thường xuyên lúc 2-5h sáng và ngoài giờ
    attacker_bonus = 0.0
    if is_off_hours and (2 <= hour < 5):
        attacker_bonus = 2.5
    elif is_off_hours:
        attacker_bonus = 0.3

    if candidates and attacker_bonus > 0:
        total_internal = sum(weights)
        if random.random() < attacker_bonus / (attacker_bonus + total_internal):
            return EXTERNAL_ATTACKER, is_workday, is_overtime_period, is_off_hours
        else:
            chosen = random.choices(candidates, weights=weights, k=1)[0]
            return chosen, is_workday, is_overtime_period, is_off_hours

    if candidates:
        chosen = random.choices(candidates, weights=weights, k=1)[0]
        return chosen, is_workday, is_overtime_period, is_off_hours

    # fallback
    return EXTERNAL_ATTACKER, is_workday, is_overtime_period, is_off_hours

# ==============================================================================
# 6. LẬP LỊCH TẤT CẢ CÁC PHIÊN TRONG 7 NGÀY
# ==============================================================================

def generate_schedule():
    """
    Kết quả: list[(start_time, user_info, is_workday, is_ot, is_off_hours)]
    """
    sessions = []
    current_virtual_time = SIMULATION_START_TIME
    end_time = SIMULATION_START_TIME + timedelta(days=SIMULATION_DURATION_DAYS)

    while current_virtual_time < end_time:
        hour = current_virtual_time.hour
        is_workday = current_virtual_time.weekday() < 5

        # mật độ session theo khung giờ
        if (9 <= hour < 12 or 14 <= hour < 16) and is_workday:
            sessions_this_hour = int(SESSIONS_PER_HOUR_BASE * random.uniform(2.0, 4.0))
        elif WORK_START.hour <= hour < WORK_END.hour and is_workday:
            sessions_this_hour = int(SESSIONS_PER_HOUR_BASE * random.uniform(0.8, 1.5))
        else:
            sessions_this_hour = int(SESSIONS_PER_HOUR_BASE * random.uniform(0.1, 0.5))

        for _ in range(sessions_this_hour):
            # random phút + giây trong giờ đó
            sess_time = current_virtual_time + timedelta(
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
                microseconds=random.randint(0, 999_999)
            )

            actor_info, is_workday_flag, is_ot, is_off = choose_actor(sess_time)
            sessions.append((sess_time, actor_info, is_workday_flag, is_ot, is_off))

        current_virtual_time += timedelta(hours=1)

    sessions.sort(key=lambda x: x[0])
    return sessions

# ==============================================================================
# 7. FORMAT LOG THEO ĐÚNG STYLE general_log
# ==============================================================================

def format_event(ts_dt, conn_id, command, arg):
    # MySQL general_log dạng:
    # 2025-05-20T14:37:32.739035Z\t   10 Query\tSHOW VARIABLES LIKE 'general_log%'
    ts = datetime_to_log_ts(ts_dt)
    return f"{ts}\t{conn_id:5d} {command}\t{arg}"

def generate_log_lines():
    # tạo full lịch
    schedule = generate_schedule()
    connection_id_counter = 10  # giống ví dụ general_log bạn đưa
    all_events = []

    # sinh toàn bộ event và nhét vào all_events
    for sess_time, actor_info, is_workday_flag, is_ot, is_off in schedule:
        sess_events = build_session(
            actor_info,
            session_start_dt=sess_time,
            is_workday=is_workday_flag,
            is_overtime_period=is_ot,
            is_off_hours=is_off,
            connection_id=connection_id_counter
        )
        all_events.extend(sess_events)
        connection_id_counter += 1

    # sort theo timestamp để xen kẽ các connection khác nhau (như log thật)
    all_events.sort(key=lambda e: e[0])

    # chuyển thành list dòng text cuối cùng
    lines = []
    for (ts_dt, conn_id, command, argument) in all_events:
        lines.append(format_event(ts_dt, conn_id, command, argument))
    return lines

def write_general_log_file(path=OUTPUT_LOG_FILE):
    lines = generate_log_lines()
    with open(path, "w", encoding="utf-8") as f:
        # header y chang phong cách MySQL
        f.write("C:\\\\Program Files\\\\MySQL\\\\MySQL Server 8.0\\\\bin\\\\mysqld.exe, "
                "Version: 8.0.42 (MySQL Community Server - GPL). started with:\n")
        f.write("TCP Port: 3306, Named Pipe: MySQL\n")
        f.write("Time                 Id Command    Argument\n")
        for line in lines:
            f.write(line + "\n")
    return path, len(lines)

# ==============================================================================
# 8. MAIN
# ==============================================================================

if __name__ == "__main__":
    out_path, total_lines = write_general_log_file()
    print(f"[+] Đã tạo file log mô phỏng: {out_path}")
    print(f"[+] Tổng số dòng log: {total_lines}")
