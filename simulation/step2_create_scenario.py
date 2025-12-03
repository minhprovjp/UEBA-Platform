# simulation/step2_create_scenario.py
import json, csv, random, uuid, sys
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# CẤU HÌNH
OUTPUT_FILE = "simulation/scenario_script_10day.csv"
QUERY_LIB = "simulation/query_library.json"
DB_STATE_FILE = "simulation/db_state.json"
USERS_CONFIG_FILE = "simulation/users_config.json" 
DAYS = 10
TOTAL_EVENTS = 20000 

try:
    with open(DB_STATE_FILE, 'r') as f: VALID_DATA = json.load(f)
except: VALID_DATA = {}

# --- LOAD USER & QUYỀN TỪ CONFIG ---
USERS_MAP = {}  # Chi tiết quyền từng user: {'user1': {'role': 'SALES', 'permissions': ...}}
ROLE_RULES = {} # Luật lệ của từng Role: {'SALES': {'sales_db': [...]}}
USER_GROUPS = {} # Danh sách user theo nhóm: {'SALES': ['user1', 'user2'], 'DEV': [...]}

try:
    with open(USERS_CONFIG_FILE, 'r') as f:
        config_data = json.load(f)
        # 1. Load Role Rules
        ROLE_RULES = config_data.get("roles", {})
        
        # 2. Load User Map và tự động Grouping
        raw_users = config_data.get("users", {})
        
        # Khởi tạo các nhóm rỗng để tránh KeyError sau này
        for role in ROLE_RULES.keys():
            USER_GROUPS[role] = []
        USER_GROUPS["EXTERNAL_HACKER"] = ["unknown_ip", "script_kiddie", "apt_group_x"]
        
        # Duyệt qua từng user trong file json để phân loại
        for username, role_name in raw_users.items():
            # Lưu vào USER_MAP để check quyền sau này
            # Lưu ý: File json của bạn chỉ lưu string "DEV", không lưu chi tiết permissions từng user
            # Nên ta sẽ map permissions từ ROLE_RULES vào đây
            USERS_MAP[username] = {
                "role": role_name,
                "permissions": ROLE_RULES.get(role_name, {})
            }
            
            # Lưu vào USER_GROUPS
            if role_name not in USER_GROUPS:
                USER_GROUPS[role_name] = []
            USER_GROUPS[role_name].append(username)
            
        # Tạo nhóm tổng hợp Bad Actors
        USER_GROUPS["ALL_BAD"] = USER_GROUPS.get("BAD_ACTOR", []) + USER_GROUPS["EXTERNAL_HACKER"]
        
        print(f"✅ Đã load Config User: {len(USERS_MAP)} users.")
        print(f"   - Sales: {len(USER_GROUPS.get('SALES', []))}")
        print(f"   - HR: {len(USER_GROUPS.get('HR', []))}")
        print(f"   - Dev: {len(USER_GROUPS.get('DEV', []))}")
        
except Exception as e:
    print(f"❌ Lỗi đọc file {USERS_CONFIG_FILE}: {e}")
    sys.exit(1)

# IP Generator
def generate_fake_ip(user):
    if user in USER_GROUPS["EXTERNAL_HACKER"]:
        return f"10.0.{random.randint(1,254)}.{random.randint(1,254)}"
    # Dave Insider dùng IP nội bộ nhưng khác dải
    if user == "dave_insider":
        return f"192.168.100.{random.randint(1,254)}"
    return f"192.168.1.{hash(user) % 250 + 1}"

# Port Generator
def generate_fake_port(behavior):
    if behavior == "SCANNING": return random.randint(10000, 65000)
    return random.randint(10000, 60000)

# KHO VŨ KHÍ
ATTACK_PAYLOADS = {
    "SQLI_CLASSIC": [
        "' OR '1'='1", 
        "' UNION SELECT 1, user(), 3, 4 -- ", 
        "'; DROP TABLE customers; --",
        "' OR 1=1 LIMIT 1000 --"
    ],
    "SQLI_BLIND": [
        "' AND SLEEP(0.1) --",
        "'; SELECT BENCHMARK(100000,MD5(1)) --",
        #"' OR IF(1=1, SLEEP(5), 0) --",
        #"1' AND (SELECT 1 FROM (SELECT(SLEEP(5)))a) --"
    ],
    "RECON": [
        "SELECT version()",
        "SELECT user()",
        "SELECT @@hostname",
        "SELECT table_name FROM information_schema.tables",
        "SELECT column_name FROM information_schema.columns WHERE table_schema='hr_db'",
        "SHOW GRANTS FOR CURRENT_USER()",
        "SELECT host, user, authentication_string FROM mysql.user"
    ],
    "PRIV_ESC": [
        "GRANT ALL PRIVILEGES ON *.* TO 'dave_insider'@'%' WITH GRANT OPTION",
        "UPDATE mysql.user SET Select_priv='Y', Insert_priv='Y', Update_priv='Y' WHERE User='sale_user_1'",
        "SET GLOBAL read_only = 0",
        "CREATE USER 'backdoor_admin'@'%' IDENTIFIED BY 'pwned'"
    ],
    "DOS": [
        "SELECT * FROM orders t1, orders t2 LIMIT 10000", # Cartesian Product chết người
        #"SELECT * FROM sales_db.order_items WHERE quantity > 0 ORDER BY RAND()", # Sort random bảng lớn
        "SELECT BENCHMARK(500000, SHA1('test'))"
    ],
    "PERSISTENCE": [
        "CREATE TRIGGER stolen_cards BEFORE INSERT ON orders FOR EACH ROW INSERT INTO hack_log VALUES (NEW.order_id, NEW.total_amount)",
        "CREATE EVENT stealer ON SCHEDULE EVERY 1 MINUTE DO SELECT * FROM hr_db.salaries INTO OUTFILE '/tmp/passwords.txt'"
    ]
}

def load_queries():
    try:
        with open(QUERY_LIB, 'r') as f: return json.load(f)
    except: return {}

def safe_replace(query, placeholder, value, is_string=False):
    if placeholder not in query: return query
    val_str = str(value)
    if is_string:
        if f"'{placeholder}'" in query: return query.replace(f"'{placeholder}'", f"'{val_str}'")
        elif f'"{placeholder}"' in query: return query.replace(f'"{placeholder}"', f"'{val_str}'")
        else: return query.replace(placeholder, f"'{val_str}'")
    else:
        return query.replace(placeholder, val_str)

def sanitize_query(q):
    """Hàm sửa lỗi Operational Noise tự động"""
    # 1. Sửa lỗi join sai cột (Operational Noise a)
    q = q.replace("c.id", "c.customer_id")
    q = q.replace("customers.id", "customers.customer_id")
    
    # 2. Sửa lỗi thiếu tên DB (Operational Noise b)
    # Thêm prefix hr_db. cho các bảng nhân sự nếu thiếu
    for tbl in ["employees", "salaries", "departments", "attendance"]:
        # Regex đơn giản: khoảng trắng + tên bảng -> thêm prefix
        if f" {tbl}" in q and f"hr_db.{tbl}" not in q:
            q = q.replace(f" {tbl}", f" hr_db.{tbl}")
    
    # Xử lý lỗi double prefix nếu lỡ thay thừa
    q = q.replace("hr_db.hr_db.", "hr_db.")
    q = q.replace("sales_db.sales_db.", "sales_db.")
        
    # Xóa dấu chấm phẩy cuối câu (Connector thường tự xử lý, để lại có thể gây lỗi với một số driver)
    q = q.strip().rstrip(';') 
    return q

def fill_placeholders(q):
    """Điền dữ liệu giả khớp với DB thật vào query"""
    is_insert = "INSERT" in q.upper()
    
    # --- 1. LẤY DATA TỪ DB_STATE ---
    cust_ids = VALID_DATA.get("customer_ids", [1])
    prod_ids = VALID_DATA.get("product_ids", [1])
    emp_ids  = VALID_DATA.get("employee_ids", [1])
    dept_ids = VALID_DATA.get("dept_ids", [1])
    camp_ids = VALID_DATA.get("campaign_ids", [1])
    cities   = VALID_DATA.get("cities", ["Hanoi"])
    cats     = VALID_DATA.get("product_categories", ["Electronics"])
    skus     = VALID_DATA.get("product_skus", ["SKU-001"])

    # --- 2. XỬ LÝ CÁC PLACEHOLDER PHỨC TẠP ---

    # {product_ids}: Dùng cho câu lệnh IN (...)
    if "{product_ids}" in q:
        # Chọn ngẫu nhiên 3-5 ID
        selected = random.sample(prod_ids, k=min(len(prod_ids), random.randint(3, 5)))
        selected_str = ", ".join(map(str, selected))
        q = q.replace("{product_ids}", selected_str)

    # {segment}: Khớp với setup_full_environment.py
    if "{segment}" in q:
        # Setup định nghĩa: ['Retail','Wholesale','VIP']
        segments = ['Retail', 'Wholesale', 'VIP']
        q = safe_replace(q, "{segment}", random.choice(segments), is_string=True)

    # {location} / {warehouse_location}: Khớp với setup
    if "{location}" in q or "{warehouse_location}" in q:
        # Setup dùng: Zone-A, Zone-B, Zone-C
        locs = [f"Zone-{x}" for x in ['A', 'B', 'C', 'D', 'E']]
        val = random.choice(locs)
        q = safe_replace(q, "{location}", val, is_string=True)
        q = safe_replace(q, "{warehouse_location}", val, is_string=True)

    # {status}: Tùy ngữ cảnh
    if "{status}" in q:
        q_lower = q.lower()
        if "marketing" in q_lower or "campaign" in q_lower:
            opts = ['Running', 'Ended', 'Planned', 'Paused']
        elif "attendance" in q_lower:
            opts = ['Present', 'Absent', 'Late', 'Leave']
        else: # Orders
            opts = ['Completed', 'Pending', 'Cancelled', 'Processing']
        q = safe_replace(q, "{status}", random.choice(opts), is_string=True)

    # {type}: Marketing Campaign Type
    if "{type}" in q:
        types = ['Social Media', 'Email', 'TV', 'Web', 'Search']
        q = safe_replace(q, "{type}", random.choice(types), is_string=True)

    # --- 3. XỬ LÝ CÁC ID CỤ THỂ ---
    q = safe_replace(q, "{customer_id}", random.choice(cust_ids))
    q = safe_replace(q, "{product_id}", random.choice(prod_ids))
    q = safe_replace(q, "{employee_id}", random.choice(emp_ids))
    q = safe_replace(q, "{dept_id}", random.choice(dept_ids))
    q = safe_replace(q, "{campaign_id}", random.choice(camp_ids))
    
    # {id} chung chung: Cần đoán xem nó là ID của cái gì
    if "{id}" in q:
        if "product" in q.lower() or "inventory" in q.lower(): val = random.choice(prod_ids)
        elif "employee" in q.lower() or "salary" in q.lower(): val = random.choice(emp_ids)
        elif "campaign" in q.lower(): val = random.choice(camp_ids)
        elif "dept" in q.lower(): val = random.choice(dept_ids)
        else: val = random.choice(cust_ids)
        q = safe_replace(q, "{id}", val)

    # Các ID phụ
    if "{order_id}" in q: q = safe_replace(q, "{order_id}", random.randint(1, 20000))
    if "{review_id}" in q: q = safe_replace(q, "{review_id}", random.randint(1, 5000))
    if "{item_id}" in q: q = safe_replace(q, "{item_id}", random.randint(1, 50000))
    if "{salary_id}" in q: q = safe_replace(q, "{salary_id}", random.randint(1, 200))
    if "{record_id}" in q: q = safe_replace(q, "{record_id}", random.randint(1, 1000))

    # --- 4. SỐ LIỆU & CHUỖI ---
    # Giá tiền
    if "{unit_price}" in q: q = safe_replace(q, "{unit_price}", round(random.uniform(10, 500), 2))
    if "{total_amount}" in q: q = safe_replace(q, "{total_amount}", round(random.uniform(50, 2000), 2))
    if "{budget}" in q: q = safe_replace(q, "{budget}", round(random.uniform(1000, 50000), 2))
    if "{salary}" in q: q = safe_replace(q, "{salary}", round(random.uniform(3000, 15000), 2))
    
    # Số lượng
    for key in ["{amount}", "{number}", "{quantity}", "{bonus}", "{rating}", "{stock_quantity}"]:
        if key == "{rating}": val = random.randint(1, 5)
        elif key == "{quantity}": val = random.randint(1, 10)
        else: val = random.randint(10, 1000)
        q = safe_replace(q, key, val)

    # Chuỗi ngẫu nhiên từ DB State hoặc Faker
    q = safe_replace(q, "{city}", random.choice(cities), is_string=True)
    q = safe_replace(q, "{category}", random.choice(cats), is_string=True)
    
    if "{sku}" in q:
        val = f"SKU-{fake.unique.ean8()}" if is_insert else random.choice(skus)
        q = safe_replace(q, "{sku}", val, is_string=True)

    q = safe_replace(q, "{supplier}", fake.company(), is_string=True)
    q = safe_replace(q, "{comment}", fake.sentence(), is_string=True)
    q = safe_replace(q, "{name}", fake.name(), is_string=True)
    q = safe_replace(q, "{email}", fake.email(), is_string=True)
    q = safe_replace(q, "{position}", fake.job(), is_string=True)
    q = safe_replace(q, "{payment_method}", random.choice(['Credit Card', 'PayPal']), is_string=True)

    # Ngày tháng
    q = safe_replace(q, "{date}", str(fake.date_this_year()), is_string=True)
    q = safe_replace(q, "{start_date}", "2025-01-01", is_string=True)
    q = safe_replace(q, "{end_date}", "2025-12-31", is_string=True)
    q = safe_replace(q, "{payment_date}", str(fake.date_this_month()), is_string=True)

    return sanitize_query(q)

def get_query_type(sql):
    sql = sql.strip().upper()
    if sql.startswith("SELECT"): return "SELECT"
    if sql.startswith("INSERT"): return "INSERT"
    if sql.startswith("UPDATE"): return "UPDATE"
    if sql.startswith("DELETE"): return "DELETE"
    return "UNKNOWN"

def is_query_allowed(username, db_target, query):
    """
    Kiểm tra xem user có quyền chạy lệnh này trên DB này không.
    """
    if username not in USERS_MAP: return True # Hacker (không có trong map) thì bỏ qua check
    
    user_info = USERS_MAP[username]
    perms = user_info.get("permissions", {})
    
    # 1. Check quyền Admin (*)
    if "*" in perms: return True
    
    # 2. Check DB
    if db_target not in perms: return False
    
    # 3. Check Command Type
    db_rights = perms.get(db_target, [])
    if "ALL" in db_rights or "ALL PRIVILEGES" in db_rights: return True
    
    cmd_type = get_query_type(query)
    if cmd_type in db_rights: return True
    
    return False

def generate_complex_scenario():
    queries = load_queries()
    if not queries: 
        print("❌ Không tìm thấy query_library.json. Hãy chạy Step 1 trước!")
        return

    scenario_data = []
    current_time = datetime.now() - timedelta(days=DAYS)
    
    print(f"📝 ĐANG VIẾT KỊCH BẢN UEBA ({TOTAL_EVENTS} dòng)...")
    
    count = 0
    while count < TOTAL_EVENTS:
        # --- 1. MÔ PHỎNG THỜI GIAN ---
        hour = current_time.hour
        weekday = current_time.weekday()
        is_weekend = weekday >= 5
        is_work_hour = (8 <= hour <= 18)
        
        # Tốc độ log
        if not is_weekend and is_work_hour: step = random.randint(2, 30)
        elif not is_weekend and 18 < hour <= 20: step = random.randint(30, 120) # OT
        else: step = random.randint(300, 900) # Đêm/Cuối tuần
        
        current_time += timedelta(seconds=step)
        timestamp_str = current_time.isoformat() + "Z"
        
        # --- 2. XÁC ĐỊNH LOẠI HÀNH VI (Bình thường vs Tấn công) ---
        # Mặc định là bình thường
        behavior = "NORMAL"
        
        # Roll xúc xắc để xem có biến cố không (Tỷ lệ thấp ~2%)
        dice = random.random()
        
        if dice < 0.005: behavior = "COMPROMISED_ACCOUNT"           # Tài khoản bị hack (0.5%)
        elif dice < 0.010: behavior = "LATERAL_MOVEMENT"            # Đi lạc phòng (0.5%)
        elif dice < 0.015: behavior = "DATA_EXFILTRATION"           # Rút dữ liệu (0.5%)
        elif dice < 0.018: behavior = "SQL_INJECTION_CLASSIC"       # Tiêm mã độc (0.3%)
        elif dice < 0.020: behavior = "INSIDER_THREAT"              # Dave phá hoại (0.2%)
        # elif dice < 0.0201: behavior = "SQL_INJECTION_BLIND"      # Blind SQLi
        elif dice < 0.022: behavior = "RECONNAISSANCE"              # Do thám
        elif dice < 0.023: behavior = "PRIVILEGE_ESCALATION"        # Leo thang
        # elif dice < 0.0253: behavior = "DOS_ATTEMPT"              # Tấn công từ chối dịch vụ
        # elif dice < 0.0254: behavior = "PERSISTENCE_BACKDOOR"     # Cài cắm backdoor
        
        # --- 3. XÂY DỰNG KỊCH BẢN CHI TIẾT ---
        user = ""
        query = ""
        db_target = ""
        is_anomaly = 0
        
        if behavior == "NORMAL":
            # Logic bình thường: Ai làm việc nấy
            if is_work_hour and not is_weekend:
                role = random.choices(["SALES", "DEV", "HR"], weights=[70, 20, 10], k=1)[0]
            else:
                role = "DEV" if random.random() < 0.8 else "SALES" # Trực đêm
                
            user = random.choice(USER_GROUPS[role])
            db_target = "hr_db" if role == "HR" else "sales_db"
            
            # Lấy query từ Library
            key = role # SALES, HR, DEV
            if key not in queries or not queries[key]: key = "SALES" # Fallback
            
            raw_query = random.choice(queries[key])
            query = fill_placeholders(raw_query)
            is_anomaly = 0

        elif behavior == "COMPROMISED_ACCOUNT":
            # Kịch bản: User bình thường (HR/Sales) đăng nhập giờ lạ (3h sáng) làm việc nhạy cảm
            # Ép thời gian thành đêm khuya giả tạo cho dòng này (hoặc giữ nguyên nếu đang là đêm)
            if is_work_hour: 
                # Hack giờ: lùi lại đêm hôm qua hoặc chờ đêm nay (nhưng đơn giản là cứ log vào giờ hiện tại coi như hack ban ngày)
                pass 
            
            victim_role = random.choice(["HR", "SALES"]) # Nạn nhân
            user = random.choice(USER_GROUPS[victim_role])
            
            # Hacker dùng nick HR để xem bảng lương hoặc User hệ thống
            raw_query = random.choice(queries.get("ATTACK", ["SELECT * FROM mysql.user"]))
            query = fill_placeholders(raw_query)
            
            # DB target tùy thuộc query tấn công
            db_target = "hr_db" if "hr_db" in query else "sales_db"
            is_anomaly = 1

        elif behavior == "LATERAL_MOVEMENT":
            # Kịch bản: Sales tò mò sang HR
            user = random.choice(USER_GROUPS["SALES"])
            db_target = "hr_db" # <--- ĐIỂM BẤT THƯỜNG
            
            # Sales chạy query của HR
            raw_query = random.choice(queries.get("HR", ["SELECT * FROM hr_db.employees"]))
            query = fill_placeholders(raw_query)
            is_anomaly = 1

        elif behavior == "DATA_EXFILTRATION":
            # Kịch bản: Dev hoặc Sales dump dữ liệu lớn
            user = random.choice(USER_GROUPS["DEV"] + USER_GROUPS["SALES"])
            db_target = "sales_db"
            
            # Query không có LIMIT hoặc SELECT * bảng lớn
            table = random.choice(["customers", "orders", "order_items"])
            query = f"SELECT * FROM sales_db.{table}" # Không limit -> Trả về hàng nghìn dòng
            
            # Hoặc dùng OUTFILE
            if random.random() < 0.5:
                query += f" INTO OUTFILE '/tmp/leak_{random.randint(1000,9999)}.csv'"
            
            is_anomaly = 1

        elif behavior == "SQL_INJECTION_CLASSIC":
            # Kịch bản: Web App bị tấn công (User bất kỳ hoặc unknown)
            user = random.choice(USER_GROUPS["SALES"] + USER_GROUPS["ALL_BAD"])
            db_target = "sales_db"
            
            # Lấy query bình thường và tiêm thuốc độc
            base_query = "SELECT * FROM sales_db.customers WHERE name = '{name}'"
            
            # Thay {name} bằng payload
            query = base_query.replace("{name}", f"Admin{random.choice(ATTACK_PAYLOADS['SQLI_CLASSIC'])}")
            is_anomaly = 1
            
        elif behavior == "SQL_INJECTION_BLIND": 
            user = random.choice(USER_GROUPS["ALL_BAD"])
            db_target = "sales_db"
            # Query có vẻ bình thường nhưng chứa SLEEP
            base = "SELECT * FROM products WHERE id = {id}"
            payload = random.choice(ATTACK_PAYLOADS["SQLI_BLIND"])
            query = base.replace("{id}", f"105 {payload}")
            is_anomaly = 1

        elif behavior == "RECONNAISSANCE":
            # Hacker dò quét thông tin
            user = random.choice(USER_GROUPS["ALL_BAD"] + USER_GROUPS["DEV"]) # Dev tò mò hoặc Hacker
            db_target = "information_schema"
            query = random.choice(ATTACK_PAYLOADS["RECON"])
            is_anomaly = 1

        elif behavior == "PRIVILEGE_ESCALATION":
            user = random.choice(USER_GROUPS["ALL_BAD"]) # Dave cố gắng chiếm quyền
            db_target = "mysql"
            query = random.choice(ATTACK_PAYLOADS["PRIV_ESC"])
            is_anomaly = 1

        elif behavior == "DOS_ATTEMPT": 
            user = random.choice(USER_GROUPS["ALL_BAD"])
            db_target = "sales_db"
            query = random.choice(ATTACK_PAYLOADS["DOS"])
            is_anomaly = 1

        elif behavior == "PERSISTENCE_BACKDOOR": 
            user = "dave_insider"
            db_target = "sales_db"
            query = random.choice(ATTACK_PAYLOADS["PERSISTENCE"])
            is_anomaly = 1

        elif behavior == "INSIDER_THREAT":
            # Kịch bản: Dave hoặc Unknown phá hoại
            user = random.choice(USER_GROUPS["ALL_BAD"])
            db_target = random.choice(["hr_db", "sales_db"])
            raw_query = random.choice(queries.get("ATTACK", ["DROP TABLE customers"]))
            query = fill_placeholders(raw_query)
            is_anomaly = 1

        # --- 4. TẠO TAG ---
        sim_ip = generate_fake_ip(user)
        sim_port = generate_fake_port(behavior)
        sim_id = uuid.uuid4().hex[:8]
        
        # Tag đầy đủ: User, IP, Port, ID, Behavior, Anomaly, Timestamp
        tag = f"/* SIM_META:{user}|{sim_ip}|{sim_port}|ID:{sim_id}|BEH:{behavior}|ANO:{is_anomaly}|TS:{timestamp_str} */"
        
        # Gắn tag vào query luôn
        final_query_with_tag = f"{tag} {query}"

        # --- 5. Ghi ra CSV ---
        scenario_data.append({
            "timestamp": timestamp_str,
            "user": user,
            "database": db_target,
            "query": final_query_with_tag, # Query đã có tag
            "is_anomaly": is_anomaly,
            "behavior_type": behavior
        })
        count += 1
        if count % 2000 == 0: sys.stdout.write(f"\r⚡ Tiến độ: {count}/{TOTAL_EVENTS}...")
        
    keys = list(scenario_data[0].keys())
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(scenario_data)
    print(f"✅ Kịch bản hoàn tất: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_complex_scenario()