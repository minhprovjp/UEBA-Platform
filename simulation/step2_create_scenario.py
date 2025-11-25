# simulation\step2_create_scenario.py
import json, csv, random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# CẤU HÌNH
OUTPUT_FILE = "simulation/scenario_script_10day.csv"
QUERY_LIB = "simulation/query_library.json"
DB_STATE_FILE = "simulation/db_state.json" 
DAYS = 10
TOTAL_EVENTS = 20000 # Tăng lên để mô hình học tốt hơn

# Load dữ liệu thật
try:
    with open(DB_STATE_FILE, 'r') as f: VALID_DATA = json.load(f)
except:
    print("⚠️ Cảnh báo: Không có db_state.json. Dùng dữ liệu giả.")
    VALID_DATA = {}

# DANH SÁCH USER (Phân loại rõ ràng)
USERS = {
    "SALES": [f"sale_user_{i}" for i in range(20)],
    "HR":    [f"hr_user_{i}" for i in range(5)],
    "DEV":   [f"dev_user_{i}" for i in range(10)],
    "ADMIN": ["admin_user"], # User đặc quyền
    "BAD_ACTOR": ["dave_insider", "unknown_ip"] # Kẻ xấu lộ mặt
}

# DANH SÁCH SQL INJECTION (Attack Pattern)
SQLI_PAYLOADS = [
    "' OR '1'='1", 
    "' UNION SELECT 1, user(), 3, 4 -- ", 
    "'; DROP TABLE users; --", 
    "' OR 1=1 LIMIT 1000 --"
]

def load_queries():
    try:
        with open(QUERY_LIB, 'r') as f: return json.load(f)
    except: return {}

def safe_replace(query, placeholder, value, is_string=False):
    if placeholder not in query: return query
    val_str = str(value)
    if is_string:
        # Kiểm tra xem đã có dấu nháy chưa để tránh double quotes
        if f"'{placeholder}'" in query: return query.replace(f"'{placeholder}'", f"'{val_str}'")
        elif f'"{placeholder}"' in query: return query.replace(f'"{placeholder}"', f"'{val_str}'")
        else: return query.replace(placeholder, f"'{val_str}'")
    else:
        return query.replace(placeholder, val_str)

def fill_placeholders(q):
    is_insert = "INSERT" in q.upper()
    
    if "{sku}" in q:
        val = f"NEW-{fake.unique.ean8()}" if is_insert else random.choice(VALID_DATA.get("product_skus", ["SKU-1"]))
        q = safe_replace(q, "{sku}", val, is_string=True)
        
    if "{email}" in q:
        val = fake.unique.email() if is_insert else "exist@example.com"
        q = safe_replace(q, "{email}", val, is_string=True)
    
    # Lấy dữ liệu ID thật
    cust_ids = VALID_DATA.get("customer_ids", [1])
    prod_ids = VALID_DATA.get("product_ids", [1])
    emp_ids  = VALID_DATA.get("employee_ids", [1])
    dept_ids = VALID_DATA.get("dept_ids", [1])
    camp_ids = VALID_DATA.get("campaign_ids", [1])
#    skus     = VALID_DATA.get("product_skus", ["SKU-001"])
    
    # 1. ID & SỐ
    q = safe_replace(q, "{customer_id}", random.choice(cust_ids))
    q = safe_replace(q, "{product_id}", random.choice(prod_ids))
    q = safe_replace(q, "{employee_id}", random.choice(emp_ids))
    q = safe_replace(q, "{dept_id}", random.choice(dept_ids))
    q = safe_replace(q, "{campaign_id}", random.choice(camp_ids))
    
    if "{status}" in q:
        opts = ['Running', 'Ended'] if "marketing" in q.lower() else ['Pending', 'Shipped']
        q = safe_replace(q, "{status}", random.choice(opts), is_string=True)
    if "{order_id}" in q: q = safe_replace(q, "{order_id}", random.randint(1, 5000))
    if "{id}" in q:
        if "product" in q: val = random.choice(prod_ids)
        elif "employee" in q: val = random.choice(emp_ids)
        else: val = random.choice(cust_ids)
        q = safe_replace(q, "{id}", val)

    for key in ["{amount}", "{number}", "{quantity}", "{price}", "{bonus}", "{rating}"]:
        q = safe_replace(q, key, random.randint(1, 1000))

    # 2. CHUỖI & NGÀY
#    q = safe_replace(q, "{sku}", random.choice(skus), is_string=True)
    q = safe_replace(q, "{city}", fake.city(), is_string=True)
    q = safe_replace(q, "{category}", random.choice(['Electronics', 'Books']), is_string=True)
    q = safe_replace(q, "{name}", fake.name(), is_string=True)
#    q = safe_replace(q, "{email}", fake.email(), is_string=True)
    q = safe_replace(q, "{date}", str(fake.date_this_year()), is_string=True)
#    q = safe_replace(q, "{status}", random.choice(['Pending','Shipped']), is_string=True)
    q = safe_replace(q, "{position}", fake.job(), is_string=True)
    q = safe_replace(q, "{department}", random.choice(['Sales', 'HR']), is_string=True)

    # Fix lỗi tên cột
    q = q.replace("id FROM hr_db.employees", "employee_id FROM hr_db.employees")
    q = q.replace("WHERE id =", "WHERE employee_id =")
    return q

def generate_complex_scenario():
    queries = load_queries()
    if not queries: return

    scenario_data = []
    current_time = datetime.now() - timedelta(days=DAYS)
    
    print(f"📝 ĐANG VIẾT KỊCH BẢN UEBA ({TOTAL_EVENTS} dòng)...")
    print("   -> Bao gồm: Compromised Account, Lateral Movement, Data Exfiltration, SQLi")
    
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
        
        # --- 2. XÁC ĐỊNH LOẠI HÀNH VI (Bình thường vs Tấn công) ---
        # Mặc định là bình thường
        behavior = "NORMAL"
        
        # Roll xúc xắc để xem có biến cố không (Tỷ lệ thấp ~2%)
        dice = random.random()
        
        if dice < 0.005: behavior = "COMPROMISED_ACCOUNT" # Tài khoản bị hack (0.5%)
        elif dice < 0.010: behavior = "LATERAL_MOVEMENT"  # Đi lạc phòng (0.5%)
        elif dice < 0.015: behavior = "DATA_EXFILTRATION" # Rút dữ liệu (0.5%)
        elif dice < 0.018: behavior = "SQL_INJECTION"     # Tiêm mã độc (0.3%)
        elif dice < 0.020: behavior = "INSIDER_THREAT"    # Dave phá hoại (0.2%)
        
        # --- 3. XÂY DỰNG KỊCH BẢN CHI TIẾT ---
        user = ""
        query = ""
        db_target = ""
        is_anomaly = 0
        
        if behavior == "NORMAL":
            # Logic bình thường: Ai làm việc nấy
            if is_work_hour and not is_weekend:
                role = random.choices(["SALES", "DEV", "HR"], weights=[60, 30, 10], k=1)[0]
            else:
                role = "DEV" if random.random() < 0.8 else "SALES" # Trực đêm
                
            user = random.choice(USERS[role])
            db_target = "hr_db" if role == "HR" else "sales_db"
            raw_query = random.choice(queries.get(role, queries["SALES"]))
            query = fill_placeholders(raw_query)
            is_anomaly = 0

        elif behavior == "COMPROMISED_ACCOUNT":
            # Kịch bản: User bình thường (HR/Sales) đăng nhập giờ lạ (3h sáng) làm việc nhạy cảm
            # Ép thời gian thành đêm khuya giả tạo cho dòng này (hoặc giữ nguyên nếu đang là đêm)
            if is_work_hour: 
                # Hack giờ: lùi lại đêm hôm qua hoặc chờ đêm nay (nhưng đơn giản là cứ log vào giờ hiện tại coi như hack ban ngày)
                pass 
            
            victim_role = random.choice(["HR", "SALES"]) # Nạn nhân
            user = random.choice(USERS[victim_role])
            
            # Hacker dùng nick HR để xem bảng lương hoặc User hệ thống
            raw_query = random.choice(queries.get("ATTACK", ["SELECT * FROM mysql.user"]))
            query = fill_placeholders(raw_query)
            
            # DB target tùy thuộc query tấn công
            db_target = "hr_db" if "hr_db" in query else "sales_db"
            is_anomaly = 1

        elif behavior == "LATERAL_MOVEMENT":
            # Kịch bản: Sales tò mò sang HR
            user = random.choice(USERS["SALES"])
            db_target = "hr_db" # <--- ĐIỂM BẤT THƯỜNG
            
            # Sales chạy query của HR
            raw_query = random.choice(queries.get("HR", ["SELECT * FROM employees"]))
            query = fill_placeholders(raw_query)
            is_anomaly = 1

        elif behavior == "DATA_EXFILTRATION":
            # Kịch bản: Dev hoặc Sales dump dữ liệu lớn
            user = random.choice(USERS["DEV"] + USERS["SALES"])
            db_target = "sales_db"
            
            # Query không có LIMIT hoặc SELECT * bảng lớn
            table = random.choice(["customers", "orders", "order_items"])
            query = f"SELECT * FROM {db_target}.{table}" # Không limit -> Trả về hàng nghìn dòng
            
            # Hoặc dùng OUTFILE
            if random.random() < 0.5:
                query += f" INTO OUTFILE '/tmp/leak_{random.randint(1000,9999)}.csv'"
            
            is_anomaly = 1

        elif behavior == "SQL_INJECTION":
            # Kịch bản: Web App bị tấn công (User bất kỳ hoặc unknown)
            user = random.choice(USERS["SALES"] + USERS["BAD_ACTOR"])
            db_target = "sales_db"
            
            # Lấy query bình thường và tiêm thuốc độc
            base_query = "SELECT * FROM sales_db.customers WHERE name = '{name}'"
            payload = random.choice(SQLI_PAYLOADS)
            
            # Thay {name} bằng payload
            query = base_query.replace("{name}", f"Admin{payload}")
            is_anomaly = 1

        elif behavior == "INSIDER_THREAT":
            # Kịch bản cũ: Dave hoặc Unknown phá hoại
            user = random.choice(USERS["BAD_ACTOR"])
            db_target = random.choice(["hr_db", "sales_db"])
            raw_query = random.choice(queries.get("ATTACK", ["DROP TABLE users"]))
            query = fill_placeholders(raw_query)
            is_anomaly = 1

        # Ghi dữ liệu
        scenario_data.append({
            "timestamp": current_time.isoformat() + "Z",
            "user": user,
            "database": db_target,
            "query": query,
            "is_anomaly": is_anomaly,
            "behavior_type": behavior # Thêm cột này để dễ debug/label
        })
        count += 1
        
    # Lưu file
    keys = list(scenario_data[0].keys())
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(scenario_data)
    print(f"✅ Kịch bản hoàn tất: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_complex_scenario()