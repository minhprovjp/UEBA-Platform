import json, csv, random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# CẤU HÌNH
OUTPUT_FILE = "simulation/scenario_script_1day.csv"
QUERY_LIB = "simulation/query_library.json"
DAYS = 1
TOTAL_EVENTS = 100 # Số lượng dòng kịch bản (Tăng lên tùy thích)

USERS = {
    "SALES": [f"sale_user_{i}" for i in range(20)],
    "HR":    [f"hr_user_{i}" for i in range(5)],
    "DEV":   [f"dev_user_{i}" for i in range(10)],
    "ATTACKER": ["dave_insider", "unknown_ip"]
}

def load_queries():
    try:
        with open(QUERY_LIB, 'r') as f: return json.load(f)
    except: return {}

def fill_placeholders(q):
    # 1. Nhóm ID và Số lượng
    if "{id}" in q:         q = q.replace("{id}", str(random.randint(1, 1000)))
    if "{number}" in q:     q = q.replace("{number}", str(random.randint(1, 500)))
    if "{amount}" in q:     q = q.replace("{amount}", str(random.randint(1000, 20000))) # Lương/Tiền
    if "{bonus}" in q:      q = q.replace("{bonus}", str(random.randint(100, 5000)))    # Thưởng
    
    # 2. Nhóm Thông tin cá nhân (Dùng Faker)
    if "{name}" in q:       q = q.replace("{name}", fake.first_name())
    if "{city}" in q:       q = q.replace("{city}", fake.city())
    if "{position}" in q:   q = q.replace("{position}", random.choice(['Staff', 'Manager', 'Director', 'Intern', 'Engineer']))
    if "{department}" in q: q = q.replace("{department}", random.choice(['Sales', 'Marketing', 'HR', 'IT', 'Finance']))
    
    # 3. Nhóm Sản phẩm & Danh mục
    if "{category}" in q:   q = q.replace("{category}", f"'{random.choice(['Electronics', 'Books', 'Home', 'Fashion'])}'")
    if "{sku}" in q:        q = q.replace("{sku}", f"PROD-{random.randint(100,999)}")
    if "{quantity}" in q:   q = q.replace("{quantity}", str(random.randint(1, 50)))
    if "{price}" in q:      q = q.replace("{price}", str(random.randint(10, 1000)))

    # 4. Nhóm Thời gian
    if "{date}" in q:       q = q.replace("{date}", str(fake.date_this_year()))
    if "{year}" in q:       q = q.replace("{year}", "2025")
    if "{month}" in q:      q = q.replace("{month}", str(random.randint(1, 12)))
    if "{day}" in q:        q = q.replace("{day}", str(random.randint(1, 28)))

    # 5. Fix lỗi tên cột đặc thù của schema (Quan trọng)
    # AI hay viết 'id' cho bảng employees, nhưng schema thật là 'employee_id'
    q = q.replace("id FROM hr_db.employees", "employee_id FROM hr_db.employees")
    q = q.replace("WHERE id =", "WHERE employee_id =") # Sửa chung cho các bảng dùng employee_id
    
    return q

def generate_scenario():
    queries = load_queries()
    if not queries:
        print("❌ Chưa có file query_library.json. Hãy chạy Step 1 trước!")
        return

    scenario_data = []
    current_time = datetime.now() - timedelta(days=DAYS)
    
    print(f"📝 Đang viết kịch bản (STEP 2)...")
    
    for _ in range(TOTAL_EVENTS):
        # 1. Logic thời gian (Ngày nhanh, Đêm chậm)
        hour = current_time.hour
        is_business_hours = 8 <= hour <= 18
        
        if is_business_hours: step = random.randint(2, 60) # Giờ làm việc: log dày
        else: step = random.randint(300, 1200) # Đêm: log thưa
        
        current_time += timedelta(seconds=step)
        
        # 2. Logic Phân vai (Role) chuẩn doanh nghiệp
        is_attack = False
        role = "SALES" # Mặc định
        
        if not is_business_hours: # Ban đêm
            if random.random() < 0.1: # 10% là tấn công
                role = "ATTACKER"
                is_attack = True
            else:
                role = "DEV" # Dev hay OT đêm
        else: # Ban ngày
            role = random.choices(["SALES", "DEV", "HR"], weights=[60, 30, 10], k=1)[0]
            if random.random() < 0.005: # 0.5% tấn công ban ngày (Insider)
                role = "ATTACKER"
                is_attack = True

        # 3. Chọn User và Query
        user_list = USERS.get(role, USERS["SALES"])
        user = random.choice(user_list)
        
        # Chọn query đúng loại phòng ban
        raw_query = random.choice(queries.get(role, queries["SALES"]))
        final_query = fill_placeholders(raw_query)
        
        db_target = "hr_db" if role == "HR" else "sales_db"
        if role == "ATTACKER": db_target = random.choice(["sales_db", "hr_db"])

        scenario_data.append({
            "timestamp": current_time.isoformat() + "Z",
            "user": user,
            "database": db_target,
            "query": final_query,
            "is_anomaly": 1 if is_attack else 0
        })
        
    keys = scenario_data[0].keys()
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(scenario_data)
        
    print(f"✅ Kịch bản {TOTAL_EVENTS} dòng đã lưu: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_scenario()