# Vietnamese Medium-Sized Sales Company User Generator
import mysql.connector
from faker import Faker
import random
import json
import os
import unicodedata

# --- CẤU HÌNH ---
DB_CONFIG = {"host": "localhost", "port": 3306, "user": "root", "password": "root"}
USERS_CONFIG_FILE = "simulation/users_config.json"

# [NEW] Cấu hình việc tạo lại tên user
# True: Luôn random tên mới mỗi khi chạy (như code cũ)
# False: Cố gắng đọc lại file config cũ để giữ nguyên tên user. Nếu file chưa có thì mới tạo mới.
REGENERATE_USERS = False 

# Vietnamese Faker for authentic Vietnamese names
fake_vn = Faker('vi_VN')

# Common Vietnamese family names (họ)
VIETNAMESE_FAMILY_NAMES = [
    "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng",
    "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý", "Đinh", "Đào", "Lương", "Trương",
    "Tạ", "Quách", "Vương", "Lại", "Thái", "Cao", "Chu", "Triệu", "Lưu", "Tô",
    "Đoàn", "Hà", "Tăng", "Mạc", "Kiều", "Ông", "Đồng", "Quan", "Hứa", "Khương"
]

# Common Vietnamese middle names (tên đệm)
VIETNAMESE_MIDDLE_NAMES = {
    "male": ["Văn", "Đức", "Minh", "Quang", "Hữu", "Công", "Thành", "Xuân", "Thanh", "Tuấn"],
    "female": ["Thị", "Minh", "Thu", "Hồng", "Lan", "Mai", "Hương", "Linh", "Ngọc", "Phương"]
}

# Common Vietnamese given names (tên)
VIETNAMESE_GIVEN_NAMES = {
    "male": [
        "Nam", "Hùng", "Dũng", "Tuấn", "Hải", "Long", "Quang", "Minh", "Đức", "Thành",
        "Hoàng", "Khang", "Phong", "Tùng", "Việt", "Bảo", "Khánh", "Tân", "Hưng", "Thắng",
        "Cường", "Sơn", "Tú", "Hiếu", "Trung", "Kiên", "Lâm", "Phúc", "An", "Đạt"
    ],
    "female": [
        "Linh", "Hương", "Lan", "Mai", "Thu", "Hà", "Nga", "Hoa", "Trang", "Nhung",
        "Thảo", "Yến", "Oanh", "Dung", "Hạnh", "Tâm", "Châu", "Vân", "Xuân", "Diệu",
        "Phương", "Ngọc", "Hồng", "Bích", "Thúy", "Giang", "Ly", "My", "Anh", "Huệ"
    ]
}

def remove_vietnamese_accents(input_str):
    vietnamese_map = {
        'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
        'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
        'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
        'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
        'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
        'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
        'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
        'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
        'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
        'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
        'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
        'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
        'đ': 'd', 'Đ': 'D'
    }
    result = input_str.lower()
    for vn_char, latin_char in vietnamese_map.items():
        result = result.replace(vn_char, latin_char)
    clean_parts = []
    for part in result.split():
        clean_part = "".join(c for c in part if c.isalnum())
        if clean_part:
            clean_parts.append(clean_part)
    return "_".join(clean_parts)

def generate_vietnamese_name():
    gender = random.choice(["male", "female"])
    family_name = random.choice(VIETNAMESE_FAMILY_NAMES)
    middle_name = random.choice(VIETNAMESE_MIDDLE_NAMES[gender])
    given_name = random.choice(VIETNAMESE_GIVEN_NAMES[gender])
    full_name = f"{family_name} {middle_name} {given_name}"
    username = remove_vietnamese_accents(full_name)
    if len(username) > 30:
        short_name = f"{family_name} {given_name}"
        username = remove_vietnamese_accents(short_name)
    return username, full_name

def get_conn():
    return mysql.connector.connect(**DB_CONFIG, autocommit=True)

def setup_real_users():
    print("👤 CREATING VIETNAMESE MEDIUM-SIZED SALES COMPANY USERS & PERMISSIONS...")
    conn = get_conn()
    cur = conn.cursor()

    # Define Permissions Structure (Cấu hình quyền hạn)
    # Lưu ý: Permission vẫn được định nghĩa trong code để đảm bảo logic mới nhất luôn được áp dụng
    ROLE_PERMISSIONS_DEF = {
        "SALES": {
            "sales_db": ["SELECT", "INSERT", "UPDATE"],
            "marketing_db": ["SELECT", "INSERT", "UPDATE"],
            "support_db": ["SELECT", "INSERT", "UPDATE"],
            "description": "Nhân viên kinh doanh"
        },
        "MARKETING": {
            "sales_db": ["SELECT"],
            "marketing_db": ["SELECT", "INSERT", "UPDATE", "DELETE"],
            "support_db": ["SELECT"],
            "description": "Nhân viên marketing"
        },
        "CUSTOMER_SERVICE": {
            "sales_db": ["SELECT"],
            "support_db": ["SELECT", "INSERT", "UPDATE"],
            "marketing_db": ["SELECT"],
            "description": "Nhân viên CSKH"
        },
        "HR": {
            "hr_db": ["SELECT", "INSERT", "UPDATE", "DELETE"],
            "finance_db": ["SELECT"],
            "admin_db": ["SELECT"],
            "description": "Nhân viên nhân sự"
        },
        "FINANCE": {
            "finance_db": ["SELECT", "INSERT", "UPDATE", "DELETE"],
            "sales_db": ["SELECT"],
            "hr_db": ["SELECT"],
            "inventory_db": ["SELECT"],
            "description": "Nhân viên tài chính"
        },
        "DEV": {
            "sales_db": ["SELECT", "INSERT", "UPDATE", "DELETE", "ALTER"],
            "hr_db": ["SELECT", "INSERT", "UPDATE", "DELETE", "ALTER"],
            "inventory_db": ["SELECT", "INSERT", "UPDATE", "DELETE", "ALTER"],
            "finance_db": ["SELECT", "INSERT", "UPDATE", "DELETE", "ALTER"],
            "marketing_db": ["SELECT", "INSERT", "UPDATE", "DELETE", "ALTER"],
            "support_db": ["SELECT", "INSERT", "UPDATE", "DELETE", "ALTER"],
            "admin_db": ["SELECT", "INSERT", "UPDATE", "DELETE", "ALTER"],
            "mysql": ["SELECT"],
            "description": "Nhân viên IT/Phát triển"
        },
        "MANAGEMENT": {
            "sales_db": ["SELECT", "INSERT", "UPDATE", "DELETE"],
            "hr_db": ["SELECT"],
            "finance_db": ["SELECT"],
            "marketing_db": ["SELECT", "INSERT", "UPDATE"],
            "support_db": ["SELECT"],
            "inventory_db": ["SELECT"],
            "admin_db": ["SELECT"],
            "description": "Quản lý"
        },
        "ADMIN": {
            "*": ["ALL"],
            "description": "Quản trị viên"
        },
        "BAD_ACTOR": {
            "sales_db": ["SELECT"],
            "marketing_db": ["SELECT"],
            "description": "Tài khoản rủi ro"
        },
        "VULNERABLE": {
            "sales_db": ["SELECT"],
            "description": "Tài khoản yếu"
        }
    }

    # 1. Xác định danh sách User (user_map)
    user_map = {}
    should_generate_new = True

    # Check logic: Nếu không muốn tạo mới VÀ file config tồn tại -> Load cũ
    if not REGENERATE_USERS and os.path.exists(USERS_CONFIG_FILE):
        try:
            print(f"📂 Đang tải danh sách user cũ từ {USERS_CONFIG_FILE}...")
            with open(USERS_CONFIG_FILE, 'r', encoding='utf-8') as f:
                old_config = json.load(f)
                if "users" in old_config:
                    user_map = old_config["users"]
                    should_generate_new = False
                    print(f"✅ Đã tải thành công {len(user_map)} users cũ.")
        except Exception as e:
            print(f"⚠️ Lỗi đọc file config cũ: {e}. Sẽ tiến hành tạo mới.")
            should_generate_new = True

    # Nếu cần tạo mới (do config = True hoặc không đọc được file cũ)
    if should_generate_new:
        print("🎲 Đang tạo ngẫu nhiên danh sách user mới...")
        teams = [
            ("SALES", 35), ("MARKETING", 12), ("CUSTOMER_SERVICE", 15),
            ("HR", 6), ("FINANCE", 8), ("DEV", 10), ("MANAGEMENT", 8), ("ADMIN", 3)
        ]
        
        # Generate Regular Users
        for role, count in teams:
            for i in range(count):
                username, full_name = generate_vietnamese_name()
                original_username = username
                counter = 1
                while username in user_map:
                    username = f"{original_username}{counter}"
                    counter += 1
                user_map[username] = role

        # Generate Bad Actors (Luôn đảm bảo có các user này)
        bad_actors = {
            "nguyen_noi_bo": "BAD_ACTOR", "thuc_tap_sinh": "VULNERABLE",
            "khach_truy_cap": "VULNERABLE", "dich_vu_he_thong": "VULNERABLE",
            "nhan_vien_tam": "VULNERABLE", "tu_van_ngoai": "BAD_ACTOR"
        }
        for u, role in bad_actors.items():
            user_map[u] = role


    # 2. Dọn dẹp Database (Xóa user cũ trên DB để tạo lại quyền cho chuẩn)
    print("🧹 Đang dọn dẹp user trên MySQL...")
    cur.execute("SELECT User, Host FROM mysql.user")
    all_existing_users = cur.fetchall()
    
    # Những user hệ thống không được xóa
    system_users = ['root', 'mysql.session', 'mysql.sys', 'mysql.infoschema', 'uba_user', 'debian-sys-maint']
    
    for u, h in all_existing_users:
        # Logic xóa: Chỉ xóa nếu user nằm trong danh sách user_map chúng ta quản lý
        # Hoặc nếu user có vẻ là user cũ (không phải system).
        # An toàn nhất: Xóa tất cả những ai KHÔNG PHẢI system user.
        if u not in system_users:
            try:
                cur.execute(f"DROP USER '{u}'@'{h}'")
            except:
                pass

    # 3. Thực thi tạo User vào MySQL (Dựa trên user_map đã có)
    print(f"🏗️ Đang tiến hành tạo/cấp quyền cho {len(user_map)} users...")
    
    for username, role in user_map.items():
        try:
            # Tạo user
            cur.execute(f"CREATE USER '{username}'@'%' IDENTIFIED BY 'password'")
            
            # Cấp quyền dựa trên ROLE_PERMISSIONS_DEF
            if role == "ADMIN":
                cur.execute(f"GRANT ALL PRIVILEGES ON *.* TO '{username}'@'%'")
            else:
                role_def = ROLE_PERMISSIONS_DEF.get(role, {})
                for db_name, permissions in role_def.items():
                    if db_name == "description": continue
                    
                    if db_name == "*":
                        cur.execute(f"GRANT ALL PRIVILEGES ON *.* TO '{username}'@'%'")
                    elif db_name == "mysql":
                        cur.execute(f"GRANT SELECT ON mysql.* TO '{username}'@'%'")
                    else:
                        if permissions:
                            perm_str = ", ".join(permissions)
                            cur.execute(f"GRANT {perm_str} ON {db_name}.* TO '{username}'@'%'")
            
            # Luôn cấp quyền USAGE
            cur.execute(f"GRANT USAGE ON *.* TO '{username}'@'%'")
            
        except Exception as e:
            print(f"⚠️ Lỗi tạo user {username}: {e}")

    cur.execute("FLUSH PRIVILEGES")
    conn.close()
    
    # 4. Lưu lại Config (Luôn lưu lại để dùng cho lần sau nếu cần)
    config_data = {
        "company_info": {
            "name": "Công ty TNHH Thương mại ABC",
            "type": "Vietnamese Medium-Sized Sales Company",
            "size": "80-120 employees",
            "industry": "Sales & Trading",
            "databases": 7
        },
        "roles": ROLE_PERMISSIONS_DEF,
        "users": user_map
    }
    
    with open(USERS_CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Hoàn tất! Config đã được lưu tại: {USERS_CONFIG_FILE}")
    print(f"ℹ️ Chế độ tạo mới user: {'BẬT (Random mới)' if REGENERATE_USERS else 'TẮT (Dùng lại tên cũ)'}")

if __name__ == "__main__":
    setup_real_users()