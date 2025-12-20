# Vietnamese Medium-Sized Sales Company User Generator
import mysql.connector
from faker import Faker
import random
import json
import os
import unicodedata

# CẤU HÌNH
DB_CONFIG = {"host": "localhost", "port": 3306, "user": "root", "password": "root"}
USERS_CONFIG_FILE = "simulation/users_config.json"

# Vietnamese Faker for authentic Vietnamese names
fake_vn = Faker('vi_VN')

# Common Vietnamese family names (họ) - researched from Vietnamese demographics
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
    """
    Convert Vietnamese names to clean username format
    'Nguyễn Văn Nam' -> 'nguyen_van_nam'
    """
    # Vietnamese accent mapping for proper conversion
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
    
    # Convert to lowercase and replace Vietnamese characters
    result = input_str.lower()
    for vn_char, latin_char in vietnamese_map.items():
        result = result.replace(vn_char, latin_char)
    
    # Remove any remaining special characters and join with underscores
    clean_parts = []
    for part in result.split():
        clean_part = "".join(c for c in part if c.isalnum())
        if clean_part:
            clean_parts.append(clean_part)
    
    return "_".join(clean_parts)

def generate_vietnamese_name():
    """
    Generate authentic Vietnamese names for medium-sized company
    Returns username and full Vietnamese name
    """
    # Randomly choose gender for appropriate middle and given names
    gender = random.choice(["male", "female"])
    
    # Select name components
    family_name = random.choice(VIETNAMESE_FAMILY_NAMES)
    middle_name = random.choice(VIETNAMESE_MIDDLE_NAMES[gender])
    given_name = random.choice(VIETNAMESE_GIVEN_NAMES[gender])
    
    # Create full Vietnamese name (Họ Tên_đệm Tên)
    full_name = f"{family_name} {middle_name} {given_name}"
    
    # Create username (ho.tendem.ten)
    username = remove_vietnamese_accents(full_name)
    
    # Ensure reasonable length for MySQL (32 char limit)
    if len(username) > 30:
        # Use family name + given name only
        short_name = f"{family_name} {given_name}"
        username = remove_vietnamese_accents(short_name)
    
    return username, full_name

def get_conn():
    return mysql.connector.connect(**DB_CONFIG, autocommit=True)

def setup_real_users():
    print("👤 CREATING VIETNAMESE MEDIUM-SIZED SALES COMPANY USERS & PERMISSIONS...")
    conn = get_conn()
    cur = conn.cursor()

    # Danh sách các user đặc biệt cần xóa (hardcoded users)
    special_users = [
        "nguyen_noi_bo", "thuc_tap_sinh", "khach_truy_cap", 
        "dich_vu_he_thong", "nhan_vien_tam", "tu_van_ngoai"
    ]

    # 1. Xóa user cũ (bao gồm user tự sinh có đuôi _user, user hiện tại trong list config, và các user đặc biệt)
    print("🧹 Đang dọn dẹp user cũ...")
    
    # Lấy danh sách tất cả user trong DB để kiểm tra
    cur.execute("SELECT User, Host FROM mysql.user")
    all_existing_users = cur.fetchall()

    for u, h in all_existing_users:
        # Điều kiện xóa:
        # 1. User có trong danh sách đặc biệt (special_users)
        # 2. HOẶC User có vẻ là user được sinh ra (bạn có thể điều chỉnh logic này nếu muốn)
        # Lưu ý: Code cũ của bạn lọc theo '%_user%', nhưng user mới sinh ra lại theo tên (vd: nguyen_van_a).
        # Tốt nhất là xóa tất cả ngoại trừ các user hệ thống quan trọng.
        
        is_system_user = u in ['root', 'mysql.session', 'mysql.sys', 'mysql.infoschema', 'uba_user']
        
        # Nếu user nằm trong danh sách đặc biệt HOẶC không phải system user (để reset lại toàn bộ nhân viên)
        if (u in special_users or not is_system_user):
            try:
                cur.execute(f"DROP USER '{u}'@'{h}'")
                print(f"   Deleted old user: {u}")
            except Exception as e:
                # Bỏ qua nếu lỗi (ví dụ user đang login)
                pass

    # 2. Define Vietnamese medium-sized sales company structure (80-120 employees)
    # Enhanced structure with 7-database access
    teams = [
        ("SALES", 35),                      # Sales team - largest department
        ("MARKETING", 12),                  # Marketing team - campaigns and leads
        ("CUSTOMER_SERVICE", 15),           # Customer service - support tickets
        ("HR", 6),                         # HR team - employee management
        ("FINANCE", 8),                     # Finance team - accounting and budgets
        ("DEV", 10),                       # IT/Development team - system maintenance
        ("MANAGEMENT", 8),                  # Management - cross-department oversight
        ("ADMIN", 3)                       # System administrators - full access
    ]

    user_map = {} # username -> role
    
    for role, count in teams:
        print(f"🏢 Tạo {count} nhân viên cho phòng ban {role}...")
        for i in range(count):
            # Generate authentic Vietnamese name
            username, full_name = generate_vietnamese_name()
            
            # Ensure unique username
            original_username = username
            counter = 1
            while username in user_map:
                username = f"{original_username}{counter}"
                counter += 1
            
            user_map[username] = role
            print(f"  ✅ Tạo: {username} ({full_name}) -> {role}")
            
            # Tạo MySQL User
            try:
                cur.execute(f"CREATE USER '{username}'@'%' IDENTIFIED BY 'password'")
                
                # Cấp quyền theo vai trò với enhanced database structure
                if role == "ADMIN":
                    cur.execute(f"GRANT ALL PRIVILEGES ON *.* TO '{username}'@'%'")
                else:
                    # Get enhanced permissions from config
                    with open(USERS_CONFIG_FILE, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                    
                    role_permissions = config.get("roles", {}).get(role, {})
                    
                    for db_name, permissions in role_permissions.items():
                        if db_name == "description":
                            continue
                        elif db_name == "*":
                            cur.execute(f"GRANT ALL PRIVILEGES ON *.* TO '{username}'@'%'")
                        elif db_name == "mysql":
                            cur.execute(f"GRANT SELECT ON mysql.* TO '{username}'@'%'")
                        else:
                            if permissions:
                                perm_str = ", ".join(permissions)
                                cur.execute(f"GRANT {perm_str} ON {db_name}.* TO '{username}'@'%'")
                    
                    # Always grant basic usage
                    cur.execute(f"GRANT USAGE ON *.* TO '{username}'@'%'")
            except Exception as e:
                print(f"⚠️ Lỗi tạo user {username}: {e}")

    # Create specific Vietnamese accounts for attack scenarios
    print("🔒 Tạo tài khoản đặc biệt cho kịch bản bảo mật...")
    bad_actors = {
        "nguyen_noi_bo": "BAD_ACTOR",           # Insider Threat (Vietnamese name)
        "thuc_tap_sinh": "VULNERABLE",          # Intern account (Vietnamese)
        "khach_truy_cap": "VULNERABLE",         # Guest access account (Vietnamese)
        "dich_vu_he_thong": "VULNERABLE",       # Service account (Vietnamese)
        "nhan_vien_tam": "VULNERABLE",          # Temporary employee (Vietnamese)
        "tu_van_ngoai": "BAD_ACTOR"             # External consultant (Vietnamese)
    }
    
    for u, role in bad_actors.items():
        try:
            cur.execute(f"CREATE USER '{u}'@'%' IDENTIFIED BY 'password'")
            cur.execute(f"GRANT SELECT ON sales_db.* TO '{u}'@'%'")
            user_map[u] = role
            print(f"  ✅ Tạo tài khoản đặc biệt: {u} -> {role}")
        except Exception as e:
            print(f"⚠️ Lỗi tạo tài khoản đặc biệt {u}: {e}")

    cur.execute("FLUSH PRIVILEGES")
    conn.close()
    
    # Save configuration with enhanced 7-database Vietnamese company role permissions
    config_data = {
        "company_info": {
            "name": "Công ty TNHH Thương mại ABC",
            "type": "Vietnamese Medium-Sized Sales Company",
            "size": "80-120 employees",
            "industry": "Sales & Trading",
            "databases": 7,
            "database_list": [
                "sales_db", "hr_db", "inventory_db", "finance_db", 
                "marketing_db", "support_db", "admin_db"
            ]
        },
        "roles": {
            "SALES": {
                "sales_db": ["SELECT", "INSERT", "UPDATE"],
                "marketing_db": ["SELECT", "INSERT", "UPDATE"],
                "support_db": ["SELECT", "INSERT", "UPDATE"],
                "description": "Nhân viên kinh doanh - truy cập bán hàng, marketing, hỗ trợ khách hàng"
            },
            "MARKETING": {
                "sales_db": ["SELECT"],
                "marketing_db": ["SELECT", "INSERT", "UPDATE", "DELETE"],
                "support_db": ["SELECT"],
                "description": "Nhân viên marketing - quản lý chiến dịch và leads"
            },
            "CUSTOMER_SERVICE": {
                "sales_db": ["SELECT"],
                "support_db": ["SELECT", "INSERT", "UPDATE"],
                "marketing_db": ["SELECT"],
                "description": "Nhân viên chăm sóc khách hàng - xử lý tickets và hỗ trợ"
            },
            "HR": {
                "hr_db": ["SELECT", "INSERT", "UPDATE", "DELETE"],
                "finance_db": ["SELECT"],
                "admin_db": ["SELECT"],
                "description": "Nhân viên nhân sự - quản lý nhân sự và lương"
            },
            "FINANCE": {
                "finance_db": ["SELECT", "INSERT", "UPDATE", "DELETE"],
                "sales_db": ["SELECT"],
                "hr_db": ["SELECT"],
                "inventory_db": ["SELECT"],
                "description": "Nhân viên tài chính - quản lý tài chính và kế toán"
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
                "description": "Nhân viên IT/Phát triển - truy cập toàn bộ hệ thống"
            },
            "MANAGEMENT": {
                "sales_db": ["SELECT", "INSERT", "UPDATE", "DELETE"],
                "hr_db": ["SELECT"],
                "finance_db": ["SELECT"],
                "marketing_db": ["SELECT", "INSERT", "UPDATE"],
                "support_db": ["SELECT"],
                "inventory_db": ["SELECT"],
                "admin_db": ["SELECT"],
                "description": "Quản lý cấp trung và cao - truy cập đa hệ thống"
            },
            "ADMIN": {
                "*": ["ALL"],
                "description": "Quản trị viên hệ thống - toàn quyền"
            },
            "BAD_ACTOR": {
                "sales_db": ["SELECT"],
                "marketing_db": ["SELECT"],
                "description": "Tài khoản có nguy cơ bảo mật"
            },
            "VULNERABLE": {
                "sales_db": ["SELECT"],
                "description": "Tài khoản dễ bị tấn công"
            }
        },
        "users": user_map
    }
    
    with open(USERS_CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Đã tạo {len(user_map)} nhân viên Việt Nam cho công ty quy mô trung bình. Cấu hình đã lưu.")
    print(f"📊 Phân bố nhân viên theo phòng ban:")
    role_counts = {}
    for username, role in user_map.items():
        role_counts[role] = role_counts.get(role, 0) + 1
    
    for role, count in role_counts.items():
        print(f"   {role}: {count} nhân viên")
    
    total_employees = sum(role_counts.values())
    print(f"🏢 Tổng số nhân viên: {total_employees} (quy mô công ty trung bình)")
    print(f"🇻🇳 Tên Việt Nam chính thống với các họ phổ biến nhất")
    print(f"🔐 Tất cả user được tạo với mật khẩu: 'password'")
    print(f"📁 Cấu hình đã lưu tại: {USERS_CONFIG_FILE}")
    
    # Validate medium-sized company criteria
    if 80 <= total_employees <= 200:
        print(f"✅ CONFIRMED: Đây là dataset cho công ty quy mô TRUNG BÌNH ({total_employees} nhân viên)")
    else:
        print(f"⚠️ WARNING: Số lượng nhân viên ({total_employees}) không phù hợp với quy mô trung bình (80-200)")

if __name__ == "__main__":
    setup_real_users()