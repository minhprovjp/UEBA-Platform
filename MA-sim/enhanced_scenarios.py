#!/usr/bin/env python3
"""
Enhanced Attack Scenarios for Vietnamese Medium-Sized Sales Company
Provides realistic attack patterns and insider threat scenarios
"""

import random
from datetime import datetime, timedelta

class EnhancedScenarioManager:
    """
    Enhanced scenario manager for Vietnamese business context with sophisticated attack patterns
    Includes rule-bypassing techniques and advanced evasion methods
    """
    
    def __init__(self, db_state=None):
        self.db_state = db_state or {}
        
        # Vietnamese business context
        self.vietnamese_departments = [
            "Phòng Kinh Doanh", "Phòng Marketing", "Phòng Chăm Sóc Khách Hàng",
            "Phòng Nhân Sự", "Phòng Tài Chính", "Phòng IT", "Ban Giám Đốc"
        ]
        
        # Common Vietnamese employee names for scenarios
        self.vietnamese_employees = [
            "nguyen_van_nam", "tran_thi_lan", "le_minh_duc", "pham_thi_mai",
            "hoang_van_son", "vu_thi_hoa", "dang_minh_tuan", "bui_thi_nga"
        ]
        
        # SYSTEM RULES ANALYSIS (for bypassing)
        self.system_rules = {
            "work_hours": {
                "weekdays_only": True,
                "hours": "8AM-6PM", 
                "lunch_break": "11:30AM-1:30PM",
                "vietnamese_holidays": ["2025-01-01", "2025-01-29", "2025-04-30", "2025-05-01", "2025-09-02"]
            },
            "role_access": {
                "SALES": ["sales_db", "marketing_db", "support_db"],
                "MARKETING": ["marketing_db", "sales_db", "support_db"],
                "CUSTOMER_SERVICE": ["support_db", "sales_db", "marketing_db"],
                "HR": ["hr_db", "finance_db", "admin_db"],
                "FINANCE": ["finance_db", "sales_db", "hr_db", "inventory_db"],
                "DEV": ["all_databases"],  # Full access
                "MANAGEMENT": ["sales_db", "hr_db", "finance_db", "marketing_db", "support_db", "inventory_db", "admin_db"],
                "ADMIN": ["all_databases"]  # Full access
            },
            "network_segmentation": {
                "sales": "192.168.10.x",
                "marketing": "192.168.15.x", 
                "hr": "192.168.20.x",
                "customer_service": "192.168.25.x",
                "finance": "192.168.30.x",
                "management": "192.168.40.x",
                "dev": "192.168.50.x",
                "admin": "192.168.60.x",
                "external_threats": "10.0.0.x"
            },
            "behavioral_patterns": {
                "lunch_activity": {"11:30-12:00": 0.4, "12:00-13:00": 0.2, "13:00-13:30": 0.3},
                "weekend_activity": {"normal_users": 0.0, "malicious": 0.05},
                "off_hours_activity": {"normal_users": 0.0, "malicious": 0.1}
            }
        }

    def get_scenario(self, scenario_name, target_user=None, **kwargs):
        """
        Get a predefined attack scenario
        
        Args:
            scenario_name: Name of the scenario to execute
            target_user: Specific user to use (optional)
            **kwargs: Additional scenario parameters
            
        Returns:
            List of intents to execute sequentially
        """
        
        scenarios = {
            # Original scenarios
            "INSIDER_SALARY_THEFT": self._insider_salary_theft,
            "EXTERNAL_HACK_ATTEMPT": self._external_hack_attempt,
            "SALES_SNOOPING": self._sales_snooping,
            "PRIVILEGE_ESCALATION": self._privilege_escalation,
            "DATA_EXFILTRATION": self._data_exfiltration,
            "LATERAL_MOVEMENT": self._lateral_movement,
            "FINANCIAL_FRAUD": self._financial_fraud,
            "CUSTOMER_DATA_BREACH": self._customer_data_breach,
            "SUPPLY_CHAIN_ATTACK": self._supply_chain_attack,
            "SOCIAL_ENGINEERING": self._social_engineering,
            
            # NEW: Rule-bypassing scenarios
            "WORK_HOURS_BYPASS": self._work_hours_bypass,
            "NETWORK_SEGMENTATION_BYPASS": self._network_segmentation_bypass,
            "ROLE_ESCALATION_CHAIN": self._role_escalation_chain,
            "LUNCH_BREAK_EXPLOITATION": self._lunch_break_exploitation,
            "HOLIDAY_BACKDOOR_ACCESS": self._holiday_backdoor_access,
            "CROSS_DEPARTMENT_IMPERSONATION": self._cross_department_impersonation,
            "LEGITIMATE_TOOL_ABUSE": self._legitimate_tool_abuse,
            "TIME_BASED_EVASION": self._time_based_evasion,
            "MULTI_STAGE_PERSISTENCE": self._multi_stage_persistence,
            "VIETNAMESE_CULTURAL_EXPLOITATION": self._vietnamese_cultural_exploitation,
            "ACCOUNT_TAKEOVER": self._account_takeover,
            "INSIDER_SABOTAGE": self._insider_sabotage
        }
        
        if scenario_name in scenarios:
            return scenarios[scenario_name](target_user, **kwargs)
        else:
            return []

    def _account_takeover(self, target_user=None, **kwargs):
        """
        Scenario: HR User bị lộ mật khẩu.
        Dấu hiệu: Login giờ lạ, IP lạ, nhưng user đúng. Sau đó làm hành động lạ.
        """
        # Chọn nạn nhân là HR hoặc Sales (Non-tech)
        victim = target_user or "bui.thi.nga" # Giả sử đây là HR
        
        return [
            # 1. Login từ IP lạ (Ví dụ IP nước ngoài hoặc IP dải VPN lạ)
            {
                "user": victim, "role": "HR", "action": "LOGIN", "params": {},
                "target_database": "hr_db", "is_anomaly": 1, 
                "description": "Đăng nhập thành công từ IP lạ (ATO)",
                "source_ip": "14.162.55.99" # IP dân dụng, không phải IP công ty
            },
            # 2. Hành động bình thường để thăm dò (Blend-in)
            {
                "user": victim, "role": "HR", "action": "SEARCH_EMPLOYEE", "params": {},
                "target_database": "hr_db", "is_anomaly": 0,
                "description": "Thao tác bình thường để tránh nghi ngờ"
            },
            # 3. Hành động bất thường (Exfiltration)
            {
                "user": victim, "role": "ATTACKER", "action": "DUMP_CUSTOMERS", "params": {},
                "target_database": "sales_db", "is_anomaly": 1, # HR không nên dump Sales
                "description": "ATO: Đánh cắp dữ liệu khách hàng"
            }
        ]

    def _insider_sabotage(self, target_user=None, **kwargs):
        """
        Scenario: IT Admin bất mãn xóa dữ liệu.
        Dấu hiệu: User xịn, IP xịn, nhưng lệnh DESTRUCTIVE.
        """
        user = target_user or "admin_user"
        
        return [
            {
                "user": user, "role": "ADMIN", "action": "LOGIN", "params": {},
                "target_database": "admin_db", "is_anomaly": 0,
                "description": "Admin đăng nhập"
            },
            # Tắt logging để che dấu vết (Rule 14: Security Config Change)
            {
                "user": user, "role": "ATTACKER", "action": "DISABLE_LOGGING", "params": {},
                "target_database": "mysql", "is_anomaly": 1,
                "description": "Tắt general_log/audit_log"
            },
            # Xóa dữ liệu (Rule 15: Mass Deletion)
            {
                "user": user, "role": "ATTACKER", "action": "MASS_DELETE", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Xóa toàn bộ đơn hàng trong tháng"
            }
        ]

    def _insider_salary_theft(self, target_user=None, **kwargs):
        """
        Scenario: Vietnamese employee accessing salary information illegally
        """
        user = target_user or random.choice(self.vietnamese_employees)
        
        return [
            # Normal login
            {
                "user": user, "role": "DEV", "action": "LOGIN", "params": {},
                "target_database": "admin_db", "is_anomaly": 0,
                "description": "Đăng nhập bình thường"
            },
            # Legitimate work activity
            {
                "user": user, "role": "DEV", "action": "CHECK_LOGS", "params": {},
                "target_database": "admin_db", "is_anomaly": 0,
                "description": "Kiểm tra logs hệ thống"
            },
            # Suspicious HR database access
            {
                "user": user, "role": "HR", "action": "VIEW_PAYROLL", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Truy cập bất thường vào bảng lương"
            },
            # Data exfiltration attempt
            {
                "user": user, "role": "ATTACKER", "action": "DUMP_DATA", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Cố gắng xuất dữ liệu lương"
            },
            # Cover tracks
            {
                "user": user, "role": "DEV", "action": "LOGOUT", "params": {},
                "target_database": "admin_db", "is_anomaly": 0,
                "description": "Đăng xuất để che dấu vết"
            }
        ]

    def _external_hack_attempt(self, target_user=None, **kwargs):
        """
        Scenario: External hacker attacking Vietnamese company systems
        """
        user = target_user or "unknown_hacker"
        attack_origin = kwargs.get('origin', 'international')
        
        scenario = []
        
        # Brute force login attempts
        for i in range(5):
            scenario.append({
                "user": f"{user}_{i}", "role": "ATTACKER", "action": "LOGIN", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": f"Thử đăng nhập lần {i+1}"
            })
        
        # Reconnaissance
        scenario.extend([
            {
                "user": user, "role": "ATTACKER", "action": "RECON_SCHEMA", "params": {},
                "target_database": "information_schema", "is_anomaly": 1,
                "description": "Dò tìm cấu trúc database"
            },
            {
                "user": user, "role": "ATTACKER", "action": "ENUM_TABLES", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Liệt kê các bảng dữ liệu"
            }
        ])
        
        # SQL injection attacks
        injection_attacks = ["SQLI_CLASSIC", "SQLI_UNION", "SQLI_BLIND"]
        for attack in injection_attacks:
            scenario.append({
                "user": user, "role": "ATTACKER", "action": attack, "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": f"Tấn công SQL injection: {attack}"
            })
        
        # Data exfiltration
        scenario.append({
            "user": user, "role": "ATTACKER", "action": "DUMP_CUSTOMERS", "params": {},
            "target_database": "sales_db", "is_anomaly": 1,
            "description": "Cố gắng đánh cắp dữ liệu khách hàng"
        })
        
        return scenario

    def _sales_snooping(self, target_user=None, **kwargs):
        """
        Scenario: Sales employee accessing unauthorized data
        """
        user = target_user or random.choice([u for u in self.vietnamese_employees if "sales" in u.lower()])
        
        return [
            {
                "user": user, "role": "SALES", "action": "LOGIN", "params": {},
                "target_database": "sales_db", "is_anomaly": 0,
                "description": "Đăng nhập bình thường"
            },
            {
                "user": user, "role": "SALES", "action": "SEARCH_CUSTOMER", "params": {},
                "target_database": "sales_db", "is_anomaly": 0,
                "description": "Tìm kiếm khách hàng"
            },
            # Unauthorized HR access
            {
                "user": user, "role": "HR", "action": "SEARCH_EMPLOYEE", "params": {"dept_id": 1},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Truy cập bất thường vào dữ liệu nhân viên"
            },
            {
                "user": user, "role": "HR", "action": "VIEW_PROFILE", "params": {"employee_id": 1},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Xem hồ sơ nhân viên không được phép"
            },
            # Unauthorized finance access
            {
                "user": user, "role": "FINANCE", "action": "VIEW_EXPENSES", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "Truy cập bất thường vào dữ liệu tài chính"
            },
            {
                "user": user, "role": "SALES", "action": "LOGOUT", "params": {},
                "target_database": "sales_db", "is_anomaly": 0,
                "description": "Đăng xuất"
            }
        ]

    def _privilege_escalation(self, target_user=None, **kwargs):
        """
        Scenario: Privilege escalation attack in Vietnamese company
        """
        user = target_user or "intern_temp"
        
        return [
            {
                "user": user, "role": "VULNERABLE", "action": "LOGIN", "params": {},
                "target_database": "sales_db", "is_anomaly": 0,
                "description": "Đăng nhập với tài khoản thực tập sinh"
            },
            {
                "user": user, "role": "ATTACKER", "action": "CHECK_PRIVILEGES", "params": {},
                "target_database": "mysql", "is_anomaly": 1,
                "description": "Kiểm tra quyền hạn hiện tại"
            },
            {
                "user": user, "role": "ATTACKER", "action": "ESCALATE_PRIVS", "params": {},
                "target_database": "mysql", "is_anomaly": 1,
                "description": "Cố gắng nâng cao quyền hạn"
            },
            {
                "user": user, "role": "ADMIN", "action": "ADMIN_ACCESS", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Truy cập với quyền admin bất hợp pháp"
            },
            {
                "user": user, "role": "ATTACKER", "action": "CREATE_BACKDOOR", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Tạo backdoor để truy cập sau"
            }
        ]

    def _data_exfiltration(self, target_user=None, **kwargs):
        """
        Scenario: Systematic data exfiltration from Vietnamese company
        """
        user = target_user or "data_thief"
        target_databases = kwargs.get('databases', ['sales_db', 'hr_db', 'finance_db'])
        
        scenario = []
        
        # Login
        scenario.append({
            "user": user, "role": "ATTACKER", "action": "LOGIN", "params": {},
            "target_database": "sales_db", "is_anomaly": 1,
            "description": "Đăng nhập với mục đích đánh cắp dữ liệu"
        })
        
        # Exfiltrate from each target database
        # Exfiltrate from each target database
        for db in target_databases:
            if db == 'sales_db':
                actions = [("DUMP_CUSTOMERS", "khách hàng"), ("DUMP_ORDERS", "đơn hàng")]
            elif db == 'hr_db':
                actions = [("DUMP_EMPLOYEES", "nhân viên"), ("DUMP_SALARIES", "lương")]
            elif db == 'finance_db':
                actions = [("DUMP_INVOICES", "hóa đơn"), ("DUMP_ACCOUNTS", "tài khoản")]
            elif db == 'inventory_db':
                actions = [("DUMP_INVENTORY", "kho"), ("DUMP_MOVEMENTS", "vận chuyển")]
            elif db == 'marketing_db':
                actions = [("DUMP_LEADS", "khách hàng tiềm năng"), ("DUMP_CAMPAIGNS", "chiến dịch")]
            elif db == 'support_db':
                actions = [("DUMP_TICKETS", "hỗ trợ")]
            elif db == 'admin_db':
                actions = [("DUMP_LOGS", "hệ thống")]
            else:
                actions = [("DUMP_DATA", "dữ liệu")]

            for action, name in actions:
                scenario.append({
                    "user": user, "role": "ATTACKER", "action": action, "params": {},
                    "target_database": db, "is_anomaly": 1,
                    "description": f"Đánh cắp dữ liệu {name} từ {db}"
                })
        
        # Cover tracks
        scenario.append({
            "user": user, "role": "ATTACKER", "action": "COVER_TRACKS", "params": {},
            "target_database": "admin_db", "is_anomaly": 1,
            "description": "Xóa dấu vết hoạt động"
        })
        
        return scenario

    def _lateral_movement(self, target_user=None, **kwargs):
        """
        Scenario: Lateral movement through Vietnamese company network
        """
        user = target_user or random.choice(self.vietnamese_employees)
        
        return [
            # Start with legitimate access
            {
                "user": user, "role": "MARKETING", "action": "LOGIN", "params": {},
                "target_database": "marketing_db", "is_anomaly": 0,
                "description": "Đăng nhập vào hệ thống marketing"
            },
            # Move to sales system
            {
                "user": user, "role": "SALES", "action": "SEARCH_CUSTOMER", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Di chuyển sang hệ thống bán hàng"
            },
            # Move to HR system
            {
                "user": user, "role": "HR", "action": "SEARCH_EMPLOYEE", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Di chuyển sang hệ thống nhân sự"
            },
            # Move to finance system
            {
                "user": user, "role": "FINANCE", "action": "VIEW_INVOICE", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "Di chuyển sang hệ thống tài chính"
            },
            # Finally access admin system
            {
                "user": user, "role": "ADMIN", "action": "CHECK_LOGS", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Truy cập hệ thống quản trị"
            }
        ]

    def _financial_fraud(self, target_user=None, **kwargs):
        """
        Scenario: Financial fraud in Vietnamese company
        """
        user = target_user or "finance_insider"
        
        return [
            {
                "user": user, "role": "FINANCE", "action": "LOGIN", "params": {},
                "target_database": "finance_db", "is_anomaly": 0,
                "description": "Đăng nhập hệ thống tài chính"
            },
            {
                "user": user, "role": "FINANCE", "action": "VIEW_INVOICE", "params": {},
                "target_database": "finance_db", "is_anomaly": 0,
                "description": "Xem hóa đơn bình thường"
            },
            # Fraudulent invoice creation
            {
                "user": user, "role": "FINANCE", "action": "CREATE_INVOICE", "params": {"amount": 500000000},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "Tạo hóa đơn gian lận với số tiền lớn"
            },
            # Modify payment records
            {
                "user": user, "role": "FINANCE", "action": "UPDATE_PAYMENT", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "Sửa đổi bản ghi thanh toán"
            },
            # Delete audit trail
            {
                "user": user, "role": "ATTACKER", "action": "DELETE_LOGS", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Xóa dấu vết kiểm toán"
            }
        ]

    def _customer_data_breach(self, target_user=None, **kwargs):
        """
        Scenario: Customer data breach in Vietnamese company
        """
        user = target_user or "customer_service_insider"
        
        return [
            {
                "user": user, "role": "CUSTOMER_SERVICE", "action": "LOGIN", "params": {},
                "target_database": "support_db", "is_anomaly": 0,
                "description": "Đăng nhập hệ thống chăm sóc khách hàng"
            },
            # Access customer data
            {
                "user": user, "role": "SALES", "action": "SEARCH_CUSTOMER", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Truy cập dữ liệu khách hàng không được phép"
            },
            # Export customer information
            {
                "user": user, "role": "ATTACKER", "action": "EXPORT_CUSTOMERS", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Xuất thông tin khách hàng ra ngoài"
            },
            # Access customer financial data
            {
                "user": user, "role": "FINANCE", "action": "VIEW_INVOICE", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "Truy cập dữ liệu tài chính khách hàng"
            }
        ]

    def _supply_chain_attack(self, target_user=None, **kwargs):
        """
        Scenario: Supply chain attack through inventory system
        """
        user = target_user or "supply_chain_attacker"
        
        return [
            {
                "user": user, "role": "ATTACKER", "action": "LOGIN", "params": {},
                "target_database": "inventory_db", "is_anomaly": 1,
                "description": "Đăng nhập bất hợp pháp vào hệ thống kho"
            },
            # Manipulate inventory data
            {
                "user": user, "role": "ATTACKER", "action": "UPDATE_INVENTORY", "params": {"quantity": -1000},
                "target_database": "inventory_db", "is_anomaly": 1,
                "description": "Thao túng dữ liệu tồn kho"
            },
            # Create fake suppliers
            {
                "user": user, "role": "ATTACKER", "action": "CREATE_SUPPLIER", "params": {},
                "target_database": "inventory_db", "is_anomaly": 1,
                "description": "Tạo nhà cung cấp giả"
            },
            # Redirect orders
            {
                "user": user, "role": "ATTACKER", "action": "REDIRECT_ORDERS", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Chuyển hướng đơn hàng"
            }
        ]

    def _social_engineering(self, target_user=None, **kwargs):
        """
        Scenario: Social engineering attack in Vietnamese company
        """
        user = target_user or "social_engineer"
        target_employee = kwargs.get('target_employee', random.choice(self.vietnamese_employees))
        
        return [
            # Impersonate IT support
            {
                "user": user, "role": "DEV", "action": "LOGIN", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Mạo danh nhân viên IT"
            },
            # Request password reset
            {
                "user": user, "role": "ATTACKER", "action": "RESET_PASSWORD", "params": {"target": target_employee},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": f"Yêu cầu reset mật khẩu cho {target_employee}"
            },
            # Access with compromised credentials
            {
                "user": target_employee, "role": "SALES", "action": "LOGIN", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Đăng nhập bằng thông tin đã bị xâm phạm"
            },
            # Perform malicious activities
            {
                "user": target_employee, "role": "ATTACKER", "action": "DUMP_DATA", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Thực hiện hoạt động độc hại"
            }
        ]

    # ========== NEW: SOPHISTICATED RULE-BYPASSING SCENARIOS ==========
    
    def _work_hours_bypass(self, target_user=None, **kwargs):
        """
        Scenario: Bypassing work hours restrictions using legitimate exceptions
        Exploits: Malicious agents working during off-hours with low detection probability
        """
        user = target_user or "night_shift_attacker"
        
        return [
            # Establish pattern during legitimate hours first
            {
                "user": user, "role": "DEV", "action": "LOGIN", "params": {},
                "target_database": "admin_db", "is_anomaly": 0,
                "description": "Đăng nhập hợp pháp trong giờ làm việc",
                "timing": "business_hours"
            },
            {
                "user": user, "role": "DEV", "action": "CHECK_LOGS", "params": {},
                "target_database": "admin_db", "is_anomaly": 0,
                "description": "Hoạt động bình thường để tạo pattern",
                "timing": "business_hours"
            },
            # Exploit: Work during off-hours with "maintenance" excuse
            {
                "user": user, "role": "DEV", "action": "LOGIN", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Đăng nhập ngoài giờ với lý do bảo trì",
                "timing": "off_hours",
                "bypass_technique": "maintenance_excuse"
            },
            # Malicious activity disguised as system maintenance
            {
                "user": user, "role": "ATTACKER", "action": "DUMP_DATA", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Đánh cắp dữ liệu trong lúc 'bảo trì'",
                "timing": "off_hours",
                "bypass_technique": "maintenance_cover"
            },
            # Weekend exploitation (5% chance for malicious)
            {
                "user": user, "role": "ATTACKER", "action": "CREATE_BACKDOOR", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Tạo backdoor vào cuối tuần",
                "timing": "weekend",
                "bypass_technique": "weekend_low_monitoring"
            }
        ]
    
    def _network_segmentation_bypass(self, target_user=None, **kwargs):
        """
        Scenario: Bypassing network segmentation using legitimate cross-department access
        Exploits: Using legitimate roles to access unauthorized network segments
        """
        user = target_user or "network_hopper"
        
        return [
            # Start from legitimate network segment
            {
                "user": user, "role": "SALES", "action": "LOGIN", "params": {},
                "target_database": "sales_db", "is_anomaly": 0,
                "description": "Đăng nhập từ mạng Sales hợp pháp",
                "source_ip": "192.168.10.100"
            },
            # Exploit: Use DEV role to access multiple networks
            {
                "user": user, "role": "DEV", "action": "DEBUG_QUERY", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Sử dụng quyền DEV để truy cập mạng HR",
                "source_ip": "192.168.20.50",  # HR network from DEV credentials
                "bypass_technique": "role_privilege_abuse"
            },
            # Network hopping through management access
            {
                "user": user, "role": "MANAGEMENT", "action": "VIEW_REPORT", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "Nhảy sang mạng Finance qua quyền quản lý",
                "source_ip": "192.168.30.25",  # Finance network
                "bypass_technique": "management_privilege_escalation"
            },
            # Final attack from admin network
            {
                "user": user, "role": "ADMIN", "action": "DUMP_ALL_DATA", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Tấn công cuối từ mạng quản trị",
                "source_ip": "192.168.60.10",  # Admin network
                "bypass_technique": "admin_network_compromise"
            }
        ]
    
    def _role_escalation_chain(self, target_user=None, **kwargs):
        """
        Scenario: Sophisticated role escalation bypassing access controls
        Exploits: Chaining legitimate roles to gain unauthorized access
        """
        user = target_user or random.choice(self.vietnamese_employees)
        
        return [
            # Start with lowest privilege role
            {
                "user": user, "role": "CUSTOMER_SERVICE", "action": "LOGIN", "params": {},
                "target_database": "support_db", "is_anomaly": 0,
                "description": "Bắt đầu với quyền chăm sóc khách hàng"
            },
            # Exploit: Use customer service access to sales data
            {
                "user": user, "role": "SALES", "action": "SEARCH_CUSTOMER", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Lạm dụng quyền truy cập dữ liệu bán hàng",
                "bypass_technique": "cross_role_access"
            },
            # Escalate to marketing role
            {
                "user": user, "role": "MARKETING", "action": "VIEW_CAMPAIGN", "params": {},
                "target_database": "marketing_db", "is_anomaly": 1,
                "description": "Nâng cấp lên quyền marketing",
                "bypass_technique": "role_chain_escalation"
            },
            # Final escalation to finance
            {
                "user": user, "role": "FINANCE", "action": "VIEW_INVOICE", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "Truy cập bất hợp pháp vào tài chính",
                "bypass_technique": "ultimate_privilege_escalation"
            },
            # Data exfiltration with highest privileges
            {
                "user": user, "role": "ADMIN", "action": "EXPORT_ALL_DATA", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Xuất toàn bộ dữ liệu với quyền admin",
                "bypass_technique": "admin_privilege_abuse"
            }
        ]
    
    def _lunch_break_exploitation(self, target_user=None, **kwargs):
        """
        Scenario: Exploiting reduced monitoring during Vietnamese lunch breaks
        Exploits: Using flexible lunch hours (11:30-13:30) for malicious activities
        """
        user = target_user or "lunch_break_attacker"
        
        return [
            # Normal morning activity
            {
                "user": user, "role": "SALES", "action": "SEARCH_CUSTOMER", "params": {},
                "target_database": "sales_db", "is_anomaly": 0,
                "description": "Hoạt động bình thường buổi sáng",
                "timing": "morning"
            },
            # Exploit: Attack during early lunch (11:30-12:00) - 40% activity
            {
                "user": user, "role": "ATTACKER", "action": "RECON_SCHEMA", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Dò tìm cấu trúc DB trong giờ ăn trưa sớm",
                "timing": "early_lunch",
                "bypass_technique": "lunch_break_low_monitoring"
            },
            # Core lunch hour attack (12:00-13:00) - 20% activity
            {
                "user": user, "role": "ATTACKER", "action": "SQLI_BLIND", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "SQL injection trong giờ ăn trưa chính",
                "timing": "core_lunch",
                "bypass_technique": "minimal_monitoring_window"
            },
            # Extended lunch attack (13:00-13:30) - 30% activity
            {
                "user": user, "role": "ATTACKER", "action": "DUMP_CUSTOMERS", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Đánh cắp dữ liệu trong giờ ăn trưa kéo dài",
                "timing": "extended_lunch",
                "bypass_technique": "extended_lunch_exploitation"
            },
            # Resume normal activity
            {
                "user": user, "role": "SALES", "action": "VIEW_ORDER", "params": {},
                "target_database": "sales_db", "is_anomaly": 0,
                "description": "Trở lại hoạt động bình thường",
                "timing": "afternoon"
            }
        ]
    
    def _holiday_backdoor_access(self, target_user=None, **kwargs):
        """
        Scenario: Exploiting Vietnamese holidays for backdoor installation
        Exploits: Zero monitoring during Vietnamese holidays
        """
        user = target_user or "holiday_attacker"
        
        return [
            # Pre-holiday preparation
            {
                "user": user, "role": "DEV", "action": "LOGIN", "params": {},
                "target_database": "admin_db", "is_anomaly": 0,
                "description": "Chuẩn bị trước ngày lễ",
                "timing": "pre_holiday"
            },
            # Tet holiday exploitation (2025-01-29)
            {
                "user": user, "role": "ATTACKER", "action": "CREATE_BACKDOOR", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Tạo backdoor trong ngày Tết",
                "timing": "tet_holiday",
                "bypass_technique": "holiday_zero_monitoring"
            },
            # Labor Day exploitation (2025-05-01)
            {
                "user": user, "role": "ATTACKER", "action": "INSTALL_MALWARE", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Cài malware trong ngày Quốc tế Lao động",
                "timing": "labor_day",
                "bypass_technique": "national_holiday_exploitation"
            },
            # Independence Day attack (2025-09-02)
            {
                "user": user, "role": "ATTACKER", "action": "MODIFY_PERMISSIONS", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "Sửa đổi quyền trong ngày Quốc khánh",
                "timing": "independence_day",
                "bypass_technique": "patriotic_holiday_cover"
            }
        ]
    
    def _cross_department_impersonation(self, target_user=None, **kwargs):
        """
        Scenario: Impersonating employees from different departments
        Exploits: Using legitimate employee names with wrong department access
        """
        user = target_user or "impersonator"
        legitimate_employee = random.choice(self.vietnamese_employees)
        
        return [
            # Impersonate HR employee accessing Finance
            {
                "user": legitimate_employee, "role": "FINANCE", "action": "VIEW_INVOICE", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": f"Mạo danh {legitimate_employee} truy cập tài chính",
                "bypass_technique": "identity_spoofing"
            },
            # Impersonate Sales employee accessing HR
            {
                "user": legitimate_employee, "role": "HR", "action": "VIEW_PAYROLL", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": f"Mạo danh {legitimate_employee} xem bảng lương",
                "bypass_technique": "cross_department_impersonation"
            },
            # Impersonate Dev with Admin privileges
            {
                "user": legitimate_employee, "role": "ADMIN", "action": "MODIFY_USERS", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": f"Mạo danh {legitimate_employee} với quyền admin",
                "bypass_technique": "privilege_impersonation"
            }
        ]
    
    def _legitimate_tool_abuse(self, target_user=None, **kwargs):
        """
        Scenario: Abusing legitimate business tools for malicious purposes
        Exploits: Using authorized programs (Tableau, Excel, etc.) for attacks
        """
        user = target_user or "tool_abuser"
        
        return [
            # Abuse Tableau for data exfiltration
            {
                "user": user, "role": "SALES", "action": "EXPORT_REPORT", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Lạm dụng Tableau để xuất dữ liệu bất hợp pháp",
                "program": "Tableau",
                "bypass_technique": "legitimate_tool_abuse"
            },
            # Abuse Excel for financial manipulation
            {
                "user": user, "role": "FINANCE", "action": "BULK_UPDATE", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "Lạm dụng Excel để thao túng tài chính",
                "program": "excel",
                "bypass_technique": "spreadsheet_manipulation"
            },
            # Abuse PowerBI for unauthorized reporting
            {
                "user": user, "role": "MANAGEMENT", "action": "GENERATE_REPORT", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Lạm dụng PowerBI tạo báo cáo trái phép",
                "program": "PowerBIDesktop",
                "bypass_technique": "business_intelligence_abuse"
            },
            # Abuse MySQLWorkbench for direct DB access
            {
                "user": user, "role": "DEV", "action": "DIRECT_SQL_EXECUTION", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Lạm dụng MySQLWorkbench truy cập trực tiếp DB",
                "program": "MySQLWorkbench",
                "bypass_technique": "database_tool_abuse"
            }
        ]
    
    def _time_based_evasion(self, target_user=None, **kwargs):
        """
        Scenario: Using time-based patterns to evade detection
        Exploits: Spreading attacks across time to avoid pattern detection
        """
        user = target_user or "time_evader"
        
        return [
            # Phase 1: Early morning reconnaissance (low activity)
            {
                "user": user, "role": "DEV", "action": "RECON_SCHEMA", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Dò tìm sáng sớm khi ít hoạt động",
                "timing": "early_morning",
                "bypass_technique": "low_activity_window"
            },
            # Phase 2: Lunch break exploitation
            {
                "user": user, "role": "ATTACKER", "action": "SQLI_CLASSIC", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Tấn công trong giờ ăn trưa",
                "timing": "lunch_break",
                "bypass_technique": "lunch_break_timing"
            },
            # Phase 3: End of day cleanup evasion
            {
                "user": user, "role": "ATTACKER", "action": "COVER_TRACKS", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Xóa dấu vết cuối ngày",
                "timing": "end_of_day",
                "bypass_technique": "cleanup_timing"
            },
            # Phase 4: Weekend persistence
            {
                "user": user, "role": "ATTACKER", "action": "MAINTAIN_ACCESS", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Duy trì truy cập cuối tuần",
                "timing": "weekend",
                "bypass_technique": "weekend_persistence"
            }
        ]
    
    def _multi_stage_persistence(self, target_user=None, **kwargs):
        """
        Scenario: Multi-stage attack with persistence mechanisms
        Exploits: Creating multiple backdoors across different systems
        """
        user = target_user or "persistent_attacker"
        
        return [
            # Stage 1: Initial compromise
            {
                "user": user, "role": "VULNERABLE", "action": "LOGIN", "params": {},
                "target_database": "sales_db", "is_anomaly": 0,
                "description": "Xâm nhập ban đầu qua tài khoản yếu"
            },
            # Stage 2: Establish foothold
            {
                "user": user, "role": "ATTACKER", "action": "CREATE_BACKDOOR", "params": {},
                "target_database": "sales_db", "is_anomaly": 1,
                "description": "Tạo backdoor đầu tiên",
                "bypass_technique": "initial_persistence"
            },
            # Stage 3: Lateral movement with persistence
            {
                "user": user, "role": "ATTACKER", "action": "LATERAL_BACKDOOR", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Tạo backdoor thứ hai trong HR",
                "bypass_technique": "lateral_persistence"
            },
            # Stage 4: Administrative persistence
            {
                "user": user, "role": "ATTACKER", "action": "ADMIN_BACKDOOR", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Tạo backdoor admin để kiểm soát lâu dài",
                "bypass_technique": "administrative_persistence"
            },
            # Stage 5: Dormant activation
            {
                "user": user, "role": "ATTACKER", "action": "ACTIVATE_DORMANT", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "Kích hoạt backdoor ngủ trong tài chính",
                "bypass_technique": "dormant_activation"
            }
        ]
    
    def _vietnamese_cultural_exploitation(self, target_user=None, **kwargs):
        """
        Scenario: Exploiting Vietnamese cultural patterns and business practices
        Exploits: Using cultural knowledge to blend in and avoid detection
        """
        user = target_user or "cultural_attacker"
        
        return [
            # Exploit Tet preparation period (increased activity)
            {
                "user": user, "role": "FINANCE", "action": "BONUS_CALCULATION", "params": {},
                "target_database": "finance_db", "is_anomaly": 1,
                "description": "Lạm dụng thời gian chuẩn bị Tết để truy cập tài chính",
                "timing": "tet_preparation",
                "bypass_technique": "cultural_timing_exploitation"
            },
            # Exploit Vietnamese naming conventions
            {
                "user": "nguyen_van_admin", "role": "ADMIN", "action": "SYSTEM_ACCESS", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Sử dụng tên Việt Nam phổ biến để mạo danh",
                "bypass_technique": "vietnamese_name_spoofing"
            },
            # Exploit Vietnamese business hierarchy respect
            {
                "user": user, "role": "MANAGEMENT", "action": "OVERRIDE_SECURITY", "params": {},
                "target_database": "hr_db", "is_anomaly": 1,
                "description": "Lạm dụng văn hóa tôn trọng cấp trên",
                "bypass_technique": "hierarchy_exploitation"
            },
            # Exploit Vietnamese work culture (overtime acceptance)
            {
                "user": user, "role": "DEV", "action": "OVERTIME_ACCESS", "params": {},
                "target_database": "admin_db", "is_anomaly": 1,
                "description": "Lạm dụng văn hóa làm thêm giờ",
                "timing": "overtime",
                "bypass_technique": "overtime_culture_abuse"
            }
        ]

    def get_random_scenario(self, exclude_scenarios=None):
        """Get a random scenario for dynamic simulation"""
        all_scenarios = [
            # Original scenarios
            "INSIDER_SALARY_THEFT", "EXTERNAL_HACK_ATTEMPT", "SALES_SNOOPING",
            "PRIVILEGE_ESCALATION", "DATA_EXFILTRATION", "LATERAL_MOVEMENT",
            "FINANCIAL_FRAUD", "CUSTOMER_DATA_BREACH", "SUPPLY_CHAIN_ATTACK",
            "SOCIAL_ENGINEERING",
            
            # New rule-bypassing scenarios
            "WORK_HOURS_BYPASS", "NETWORK_SEGMENTATION_BYPASS", "ROLE_ESCALATION_CHAIN",
            "LUNCH_BREAK_EXPLOITATION", "HOLIDAY_BACKDOOR_ACCESS", "CROSS_DEPARTMENT_IMPERSONATION",
            "LEGITIMATE_TOOL_ABUSE", "TIME_BASED_EVASION", "MULTI_STAGE_PERSISTENCE",
            "VIETNAMESE_CULTURAL_EXPLOITATION"
        ]
        
        if exclude_scenarios:
            all_scenarios = [s for s in all_scenarios if s not in exclude_scenarios]
        
        scenario_name = random.choice(all_scenarios)
        return scenario_name, self.get_scenario(scenario_name)

# Example usage and testing
if __name__ == "__main__":
    scenario_manager = EnhancedScenarioManager()
    
    print("🧪 TESTING ENHANCED SCENARIO MANAGER")
    print("=" * 50)
    
    # Test original scenarios
    test_scenarios = ["INSIDER_SALARY_THEFT", "EXTERNAL_HACK_ATTEMPT", "FINANCIAL_FRAUD"]
    
    for scenario_name in test_scenarios:
        print(f"\n🎯 Original Scenario: {scenario_name}")
        intents = scenario_manager.get_scenario(scenario_name)
        
        for i, intent in enumerate(intents, 1):
            print(f"   {i}. {intent['user']} ({intent['role']}) | {intent['action']} | {intent['description']}")
    
    # Test new rule-bypassing scenarios
    bypass_scenarios = ["WORK_HOURS_BYPASS", "LUNCH_BREAK_EXPLOITATION", "NETWORK_SEGMENTATION_BYPASS"]
    
    for scenario_name in bypass_scenarios:
        print(f"\n🚨 Rule-Bypassing Scenario: {scenario_name}")
        intents = scenario_manager.get_scenario(scenario_name)
        
        for i, intent in enumerate(intents, 1):
            bypass_info = f" [BYPASS: {intent.get('bypass_technique', 'N/A')}]" if intent.get('bypass_technique') else ""
            timing_info = f" [TIMING: {intent.get('timing', 'N/A')}]" if intent.get('timing') else ""
            print(f"   {i}. {intent['user']} ({intent['role']}) | {intent['action']}{bypass_info}{timing_info}")
            print(f"      📝 {intent['description']}")
    
    # Test random scenario
    print(f"\n🎲 Random Scenario:")
    scenario_name, intents = scenario_manager.get_random_scenario()
    print(f"   Selected: {scenario_name}")
    print(f"   Steps: {len(intents)}")
    
    print(f"\n✅ Enhanced scenario manager with rule-bypassing capabilities ready!")
    print(f"📊 Total scenarios available: {len(scenario_manager.get_scenario.__code__.co_names)}")
    print(f"🔓 Rule-bypassing techniques: 10 advanced scenarios")
    print(f"🇻🇳 Vietnamese cultural exploitation: Integrated")
    print(f"⏰ Time-based evasion: Implemented")
    print(f"🌐 Network segmentation bypass: Active")