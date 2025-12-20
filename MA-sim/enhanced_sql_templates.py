#!/usr/bin/env python3
"""
Enhanced SQL Templates for Vietnamese Medium-Sized Sales Company
Provides realistic SQL queries for 7-database structure
"""

import random
from datetime import datetime, timedelta

class EnhancedSQLTemplates:
    """
    SQL templates for enhanced 7-database Vietnamese company structure
    """
    
    def __init__(self):
        self.vietnamese_cities = [
            "Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ", 
            "Nha Trang", "Vũng Tàu", "Bình Dương", "Đồng Nai", "Long An"
        ]
        
        self.vietnamese_companies = [
            "Công ty TNHH Thương mại Việt Nam", "Công ty CP Xuất nhập khẩu ABC",
            "Công ty TNHH Sản xuất XYZ", "Công ty CP Đầu tư và Phát triển",
            "Công ty TNHH Dịch vụ Logistics", "Công ty CP Công nghệ Thông tin"
        ]
        
        self.product_categories = [
            "Điện tử", "Nội thất", "Thời trang", "Đồ gia dụng", "Thực phẩm",
            "Mỹ phẩm", "Đồ chơi", "Sách", "Thiết bị văn phòng", "Dụng cụ thể thao"
        ]

    # SALES DATABASE QUERIES
    def get_sales_queries(self, role):
        """Get sales database queries based on user role"""
        queries = []
        
        if role in ["SALES", "MARKETING", "CUSTOMER_SERVICE", "FINANCE", "MANAGEMENT", "DEV", "ADMIN"]:
            queries.extend([
                # Customer management
                "SELECT customer_id, company_name, contact_person, city FROM sales_db.customers WHERE status = 'active' ORDER BY company_name",
                "SELECT c.company_name, COUNT(o.order_id) as total_orders FROM sales_db.customers c LEFT JOIN sales_db.orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id",
                "SELECT * FROM sales_db.customers WHERE city = '{}' AND customer_type = 'business'".format(random.choice(self.vietnamese_cities)),
                
                # Product queries
                "SELECT p.product_name, pc.category_name, p.price FROM sales_db.products p JOIN sales_db.product_categories pc ON p.category_id = pc.category_id WHERE p.status = 'active'",
                "SELECT category_name, COUNT(*) as product_count FROM sales_db.product_categories pc JOIN sales_db.products p ON pc.category_id = p.category_id GROUP BY pc.category_id",
                
                # Order analysis
                "SELECT DATE(order_date) as order_day, COUNT(*) as daily_orders, SUM(total_amount) as daily_revenue FROM sales_db.orders WHERE order_date >= CURDATE() - INTERVAL 30 DAY GROUP BY DATE(order_date)",
                "SELECT o.order_number, c.company_name, o.total_amount, o.status FROM sales_db.orders o JOIN sales_db.customers c ON o.customer_id = c.customer_id WHERE o.order_date >= CURDATE() - INTERVAL 7 DAY",
            ])
        
        if role in ["SALES", "MARKETING", "MANAGEMENT", "DEV", "ADMIN"]:
            queries.extend([
                # Sales-specific operations (only safe SELECT and UPDATE operations)
                "SELECT * FROM sales_db.customers WHERE customer_id = {}".format(random.randint(1, 1000)),
                "SELECT * FROM sales_db.orders WHERE order_id = {}".format(random.randint(1, 1000)),
                "SELECT customer_id, company_name, contact_person FROM sales_db.customers WHERE city = '{}' LIMIT 20".format(random.choice(self.vietnamese_cities)),
                "SELECT order_id, customer_id, total_amount, status FROM sales_db.orders WHERE status = '{}' ORDER BY order_date DESC LIMIT 10".format(random.choice(['confirmed', 'processing', 'shipped'])),
                # Safe UPDATE operations with high success probability
                "UPDATE sales_db.orders SET status = 'processing' WHERE status = 'confirmed' AND order_id <= {} LIMIT 1".format(random.randint(50, 200)),
                "UPDATE sales_db.customers SET phone = '0901234567' WHERE customer_id <= {} LIMIT 1".format(random.randint(20, 100)),
                "UPDATE sales_db.orders SET status = 'shipped' WHERE status = 'processing' AND order_id <= {} LIMIT 1".format(random.randint(50, 200)),
                "UPDATE sales_db.customers SET credit_limit = credit_limit + 1000000 WHERE customer_type = 'business' AND customer_id <= {} LIMIT 1".format(random.randint(20, 100)),
                # Login check query (common in logs)
                "SELECT 1 AS login_check"
            ])
        
        return queries

    # INVENTORY DATABASE QUERIES  
    def get_inventory_queries(self, role):
        """Get inventory database queries based on user role"""
        queries = []
        
        if role in ["FINANCE", "MANAGEMENT", "DEV", "ADMIN"]:
            queries.extend([
                # Stock level monitoring
                "SELECT il.product_id, il.current_stock, il.available_stock, wl.warehouse_name FROM inventory_db.inventory_levels il JOIN inventory_db.warehouse_locations wl ON il.location_id = wl.location_id WHERE il.current_stock < il.min_stock_level",
                "SELECT wl.warehouse_name, SUM(il.current_stock) as total_stock FROM inventory_db.warehouse_locations wl JOIN inventory_db.inventory_levels il ON wl.location_id = il.location_id GROUP BY wl.location_id",
                "SELECT product_id, SUM(current_stock) as total_stock FROM inventory_db.inventory_levels GROUP BY product_id HAVING total_stock > 0"
            ])
        
        if role in ["DEV", "ADMIN"]:
            queries.extend([
                # Inventory management operations (only safe SELECT operations)
                "SELECT * FROM inventory_db.inventory_levels WHERE product_id = {}".format(random.randint(1, 100)),
                "SELECT * FROM inventory_db.warehouse_locations WHERE location_id = {}".format(random.randint(1, 5)),
                "SELECT product_id, current_stock, available_stock FROM inventory_db.inventory_levels WHERE current_stock < 50"
            ])
        
        return queries

    # FINANCE DATABASE QUERIES
    def get_finance_queries(self, role):
        """Get finance database queries based on user role"""
        queries = []
        
        if role in ["FINANCE", "HR", "MANAGEMENT", "DEV", "ADMIN"]:
            queries.extend([
                # Financial reporting
                "SELECT account_type, SUM(CASE WHEN account_type IN ('asset', 'expense') THEN 1 ELSE -1 END) as balance FROM finance_db.accounts WHERE is_active = TRUE GROUP BY account_type",
                "SELECT DATE_FORMAT(invoice_date, '%Y-%m') as month, SUM(total_amount) as monthly_revenue FROM finance_db.invoices WHERE status = 'paid' GROUP BY DATE_FORMAT(invoice_date, '%Y-%m')",
                "SELECT status, COUNT(*) as invoice_count, SUM(total_amount) as total_amount FROM finance_db.invoices GROUP BY status",
                
                # Expense analysis
                "SELECT category, SUM(amount) as total_expenses FROM finance_db.expense_reports WHERE status = 'approved' AND expense_date >= CURDATE() - INTERVAL 30 DAY GROUP BY category",
                "SELECT category, SUM(amount) as total_expenses FROM finance_db.expense_reports WHERE status IN ('submitted', 'approved') GROUP BY category ORDER BY total_expenses DESC LIMIT 10"
            ])
        
        if role in ["FINANCE", "DEV", "ADMIN"]:
            queries.extend([
                # Finance operations (only safe SELECT operations)
                "SELECT * FROM finance_db.invoices WHERE invoice_id = {}".format(random.randint(1, 100)),
                "SELECT * FROM finance_db.expense_reports WHERE expense_id = {}".format(random.randint(1, 200)),
                "SELECT invoice_id, total_amount, status FROM finance_db.invoices WHERE status = 'sent' LIMIT 10"
            ])
        
        return queries

    # MARKETING DATABASE QUERIES
    def get_marketing_queries(self, role):
        """Get marketing database queries based on user role"""
        queries = []
        
        if role in ["MARKETING", "SALES", "CUSTOMER_SERVICE", "MANAGEMENT", "DEV", "ADMIN"]:
            queries.extend([
                # Campaign performance
                "SELECT campaign_type, COUNT(*) as campaign_count, AVG(budget) as avg_budget FROM marketing_db.campaigns WHERE status = 'active' GROUP BY campaign_type",
                "SELECT c.campaign_name, COUNT(l.lead_id) as leads_generated FROM marketing_db.campaigns c LEFT JOIN marketing_db.leads l ON c.campaign_id = l.lead_source GROUP BY c.campaign_id",
                
                # Lead management
                "SELECT status, COUNT(*) as lead_count, AVG(estimated_value) as avg_value FROM marketing_db.leads GROUP BY status",
                "SELECT assigned_to, COUNT(*) as assigned_leads, SUM(estimated_value) as total_pipeline FROM marketing_db.leads WHERE status IN ('qualified', 'proposal', 'negotiation') GROUP BY assigned_to",
                "SELECT DATE(created_at) as lead_date, COUNT(*) as daily_leads FROM marketing_db.leads WHERE created_at >= CURDATE() - INTERVAL 30 DAY GROUP BY DATE(created_at)"
            ])
        
        if role in ["MARKETING", "SALES", "MANAGEMENT", "DEV", "ADMIN"]:
            queries.extend([
                # Marketing operations (only safe SELECT operations)
                "SELECT * FROM marketing_db.leads WHERE lead_id = {}".format(random.randint(1, 100)),
                "SELECT * FROM marketing_db.campaigns WHERE campaign_id = {}".format(random.randint(1, 50)),
                "SELECT lead_id, status, estimated_value FROM marketing_db.leads WHERE status = '{}' LIMIT 10".format(random.choice(['new', 'contacted', 'qualified']))
            ])
        
        return queries

    # SUPPORT DATABASE QUERIES
    def get_support_queries(self, role):
        """Get support database queries based on user role"""
        queries = []
        
        if role in ["CUSTOMER_SERVICE", "SALES", "MARKETING", "MANAGEMENT", "ADMIN"]:
            queries.extend([
                # Support ticket analysis
                "SELECT status, COUNT(*) as ticket_count FROM support_db.support_tickets GROUP BY status",
                "SELECT priority, AVG(DATEDIFF(COALESCE(resolved_at, NOW()), created_at)) as avg_resolution_days FROM support_db.support_tickets GROUP BY priority",
                "SELECT assigned_to, COUNT(*) as assigned_tickets FROM support_db.support_tickets WHERE status IN ('open', 'in_progress') GROUP BY assigned_to",
                "SELECT category, COUNT(*) as ticket_count FROM support_db.support_tickets WHERE created_at >= CURDATE() - INTERVAL 30 DAY GROUP BY category"
            ])
        
        if role in ["CUSTOMER_SERVICE", "ADMIN"]:
            queries.extend([
                # Support operations (only safe SELECT operations)
                "SELECT * FROM support_db.support_tickets WHERE ticket_id = {}".format(random.randint(1, 100)),
                "SELECT ticket_id, status, priority FROM support_db.support_tickets WHERE status = 'open' LIMIT 10",
                "SELECT * FROM support_db.support_tickets WHERE assigned_to = 'support_agent_{}'".format(random.randint(1, 10))
            ])
        
        return queries

    # HR DATABASE QUERIES (Enhanced)
    def get_hr_queries(self, role):
        """Get HR database queries based on user role"""
        queries = []
        
        if role in ["HR", "FINANCE", "MANAGEMENT", "DEV", "ADMIN"]:
            queries.extend([
                # Employee analytics
                "SELECT COUNT(*) as employee_count FROM hr_db.employees",
                "SELECT position, COUNT(*) as position_count FROM hr_db.employees GROUP BY position",
                "SELECT DATE_FORMAT(hire_date, '%Y-%m') as hire_month, COUNT(*) as new_hires FROM hr_db.employees WHERE hire_date >= CURDATE() - INTERVAL 12 MONTH GROUP BY DATE_FORMAT(hire_date, '%Y-%m')",
                
                # Attendance monitoring
                "SELECT COUNT(*) as attendance_days FROM hr_db.attendance WHERE date >= CURDATE() - INTERVAL 30 DAY",
                "SELECT status, COUNT(*) as status_count FROM hr_db.attendance WHERE date >= CURDATE() - INTERVAL 7 DAY GROUP BY status"
            ])
        
        if role in ["HR", "DEV", "ADMIN"]:
            queries.extend([
                # HR operations (only safe SELECT operations)
                "SELECT * FROM hr_db.employees WHERE id = {}".format(random.randint(1, 200)),
                "SELECT * FROM hr_db.departments WHERE id = {}".format(random.randint(1, 10)),
                "SELECT id, name, position FROM hr_db.employees WHERE id > {} LIMIT 10".format(random.randint(1, 10))
            ])
        
        return queries

    # ADMIN DATABASE QUERIES
    def get_admin_queries(self, role):
        """Get admin database queries based to user role"""
        queries = []
        
        if role in ["HR", "MANAGEMENT", "DEV", "ADMIN"]:
            queries.extend([
                # System monitoring
                "SELECT log_level, COUNT(*) as log_count FROM admin_db.system_logs WHERE created_at >= CURDATE() - INTERVAL 24 HOUR GROUP BY log_level",
                "SELECT module, COUNT(*) as error_count FROM admin_db.system_logs WHERE log_level = 'error' AND created_at >= CURDATE() - INTERVAL 7 DAY GROUP BY module",
                "SELECT user_id, COUNT(*) as session_count FROM admin_db.user_sessions WHERE login_time >= CURDATE() - INTERVAL 7 DAY GROUP BY user_id",
            ])
        
        if role in ["DEV", "ADMIN"]:
            queries.extend([
                # System operations (only safe SELECT operations)
                "SELECT * FROM admin_db.system_logs WHERE log_id = {}".format(random.randint(1, 1000)),
                "SELECT * FROM admin_db.user_sessions WHERE session_id = '{}'".format(random.randint(1000, 9999)),
                "SELECT log_level, COUNT(*) as count FROM admin_db.system_logs WHERE created_at >= CURDATE() - INTERVAL 1 DAY GROUP BY log_level"
            ])
        
        return queries

    def get_cross_database_queries(self, role):
        """Get cross-database queries for complex business operations"""
        queries = []
        
        if role in ["MANAGEMENT", "FINANCE", "ADMIN"]:
            queries.extend([
                # Cross-database analytics (these would be executed as separate queries)
                "-- Sales and Inventory Integration",
                "-- Customer Order Analysis with Stock Levels", 
                "-- Financial Performance with Marketing ROI",
                "-- Support Ticket Impact on Sales"
            ])
        
        return queries

    def get_queries_by_database_and_role(self, database, role):
        """Get appropriate queries for specific database and user role"""
        query_map = {
            'sales_db': self.get_sales_queries,
            'inventory_db': self.get_inventory_queries,
            'finance_db': self.get_finance_queries,
            'marketing_db': self.get_marketing_queries,
            'support_db': self.get_support_queries,
            'hr_db': self.get_hr_queries,
            'admin_db': self.get_admin_queries
        }
        
        if database in query_map:
            return query_map[database](role)
        else:
            return []

    def get_malicious_queries(self, database, attack_type="sql_injection"):
        """Get malicious queries for security testing"""
        malicious_queries = []
        
        # Determine appropriate table for the target database
        table_map = {
            'sales_db': 'customers',
            'hr_db': 'employees',
            'finance_db': 'invoices',
            'marketing_db': 'leads',
            'support_db': 'support_tickets',
            'inventory_db': 'products',
            'admin_db': 'user_sessions'
        }
        target_table = table_map.get(database, 'customers')
        
        if attack_type == "sql_injection":
            malicious_queries.extend([
                f"SELECT * FROM {target_table} WHERE id = 1 OR 1=1--",
                f"SELECT * FROM {target_table}; DROP TABLE {target_table};--",
                "SELECT user(), version(), database()",
                "SELECT 1,2 UNION SELECT table_name, column_name FROM information_schema.columns--"
            ])
        
        elif attack_type == "privilege_escalation":
            malicious_queries.extend([
                "SHOW GRANTS FOR CURRENT_USER()",
                "SELECT * FROM mysql.user",
                "SELECT * FROM information_schema.user_privileges",
                "SHOW DATABASES"
            ])
        
        elif attack_type == "data_exfiltration":
            malicious_queries.extend([
                f"SELECT * FROM {target_table} LIMIT 10000",
                f"SELECT * FROM {target_table} WHERE id > 0",
                f"SELECT * FROM {target_table} ORDER BY 1 LIMIT 1000",
                f"SELECT * FROM {database}.{target_table}"
            ])
        
        return malicious_queries

# Create instance for backward compatibility
ENHANCED_SQL_TEMPLATES = EnhancedSQLTemplates()

# Example usage and testing
if __name__ == "__main__":
    templates = EnhancedSQLTemplates()
    
    print("🧪 TESTING ENHANCED SQL TEMPLATES")
    print("=" * 50)
    
    # Test queries for different roles and databases
    test_cases = [
        ("sales_db", "SALES"),
        ("inventory_db", "FINANCE"), 
        ("marketing_db", "MARKETING"),
        ("support_db", "CUSTOMER_SERVICE"),
        ("hr_db", "HR"),
        ("admin_db", "ADMIN")
    ]
    
    for database, role in test_cases:
        queries = templates.get_queries_by_database_and_role(database, role)
        print(f"\n📊 {database.upper()} - {role} Role:")
        for i, query in enumerate(queries[:3], 1):  # Show first 3 queries
            print(f"   {i}. {query[:80]}...")
    
    print(f"\n🔒 Malicious Queries Sample:")
    malicious = templates.get_malicious_queries("sales_db", "sql_injection")
    for i, query in enumerate(malicious[:2], 1):
        print(f"   {i}. {query}")
    
    print(f"\n✅ Enhanced SQL templates ready for 7-database simulation")