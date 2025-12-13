#!/usr/bin/env python3
"""
Enhanced SQL Translator for Vietnamese Medium-Sized Sales Company
Converts agent intents to SQL queries using enriched templates and Vietnamese business context
"""

import random
from faker import Faker
from enhanced_sql_templates import EnhancedSQLTemplates

# Vietnamese Faker for realistic data
fake_vn = Faker('vi_VN')

class EnhancedSQLTranslator:
    """
    Enhanced SQL translator that converts agent intents to realistic SQL queries
    for Vietnamese business operations across 7 specialized databases
    """
    
    def __init__(self, db_state=None):
        self.db_state = db_state or {}
        self.sql_templates = EnhancedSQLTemplates()
        
        # Vietnamese business data for realistic queries
        self.vietnamese_cities = [
            "Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ", 
            "Nha Trang", "Vũng Tàu", "Bình Dương", "Đồng Nai", "Long An"
        ]
        
        self.vietnamese_companies = [
            "Công ty TNHH Thương mại Việt Nam", "Công ty CP Xuất nhập khẩu ABC",
            "Công ty TNHH Sản xuất XYZ", "Công ty CP Đầu tư và Phát triển"
        ]
        
        # Enhanced action to SQL mapping for 7-database structure
        self.action_sql_map = {
            # Sales operations
            "SEARCH_CUSTOMER": self._generate_customer_search,
            "VIEW_CUSTOMER": self._generate_customer_view,
            "UPDATE_CUSTOMER": self._generate_customer_update,
            "CREATE_ORDER": self._generate_order_create,
            "SEARCH_ORDER": self._generate_order_search,
            "VIEW_ORDER": self._generate_order_view,
            "UPDATE_ORDER_STATUS": self._generate_order_update,
            "ADD_ITEM": self._generate_add_item,
            
            # HR operations
            "SEARCH_EMPLOYEE": self._generate_employee_search,
            "VIEW_PROFILE": self._generate_employee_view,
            "CHECK_ATTENDANCE": self._generate_attendance_check,
            "VIEW_PAYROLL": self._generate_payroll_view,
            "UPDATE_SALARY": self._generate_salary_update,
            
            # Marketing operations
            "SEARCH_CAMPAIGN": self._generate_campaign_search,
            "VIEW_CAMPAIGN": self._generate_campaign_view,
            "CREATE_LEAD": self._generate_lead_create,
            "UPDATE_LEAD": self._generate_lead_update,
            
            # Support operations
            "SEARCH_TICKET": self._generate_ticket_search,
            "VIEW_TICKET": self._generate_ticket_view,
            "CREATE_TICKET": self._generate_ticket_create,
            "UPDATE_TICKET": self._generate_ticket_update,
            
            # Finance operations
            "VIEW_INVOICE": self._generate_invoice_view,
            "CREATE_INVOICE": self._generate_invoice_create,
            "VIEW_EXPENSES": self._generate_expense_view,
            "VIEW_REPORT": self._generate_report_view,
            
            # Inventory operations
            "CHECK_STOCK": self._generate_stock_check,
            "UPDATE_INVENTORY": self._generate_inventory_update,
            
            # System operations
            "LOGIN": lambda intent, params: "SELECT 1 AS login_check",
            "LOGOUT": lambda intent, params: "SELECT 'Logged out' AS status",
            "START": lambda intent, params: "SELECT 1 AS start_session"
        }

    def translate(self, intent):
        """
        Enhanced translation of agent intent to SQL query
        
        Args:
            intent: Dictionary containing user, role, action, params, target_database, etc.
            
        Returns:
            String: SQL query appropriate for the intent and Vietnamese business context
        """
        role = intent.get('role', 'SALES')
        action = intent.get('action', 'LOGIN')
        params = intent.get('params', {}).copy()
        target_database = intent.get('target_database', 'sales_db')
        
        # Handle malicious intents
        if intent.get('is_anomaly', 0) == 1:
            return self._generate_malicious_sql(intent, params)
        
        # Use enhanced SQL templates based on database and role
        if action in self.action_sql_map:
            try:
                sql_generator = self.action_sql_map[action]
                if callable(sql_generator):
                    return sql_generator(intent, params)
                else:
                    return sql_generator
            except Exception as e:
                # Fallback to template-based generation
                return self._generate_from_templates(intent, params)
        else:
            # Use enhanced templates for unknown actions
            return self._generate_from_templates(intent, params)

    def _generate_from_templates(self, intent, params):
        """Generate SQL using enhanced templates"""
        role = intent.get('role', 'SALES')
        target_database = intent.get('target_database', 'sales_db')
        
        # Get queries from enhanced templates
        queries = self.sql_templates.get_queries_by_database_and_role(target_database, role)
        
        if queries:
            sql_template = random.choice(queries)
            return self._fill_template_params(sql_template, params)
        else:
            # Fallback query
            return f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{target_database}'"

    def _fill_template_params(self, sql_template, params):
        """Fill template parameters with realistic Vietnamese business data"""
        
        # Add missing parameters with Vietnamese context
        if "{city}" in sql_template and "city" not in params:
            params["city"] = random.choice(self.vietnamese_cities)
            
        if "{company_name}" in sql_template and "company_name" not in params:
            params["company_name"] = random.choice(self.vietnamese_companies)
            
        if "{name}" in sql_template and "name" not in params:
            params["name"] = fake_vn.name()
            
        if "{phone}" in sql_template and "phone" not in params:
            params["phone"] = fake_vn.phone_number()[:15]
            
        if "{email}" in sql_template and "email" not in params:
            params["email"] = fake_vn.email()
            
        if "{amount}" in sql_template and "amount" not in params:
            params["amount"] = random.randint(1000000, 50000000)  # Vietnamese currency
            
        if "{price}" in sql_template and "price" not in params:
            params["price"] = random.randint(50000, 5000000)
            
        if "{quantity}" in sql_template and "quantity" not in params:
            params["quantity"] = random.randint(1, 100)
            
        if "{status}" in sql_template and "status" not in params:
            params["status"] = random.choice(['active', 'inactive', 'pending', 'completed'])
            
        if "{date}" in sql_template and "date" not in params:
            params["date"] = "2025-01-01"
        
        # Fill entity IDs from db_state if available
        entity_mappings = {
            "customer_id": "customer_ids",
            "order_id": "order_ids",
            "product_id": "product_ids",
            "employee_id": "employee_ids",
            "campaign_id": "campaign_ids"
        }
        
        for param_key, db_key in entity_mappings.items():
            if f"{{{param_key}}}" in sql_template and param_key not in params:
                if self.db_state and db_key in self.db_state and self.db_state[db_key]:
                    params[param_key] = random.choice(self.db_state[db_key])
                else:
                    params[param_key] = random.randint(1, 1000)
        
        # Format the SQL template
        try:
            return sql_template.format(**params)
        except KeyError as e:
            # Handle missing parameters gracefully
            return f"SELECT 'Missing parameter {e}' AS error"

    # Specific SQL generators for different operations
    def _generate_customer_search(self, intent, params):
        """Generate customer search SQL"""
        database = intent.get('target_database', 'sales_db')
        city = params.get('city', random.choice(self.vietnamese_cities))
        return f"SELECT customer_id, company_name, contact_person FROM {database}.customers WHERE city = '{city}' LIMIT 20"

    def _generate_customer_view(self, intent, params):
        """Generate customer view SQL"""
        database = intent.get('target_database', 'sales_db')
        customer_id = params.get('customer_id', random.randint(1, 100))
        return f"SELECT * FROM {database}.customers WHERE customer_id = {customer_id}"

    def _generate_customer_update(self, intent, params):
        """Generate customer update SQL"""
        database = intent.get('target_database', 'sales_db')
        customer_id = params.get('customer_id', random.randint(1, 100))
        phone = params.get('phone', fake_vn.phone_number()[:15])
        return f"UPDATE {database}.customers SET phone = '{phone}' WHERE customer_id = {customer_id}"

    def _generate_order_create(self, intent, params):
        """Generate order creation SQL - using safe SELECT instead of INSERT to avoid foreign key constraints"""
        database = intent.get('target_database', 'sales_db')
        customer_id = params.get('customer_id', random.randint(1, 1000))
        # Instead of INSERT, use a SELECT that simulates order creation validation
        return f"SELECT customer_id, company_name, contact_person FROM {database}.customers WHERE customer_id = {customer_id} AND status = 'active'"

    def _generate_order_search(self, intent, params):
        """Generate order search SQL"""
        database = intent.get('target_database', 'sales_db')
        status = params.get('status', random.choice(['confirmed', 'processing', 'shipped']))
        return f"SELECT order_id, customer_id, total_amount, status FROM {database}.orders WHERE status = '{status}' ORDER BY order_date DESC LIMIT 10"

    def _generate_order_view(self, intent, params):
        """Generate order view SQL"""
        database = intent.get('target_database', 'sales_db')
        order_id = params.get('order_id', random.randint(1, 1000))
        return f"SELECT * FROM {database}.orders WHERE order_id = {order_id}"

    def _generate_order_update(self, intent, params):
        """Generate order update SQL"""
        database = intent.get('target_database', 'sales_db')
        order_id = params.get('order_id', random.randint(1, 200))  # Match actual data range
        status = params.get('status', 'processing')
        # Use more flexible conditions that are likely to match
        if random.random() < 0.7:  # 70% chance of using broader condition
            return f"UPDATE {database}.orders SET status = '{status}' WHERE order_id <= {order_id} AND status IN ('confirmed', 'processing') LIMIT 1"
        else:
            current_status = random.choice(['confirmed', 'processing', 'shipped'])
            return f"UPDATE {database}.orders SET status = '{status}' WHERE order_id = {order_id} AND status = '{current_status}'"

    def _generate_add_item(self, intent, params):
        """Generate add item SQL - using safe SELECT instead of INSERT to avoid foreign key constraints"""
        database = intent.get('target_database', 'sales_db')
        # Instead of INSERT into order_items, use a SELECT that simulates item validation
        return f"SELECT category_name, COUNT(*) as product_count FROM {database}.product_categories pc JOIN {database}.products p ON pc.category_id = p.category_id GROUP BY pc.category_id"

    def _generate_employee_search(self, intent, params):
        """Generate employee search SQL"""
        database = intent.get('target_database', 'hr_db')
        dept_id = params.get('dept_id', random.randint(1, 5))
        return f"SELECT id, name, position FROM {database}.employees WHERE id > {dept_id} LIMIT 20"

    def _generate_employee_view(self, intent, params):
        """Generate employee view SQL"""
        database = intent.get('target_database', 'hr_db')
        employee_id = params.get('employee_id', random.randint(1, 200))
        return f"SELECT * FROM {database}.employees WHERE id = {employee_id}"

    def _generate_attendance_check(self, intent, params):
        """Generate attendance check SQL"""
        database = intent.get('target_database', 'hr_db')
        employee_id = params.get('employee_id', random.randint(1, 200))
        return f"SELECT * FROM {database}.attendance WHERE employee_id = {employee_id} ORDER BY date DESC LIMIT 30"

    def _generate_payroll_view(self, intent, params):
        """Generate payroll view SQL"""
        database = intent.get('target_database', 'hr_db')
        date = params.get('date', '2025-01-01')
        return f"SELECT SUM(amount) as total_payroll FROM {database}.salaries WHERE payment_date >= '{date}'"

    def _generate_salary_update(self, intent, params):
        """Generate salary update SQL"""
        database = intent.get('target_database', 'hr_db')
        employee_id = params.get('employee_id', random.randint(1, 200))
        bonus = params.get('bonus', random.randint(1000000, 5000000))
        return f"SELECT * FROM {database}.employees WHERE id = {employee_id}"

    def _generate_campaign_search(self, intent, params):
        """Generate campaign search SQL"""
        database = intent.get('target_database', 'marketing_db')
        status = params.get('status', 'active')
        return f"SELECT campaign_id, campaign_name, status FROM {database}.campaigns WHERE status = '{status}' LIMIT 10"

    def _generate_campaign_view(self, intent, params):
        """Generate campaign view SQL"""
        database = intent.get('target_database', 'marketing_db')
        campaign_id = params.get('campaign_id', random.randint(1, 50))
        return f"SELECT * FROM {database}.campaigns WHERE campaign_id = {campaign_id}"

    def _generate_lead_create(self, intent, params):
        """Generate lead creation SQL"""
        database = intent.get('target_database', 'marketing_db')
        company_name = params.get('company_name', random.choice(self.vietnamese_companies))
        contact_name = params.get('contact_name', fake_vn.name())
        email = params.get('email', fake_vn.email())
        assigned_to = intent.get('user', 'marketing_rep')
        # Use safe SELECT instead of INSERT to avoid constraint issues
        return f"SELECT lead_id, company_name, contact_name, status FROM {database}.leads WHERE assigned_to = '{assigned_to}' LIMIT 10"

    def _generate_lead_update(self, intent, params):
        """Generate lead update SQL"""
        database = intent.get('target_database', 'marketing_db')
        lead_id = params.get('lead_id', random.randint(1, 100))  # Match actual data range
        status = params.get('status', random.choice(['contacted', 'qualified', 'proposal']))
        # Use more flexible conditions that are likely to match
        if random.random() < 0.7:  # 70% chance of using broader condition
            return f"UPDATE {database}.leads SET status = '{status}' WHERE lead_id <= {lead_id} AND status IN ('new', 'contacted') LIMIT 1"
        else:
            current_status = random.choice(['new', 'contacted', 'qualified'])
            return f"UPDATE {database}.leads SET status = '{status}' WHERE lead_id = {lead_id} AND status = '{current_status}'"

    def _generate_ticket_search(self, intent, params):
        """Generate support ticket search SQL"""
        database = intent.get('target_database', 'support_db')
        status = params.get('status', 'open')
        return f"SELECT ticket_id, subject, status FROM {database}.support_tickets WHERE status = '{status}' LIMIT 10"

    def _generate_ticket_view(self, intent, params):
        """Generate support ticket view SQL"""
        database = intent.get('target_database', 'support_db')
        ticket_id = params.get('ticket_id', random.randint(1, 100))
        return f"SELECT * FROM {database}.support_tickets WHERE ticket_id = {ticket_id}"

    def _generate_ticket_create(self, intent, params):
        """Generate support ticket creation SQL"""
        database = intent.get('target_database', 'support_db')
        customer_id = params.get('customer_id', random.randint(1, 100))
        subject = params.get('subject', 'Vấn đề về sản phẩm')
        assigned_to = intent.get('user', 'support_agent')
        # Use safe SELECT instead of INSERT to avoid constraint issues
        return f"SELECT ticket_id, customer_id, subject, status FROM {database}.support_tickets WHERE assigned_to = '{assigned_to}' AND status = 'open' LIMIT 10"

    def _generate_ticket_update(self, intent, params):
        """Generate support ticket update SQL"""
        database = intent.get('target_database', 'support_db')
        ticket_id = params.get('ticket_id', random.randint(1, 100))  # Match actual data range
        status = params.get('status', 'in_progress')
        # Use more flexible conditions that are likely to match
        if random.random() < 0.7:  # 70% chance of using broader condition
            return f"UPDATE {database}.support_tickets SET status = '{status}' WHERE ticket_id <= {ticket_id} AND status IN ('open', 'in_progress') LIMIT 1"
        else:
            current_status = random.choice(['open', 'in_progress'])
            return f"UPDATE {database}.support_tickets SET status = '{status}' WHERE ticket_id = {ticket_id} AND status = '{current_status}'"

    def _generate_invoice_view(self, intent, params):
        """Generate invoice view SQL"""
        database = intent.get('target_database', 'finance_db')
        customer_id = params.get('customer_id', random.randint(1, 100))
        return f"SELECT * FROM {database}.invoices WHERE customer_id = {customer_id} ORDER BY invoice_date DESC LIMIT 10"

    def _generate_invoice_create(self, intent, params):
        """Generate invoice creation SQL"""
        database = intent.get('target_database', 'finance_db')
        customer_id = params.get('customer_id', random.randint(1, 100))
        amount = params.get('amount', random.randint(10000000, 50000000))
        # Use safe SELECT instead of INSERT to avoid constraint issues
        return f"SELECT invoice_id, customer_id, total_amount, status FROM {database}.invoices WHERE customer_id = {customer_id} LIMIT 10"

    def _generate_expense_view(self, intent, params):
        """Generate expense view SQL"""
        database = intent.get('target_database', 'finance_db')
        employee_id = intent.get('user', 'employee')
        return f"SELECT * FROM {database}.expense_reports WHERE category = 'travel' ORDER BY expense_date DESC LIMIT 10"

    def _generate_report_view(self, intent, params):
        """Generate report view SQL"""
        database = intent.get('target_database', 'finance_db')
        # Generate a safe financial report query
        return f"SELECT DATE_FORMAT(invoice_date, '%Y-%m') as month, SUM(total_amount) as monthly_revenue FROM {database}.invoices WHERE status = 'paid' GROUP BY DATE_FORMAT(invoice_date, '%Y-%m') ORDER BY month DESC LIMIT 12"

    def _generate_stock_check(self, intent, params):
        """Generate stock check SQL"""
        database = intent.get('target_database', 'inventory_db')
        product_id = params.get('product_id', random.randint(1, 500))
        return f"SELECT current_stock, available_stock FROM {database}.inventory_levels WHERE product_id = {product_id}"

    def _generate_inventory_update(self, intent, params):
        """Generate inventory update SQL"""
        database = intent.get('target_database', 'inventory_db')
        product_id = params.get('product_id', random.randint(1, 100))
        quantity = params.get('quantity', random.randint(10, 100))
        return f"UPDATE {database}.inventory_levels SET current_stock = current_stock + {quantity} WHERE product_id = {product_id}"

    def _generate_malicious_sql(self, intent, params):
        """Generate malicious SQL for security testing"""
        attack_type = intent.get('attack_chain', 'sql_injection')
        
        malicious_queries = {
            'sql_injection': [
                "SELECT * FROM customers WHERE customer_id = 1 OR 1=1--",
                "SELECT * FROM products; DROP TABLE products;--",
                "SELECT user(), version(), database()",
                "SELECT 1,2 UNION SELECT table_name, column_name FROM information_schema.columns--"
            ],
            'reconnaissance': [
                "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()",
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'customers'",
                "SHOW GRANTS FOR CURRENT_USER()",
                "SELECT @@version, @@hostname, USER()"
            ],
            'data_exfiltration': [
                "SELECT * FROM customers LIMIT 10000",
                "SELECT customer_id, company_name, contact_person, phone FROM customers",
                "SELECT * FROM orders WHERE total_amount > 10000000",
                "SELECT id, name, position FROM hr_db.employees"
            ],
            'privilege_escalation': [
                "SHOW GRANTS FOR CURRENT_USER()",
                "SELECT * FROM mysql.user",
                "SELECT * FROM information_schema.user_privileges",
                "SHOW DATABASES"
            ]
        }
        
        queries = malicious_queries.get(attack_type, malicious_queries['sql_injection'])
        return random.choice(queries)

# Backward compatibility
SQLTranslator = EnhancedSQLTranslator

# Example usage and testing
if __name__ == "__main__":
    translator = EnhancedSQLTranslator()
    
    print("🧪 TESTING ENHANCED SQL TRANSLATOR")
    print("=" * 50)
    
    # Test normal business operations
    test_intents = [
        {
            'user': 'nguyen_van_nam',
            'role': 'SALES',
            'action': 'SEARCH_CUSTOMER',
            'params': {},
            'target_database': 'sales_db',
            'is_anomaly': 0
        },
        {
            'user': 'tran_thi_lan',
            'role': 'HR',
            'action': 'VIEW_PROFILE',
            'params': {'employee_id': 123},
            'target_database': 'hr_db',
            'is_anomaly': 0
        },
        {
            'user': 'le_minh_duc',
            'role': 'MARKETING',
            'action': 'CREATE_LEAD',
            'params': {},
            'target_database': 'marketing_db',
            'is_anomaly': 0
        }
    ]
    
    for i, intent in enumerate(test_intents, 1):
        sql = translator.translate(intent)
        print(f"\n{i}. {intent['role']} - {intent['action']}:")
        print(f"   SQL: {sql}")
    
    # Test malicious intent
    malicious_intent = {
        'user': 'hacker',
        'role': 'ATTACKER',
        'action': 'SQLI_CLASSIC',
        'params': {},
        'target_database': 'sales_db',
        'is_anomaly': 1,
        'attack_chain': 'sql_injection'
    }
    
    sql = translator.translate(malicious_intent)
    print(f"\n🔴 Malicious SQL:")
    print(f"   SQL: {sql}")
    
    print(f"\n✅ Enhanced SQL translator ready for Vietnamese business simulation")