#!/usr/bin/env python3
"""
Corrected Enhanced SQL Library - Uses only existing tables
Eliminates ALL table existence errors
"""

class CorrectedEnhancedSQLLibrary:
    """Corrected SQL library that only uses existing tables"""
    
    def __init__(self):
        # Actual table mappings from database
        self.database_tables = {'sales_db': ['customer_contacts', 'customers', 'order_items', 'order_payments', 'orders', 'product_categories', 'products'], 'inventory_db': ['inventory_adjustments', 'inventory_levels', 'stock_movements', 'warehouse_locations'], 'finance_db': ['accounts', 'budget_plans', 'expense_reports', 'invoice_items', 'invoices', 'payments'], 'marketing_db': ['campaign_performance', 'campaigns', 'lead_activities', 'lead_sources', 'leads'], 'support_db': ['knowledge_base', 'support_tickets', 'ticket_categories', 'ticket_responses'], 'hr_db': ['attendance', 'departments', 'employee_benefits', 'employees', 'salaries'], 'admin_db': ['report_schedules', 'system_config', 'system_logs', 'user_sessions']}
        
        # Role-based database access
        self.role_database_access = {
            'SALES': ['sales_db', 'marketing_db', 'support_db'],
            'MARKETING': ['marketing_db', 'sales_db', 'support_db'],
            'CUSTOMER_SERVICE': ['support_db', 'sales_db', 'marketing_db'],
            'HR': ['hr_db', 'finance_db', 'admin_db'],
            'FINANCE': ['finance_db', 'sales_db', 'hr_db', 'inventory_db'],
            'DEV': list(self.database_tables.keys()),
            'MANAGEMENT': list(self.database_tables.keys()),
            'ADMIN': list(self.database_tables.keys()),
        }
    
    def get_queries_by_database_and_role(self, database, role, complexity='ALL'):
        """Get queries that only use existing tables"""
        
        # Check permissions
        if database not in self.role_database_access.get(role, []):
            return []
        
        # Check if database exists
        if database not in self.database_tables:
            return []
        
        available_tables = self.database_tables[database]
        if not available_tables:
            return []
        
        queries = []
        
        # Generate safe queries using only existing tables
        if database == 'sales_db':
            if 'customers' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.customers",
                    f"SELECT customer_id, company_name, city FROM {database}.customers WHERE status = 'active' LIMIT 10",
                    f"SELECT city, COUNT(*) as customer_count FROM {database}.customers GROUP BY city LIMIT 10"
                ])
            if 'orders' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.orders",
                    f"SELECT order_id, total_amount FROM {database}.orders LIMIT 10"
                ])
            if 'products' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.products",
                    f"SELECT product_name, price FROM {database}.products LIMIT 10"
                ])
        
        elif database == 'hr_db':
            if 'employees' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.employees",
                    f"SELECT name, position FROM {database}.employees LIMIT 10",
                    f"SELECT position, COUNT(*) as count FROM {database}.employees GROUP BY position LIMIT 10"
                ])
            if 'departments' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.departments",
                    f"SELECT dept_name FROM {database}.departments LIMIT 10"
                ])
            if 'salaries' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.salaries",
                    f"SELECT employee_id, amount FROM {database}.salaries LIMIT 10"
                ])
            if 'attendance' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.attendance",
                    f"SELECT employee_id, date, status FROM {database}.attendance WHERE date >= CURDATE() - INTERVAL 7 DAY LIMIT 10"
                ])
            if 'employee_benefits' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.employee_benefits",
                    f"SELECT benefit_type, COUNT(*) as count FROM {database}.employee_benefits GROUP BY benefit_type LIMIT 10"
                ])
        
        elif database == 'admin_db':
            if 'system_logs' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.system_logs",
                    f"SELECT log_level, COUNT(*) as log_count FROM {database}.system_logs GROUP BY log_level LIMIT 10",
                    f"SELECT module, COUNT(*) as module_logs FROM {database}.system_logs WHERE created_at >= CURDATE() - INTERVAL 7 DAY GROUP BY module LIMIT 10"
                ])
            if 'user_sessions' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.user_sessions",
                    f"SELECT user_id, login_time FROM {database}.user_sessions WHERE is_active = TRUE LIMIT 10"
                ])
            if 'report_schedules' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.report_schedules",
                    f"SELECT report_name, schedule_frequency FROM {database}.report_schedules WHERE is_active = TRUE LIMIT 10"
                ])
            if 'system_config' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.system_config",
                    f"SELECT config_key, config_value FROM {database}.system_config WHERE is_active = TRUE LIMIT 10"
                ])
        
        elif database == 'inventory_db':
            if 'inventory_levels' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.inventory_levels",
                    f"SELECT product_id, current_stock FROM {database}.inventory_levels WHERE current_stock > 0 LIMIT 10"
                ])
            if 'warehouse_locations' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.warehouse_locations",
                    f"SELECT warehouse_name, city FROM {database}.warehouse_locations LIMIT 10"
                ])
            if 'stock_movements' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.stock_movements",
                    f"SELECT movement_type, COUNT(*) as count FROM {database}.stock_movements GROUP BY movement_type LIMIT 10"
                ])
            if 'inventory_adjustments' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.inventory_adjustments",
                    f"SELECT reason, COUNT(*) as count FROM {database}.inventory_adjustments GROUP BY reason LIMIT 10"
                ])
        
        elif database == 'finance_db':
            if 'invoices' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.invoices",
                    f"SELECT invoice_number, total_amount FROM {database}.invoices LIMIT 10"
                ])
            if 'expense_reports' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.expense_reports",
                    f"SELECT category, SUM(amount) as total FROM {database}.expense_reports GROUP BY category LIMIT 10"
                ])
            if 'accounts' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.accounts",
                    f"SELECT account_name, account_type FROM {database}.accounts LIMIT 10"
                ])
            if 'budget_plans' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.budget_plans",
                    f"SELECT department, planned_amount FROM {database}.budget_plans LIMIT 10"
                ])
            if 'payments' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.payments",
                    f"SELECT payment_method, COUNT(*) as count FROM {database}.payments GROUP BY payment_method LIMIT 10"
                ])
            if 'invoice_items' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.invoice_items",
                    f"SELECT description, quantity, unit_price FROM {database}.invoice_items LIMIT 10"
                ])
        
        elif database == 'marketing_db':
            if 'campaigns' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.campaigns",
                    f"SELECT campaign_name FROM {database}.campaigns LIMIT 10"
                ])
            if 'leads' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.leads",
                    f"SELECT status, COUNT(*) as count FROM {database}.leads GROUP BY status LIMIT 10"
                ])
            if 'lead_activities' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.lead_activities",
                    f"SELECT activity_type, COUNT(*) as count FROM {database}.lead_activities GROUP BY activity_type LIMIT 10"
                ])
            if 'campaign_performance' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.campaign_performance",
                    f"SELECT campaign_id, SUM(impressions) as total_impressions FROM {database}.campaign_performance GROUP BY campaign_id LIMIT 10"
                ])
            if 'lead_sources' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.lead_sources",
                    f"SELECT source_name, source_type FROM {database}.lead_sources WHERE is_active = TRUE LIMIT 10"
                ])
        
        elif database == 'support_db':
            if 'support_tickets' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.support_tickets",
                    f"SELECT ticket_number, priority FROM {database}.support_tickets LIMIT 10",
                    f"SELECT priority, COUNT(*) as count FROM {database}.support_tickets GROUP BY priority LIMIT 10"
                ])
            if 'ticket_responses' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.ticket_responses",
                    f"SELECT ticket_id, created_by FROM {database}.ticket_responses LIMIT 10"
                ])
            if 'knowledge_base' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.knowledge_base",
                    f"SELECT title, category FROM {database}.knowledge_base LIMIT 10"
                ])
            if 'ticket_categories' in available_tables:
                queries.extend([
                    f"SELECT COUNT(*) FROM {database}.ticket_categories",
                    f"SELECT category_name, priority_level FROM {database}.ticket_categories WHERE is_active = TRUE LIMIT 10"
                ])
        
        # Format queries with actual database name
        formatted_queries = []
        for query in queries:
            formatted_queries.append(query.format(database=database))
        
        return formatted_queries
    
    def get_malicious_queries_enriched(self, attack_type='sql_injection'):
        """Get malicious queries for security testing"""
        malicious_queries = []
        
        if attack_type == 'sql_injection':
            malicious_queries.extend([
                "SELECT * FROM sales_db.customers WHERE customer_id = 1 OR 1=1--",
                "SELECT user(), version(), database()",
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'sales_db'",
                "UNION SELECT username, password FROM mysql.user--"
            ])
        
        elif attack_type == 'privilege_escalation':
            malicious_queries.extend([
                "SHOW GRANTS FOR CURRENT_USER()",
                "SELECT * FROM information_schema.user_privileges",
                "SHOW DATABASES",
                "SELECT * FROM mysql.user WHERE user = 'root'"
            ])
        
        elif attack_type == 'data_exfiltration':
            malicious_queries.extend([
                "SELECT * FROM sales_db.customers LIMIT 1000",
                "SELECT customer_id, company_name, email FROM sales_db.customers",
                "SELECT name, salary FROM hr_db.employees",
                "SELECT * FROM finance_db.invoices WHERE total_amount > 1000000"
            ])
        
        return malicious_queries
    
    def get_safe_query_for_role_and_database(self, role, database):
        """Get a guaranteed safe query for any role/database combination"""
        
        # Check permissions first
        if database not in self.role_database_access.get(role, []):
            return "SELECT 'Access Denied' as message"
        
        # Get a safe query using actual tables
        if database in self.database_tables and self.database_tables[database]:
            first_table = self.database_tables[database][0]
            return f"SELECT COUNT(*) FROM {database}.{first_table}".format(database=database, first_table=first_table)
        
        # Fallback
        return "SELECT 1 as test_query"

# Create corrected instance for import
CORRECTED_SQL_LIBRARY = CorrectedEnhancedSQLLibrary()
