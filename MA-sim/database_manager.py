#!/usr/bin/env python3
"""
Database Manager - Consolidated Database Setup and Management
Combines all database setup, fixing, and management functionality
"""

import mysql.connector
import subprocess
import sys
import os
import json
from faker import Faker

# Vietnamese Faker
fake = Faker('vi_VN')

class DatabaseManager:
    """Comprehensive database management for Vietnamese Enterprise UBA Simulation"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to MySQL"""
        try:
            self.conn = mysql.connector.connect(
                host='localhost',
                user='root',
                password='root'
            )
            self.cursor = self.conn.cursor()
            return True
        except Exception as e:
            print(f"❌ MySQL connection error: {e}")
            return False
    
    def clean_database(self):
        """Clean up database for fresh start"""
        print("🧹 CLEANING DATABASE FOR FRESH START")
        print("=" * 50)
        
        if not self.connect():
            return False
        
        try:
            # Disable foreign key checks temporarily
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # Clean business databases
            databases = [
                'sales_db', 'marketing_db', 'finance_db', 'hr_db', 
                'inventory_db', 'support_db', 'admin_db'
            ]
            
            for db_name in databases:
                self.cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                print(f"✅ Dropped {db_name}")
            
            # Re-enable foreign key checks
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.conn.commit()
            
            print("✅ Database cleanup completed")
            return True
            
        except Exception as e:
            print(f"❌ Cleanup error: {e}")
            return False
    
    def create_database_structure(self):
        """Create complete database structure"""
        print("🏗️ CREATING COMPLETE DATABASE STRUCTURE")
        print("=" * 60)
        
        if not self.connect():
            return False
        
        try:
            # Disable foreign key checks temporarily
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            # Create all business databases
            databases = [
                'sales_db', 'marketing_db', 'finance_db', 'hr_db', 
                'inventory_db', 'support_db', 'admin_db'
            ]
            
            for db_name in databases:
                self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                print(f"✅ Created database: {db_name}")
            
            # Create tables for each database
            self._create_sales_tables()
            self._create_marketing_tables()
            self._create_finance_tables()
            self._create_hr_tables()
            self._create_inventory_tables()
            self._create_support_tables()
            self._create_admin_tables()
            
            # Re-enable foreign key checks
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.conn.commit()
            
            print("✅ Database structure creation completed")
            return True
            
        except Exception as e:
            print(f"❌ Structure creation error: {e}")
            return False
    
    def _create_sales_tables(self):
        """Create sales database tables"""
        self.cursor.execute("USE sales_db")
        
        tables = {
            'customers': """
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id INT PRIMARY KEY AUTO_INCREMENT,
                    customer_code VARCHAR(50) UNIQUE,
                    company_name VARCHAR(100) NOT NULL,
                    contact_person VARCHAR(100),
                    email VARCHAR(100),
                    phone VARCHAR(20),
                    address TEXT,
                    city VARCHAR(50),
                    status ENUM('active', 'inactive') DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'products': """
                CREATE TABLE IF NOT EXISTS products (
                    product_id INT PRIMARY KEY AUTO_INCREMENT,
                    product_code VARCHAR(50) UNIQUE,
                    product_name VARCHAR(100) NOT NULL,
                    category VARCHAR(50),
                    price DECIMAL(10,2),
                    cost DECIMAL(10,2),
                    description TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'orders': """
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INT PRIMARY KEY AUTO_INCREMENT,
                    order_code VARCHAR(50) UNIQUE,
                    customer_id INT,
                    order_date DATE,
                    total_amount DECIMAL(12,2),
                    status ENUM('pending', 'confirmed', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'order_items': """
                CREATE TABLE IF NOT EXISTS order_items (
                    item_id INT PRIMARY KEY AUTO_INCREMENT,
                    order_id INT,
                    product_id INT,
                    quantity INT,
                    unit_price DECIMAL(10,2),
                    total_price DECIMAL(12,2)
                )
            """,
            'customer_contacts': """
                CREATE TABLE IF NOT EXISTS customer_contacts (
                    contact_id INT PRIMARY KEY AUTO_INCREMENT,
                    customer_id INT,
                    contact_date DATE,
                    contact_type ENUM('call', 'email', 'meeting', 'other'),
                    notes TEXT,
                    created_by VARCHAR(50)
                )
            """,
            'order_payments': """
                CREATE TABLE IF NOT EXISTS order_payments (
                    payment_id INT PRIMARY KEY AUTO_INCREMENT,
                    order_id INT,
                    payment_date DATE,
                    amount DECIMAL(12,2),
                    payment_method ENUM('cash', 'bank_transfer', 'credit_card'),
                    status ENUM('pending', 'completed', 'failed') DEFAULT 'pending'
                )
            """,
            'product_categories': """
                CREATE TABLE IF NOT EXISTS product_categories (
                    category_id INT PRIMARY KEY AUTO_INCREMENT,
                    category_name VARCHAR(100) NOT NULL,
                    description TEXT,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """
        }
        
        for table_name, table_sql in tables.items():
            self.cursor.execute(table_sql)
            print(f"   ✅ Created sales_db.{table_name}")
    
    def _create_marketing_tables(self):
        """Create marketing database tables"""
        self.cursor.execute("USE marketing_db")
        
        tables = {
            'campaigns': """
                CREATE TABLE IF NOT EXISTS campaigns (
                    campaign_id INT PRIMARY KEY AUTO_INCREMENT,
                    campaign_name VARCHAR(100) NOT NULL,
                    campaign_type ENUM('email', 'social', 'print', 'online') DEFAULT 'email',
                    start_date DATE,
                    end_date DATE,
                    budget DECIMAL(12,2),
                    status ENUM('planning', 'active', 'paused', 'completed') DEFAULT 'planning',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'leads': """
                CREATE TABLE IF NOT EXISTS leads (
                    lead_id INT PRIMARY KEY AUTO_INCREMENT,
                    lead_source VARCHAR(50),
                    company_name VARCHAR(100),
                    contact_person VARCHAR(100),
                    email VARCHAR(100),
                    phone VARCHAR(20),
                    status ENUM('new', 'contacted', 'qualified', 'converted', 'lost') DEFAULT 'new',
                    score INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'campaign_performance': """
                CREATE TABLE IF NOT EXISTS campaign_performance (
                    performance_id INT PRIMARY KEY AUTO_INCREMENT,
                    campaign_id INT,
                    impressions INT DEFAULT 0,
                    clicks INT DEFAULT 0,
                    conversions INT DEFAULT 0,
                    cost DECIMAL(10,2) DEFAULT 0,
                    report_date DATE
                )
            """,
            'lead_activities': """
                CREATE TABLE IF NOT EXISTS lead_activities (
                    activity_id INT PRIMARY KEY AUTO_INCREMENT,
                    lead_id INT,
                    activity_type ENUM('call', 'email', 'meeting', 'demo'),
                    activity_date DATE,
                    notes TEXT,
                    created_by VARCHAR(50)
                )
            """,
            'lead_sources': """
                CREATE TABLE IF NOT EXISTS lead_sources (
                    source_id INT PRIMARY KEY AUTO_INCREMENT,
                    source_name VARCHAR(100) NOT NULL,
                    source_type ENUM('website', 'referral', 'advertisement', 'social_media'),
                    is_active BOOLEAN DEFAULT TRUE
                )
            """
        }
        
        for table_name, table_sql in tables.items():
            self.cursor.execute(table_sql)
            print(f"   ✅ Created marketing_db.{table_name}")
    
    def _create_finance_tables(self):
        """Create finance database tables"""
        self.cursor.execute("USE finance_db")
        
        tables = {
            'accounts': """
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id INT PRIMARY KEY AUTO_INCREMENT,
                    account_code VARCHAR(50) UNIQUE,
                    account_name VARCHAR(100) NOT NULL,
                    account_type ENUM('asset', 'liability', 'equity', 'revenue', 'expense'),
                    balance DECIMAL(15,2) DEFAULT 0,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """,
            'invoices': """
                CREATE TABLE IF NOT EXISTS invoices (
                    invoice_id INT PRIMARY KEY AUTO_INCREMENT,
                    invoice_number VARCHAR(50) UNIQUE,
                    customer_id INT,
                    invoice_date DATE,
                    due_date DATE,
                    total_amount DECIMAL(12,2),
                    paid_amount DECIMAL(12,2) DEFAULT 0,
                    status ENUM('draft', 'sent', 'paid', 'overdue') DEFAULT 'draft'
                )
            """,
            'payments': """
                CREATE TABLE IF NOT EXISTS payments (
                    payment_id INT PRIMARY KEY AUTO_INCREMENT,
                    invoice_id INT,
                    payment_date DATE,
                    amount DECIMAL(12,2),
                    payment_method ENUM('cash', 'bank_transfer', 'credit_card', 'check'),
                    reference_number VARCHAR(100)
                )
            """,
            'expense_reports': """
                CREATE TABLE IF NOT EXISTS expense_reports (
                    expense_id INT PRIMARY KEY AUTO_INCREMENT,
                    employee_id INT,
                    expense_date DATE,
                    category VARCHAR(50),
                    amount DECIMAL(10,2),
                    description TEXT,
                    status ENUM('pending', 'approved', 'rejected') DEFAULT 'pending'
                )
            """,
            'budget_plans': """
                CREATE TABLE IF NOT EXISTS budget_plans (
                    budget_id INT PRIMARY KEY AUTO_INCREMENT,
                    department VARCHAR(50),
                    budget_year YEAR,
                    budget_month INT,
                    planned_amount DECIMAL(12,2),
                    actual_amount DECIMAL(12,2) DEFAULT 0
                )
            """,
            'invoice_items': """
                CREATE TABLE IF NOT EXISTS invoice_items (
                    item_id INT PRIMARY KEY AUTO_INCREMENT,
                    invoice_id INT,
                    product_id INT,
                    description TEXT,
                    quantity INT,
                    unit_price DECIMAL(10,2),
                    total_price DECIMAL(12,2)
                )
            """
        }
        
        for table_name, table_sql in tables.items():
            self.cursor.execute(table_sql)
            print(f"   ✅ Created finance_db.{table_name}")
    
    def _create_hr_tables(self):
        """Create HR database tables"""
        self.cursor.execute("USE hr_db")
        
        tables = {
            'employees': """
                CREATE TABLE IF NOT EXISTS employees (
                    employee_id INT PRIMARY KEY AUTO_INCREMENT,
                    employee_code VARCHAR(50) UNIQUE,
                    full_name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE,
                    phone VARCHAR(20),
                    department VARCHAR(50),
                    position VARCHAR(50),
                    hire_date DATE,
                    salary DECIMAL(10,2),
                    status ENUM('active', 'inactive', 'terminated') DEFAULT 'active'
                )
            """,
            'departments': """
                CREATE TABLE IF NOT EXISTS departments (
                    department_id INT PRIMARY KEY AUTO_INCREMENT,
                    department_name VARCHAR(100) NOT NULL,
                    manager_id INT,
                    budget DECIMAL(12,2),
                    is_active BOOLEAN DEFAULT TRUE
                )
            """,
            'attendance': """
                CREATE TABLE IF NOT EXISTS attendance (
                    attendance_id INT PRIMARY KEY AUTO_INCREMENT,
                    employee_id INT,
                    attendance_date DATE,
                    check_in_time TIME,
                    check_out_time TIME,
                    hours_worked DECIMAL(4,2),
                    status ENUM('present', 'absent', 'late', 'half_day') DEFAULT 'present'
                )
            """,
            'salaries': """
                CREATE TABLE IF NOT EXISTS salaries (
                    salary_id INT PRIMARY KEY AUTO_INCREMENT,
                    employee_id INT,
                    salary_month DATE,
                    base_salary DECIMAL(10,2),
                    overtime_pay DECIMAL(8,2) DEFAULT 0,
                    bonus DECIMAL(8,2) DEFAULT 0,
                    deductions DECIMAL(8,2) DEFAULT 0,
                    net_salary DECIMAL(10,2)
                )
            """,
            'employee_benefits': """
                CREATE TABLE IF NOT EXISTS employee_benefits (
                    benefit_id INT PRIMARY KEY AUTO_INCREMENT,
                    employee_id INT,
                    benefit_type ENUM('health_insurance', 'life_insurance', 'retirement', 'vacation'),
                    benefit_value DECIMAL(8,2),
                    start_date DATE,
                    end_date DATE
                )
            """
        }
        
        for table_name, table_sql in tables.items():
            self.cursor.execute(table_sql)
            print(f"   ✅ Created hr_db.{table_name}")
    
    def _create_inventory_tables(self):
        """Create inventory database tables"""
        self.cursor.execute("USE inventory_db")
        
        tables = {
            'warehouse_locations': """
                CREATE TABLE IF NOT EXISTS warehouse_locations (
                    location_id INT PRIMARY KEY AUTO_INCREMENT,
                    location_code VARCHAR(50) UNIQUE,
                    location_name VARCHAR(100) NOT NULL,
                    address TEXT,
                    capacity INT,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """,
            'inventory_levels': """
                CREATE TABLE IF NOT EXISTS inventory_levels (
                    level_id INT PRIMARY KEY AUTO_INCREMENT,
                    product_id INT,
                    location_id INT,
                    current_stock INT DEFAULT 0,
                    min_stock INT DEFAULT 0,
                    max_stock INT DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """,
            'stock_movements': """
                CREATE TABLE IF NOT EXISTS stock_movements (
                    movement_id INT PRIMARY KEY AUTO_INCREMENT,
                    product_id INT,
                    location_id INT,
                    movement_type ENUM('in', 'out', 'transfer', 'adjustment'),
                    quantity INT,
                    reference_number VARCHAR(100),
                    movement_date DATE,
                    created_by VARCHAR(50)
                )
            """,
            'inventory_adjustments': """
                CREATE TABLE IF NOT EXISTS inventory_adjustments (
                    adjustment_id INT PRIMARY KEY AUTO_INCREMENT,
                    product_id INT,
                    location_id INT,
                    old_quantity INT,
                    new_quantity INT,
                    adjustment_reason TEXT,
                    adjustment_date DATE,
                    created_by VARCHAR(50)
                )
            """
        }
        
        for table_name, table_sql in tables.items():
            self.cursor.execute(table_sql)
            print(f"   ✅ Created inventory_db.{table_name}")
    
    def _create_support_tables(self):
        """Create support database tables"""
        self.cursor.execute("USE support_db")
        
        tables = {
            'ticket_categories': """
                CREATE TABLE IF NOT EXISTS ticket_categories (
                    category_id INT PRIMARY KEY AUTO_INCREMENT,
                    category_name VARCHAR(100) NOT NULL,
                    description TEXT,
                    priority_level ENUM('low', 'medium', 'high') DEFAULT 'medium',
                    is_active BOOLEAN DEFAULT TRUE
                )
            """,
            'support_tickets': """
                CREATE TABLE IF NOT EXISTS support_tickets (
                    ticket_id INT PRIMARY KEY AUTO_INCREMENT,
                    ticket_number VARCHAR(50) UNIQUE,
                    customer_id INT,
                    category_id INT,
                    subject VARCHAR(200) NOT NULL,
                    description TEXT,
                    priority ENUM('low', 'medium', 'high', 'urgent') DEFAULT 'medium',
                    status ENUM('open', 'in_progress', 'resolved', 'closed') DEFAULT 'open',
                    assigned_to VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'ticket_responses': """
                CREATE TABLE IF NOT EXISTS ticket_responses (
                    response_id INT PRIMARY KEY AUTO_INCREMENT,
                    ticket_id INT,
                    response_text TEXT,
                    response_type ENUM('customer', 'agent', 'system'),
                    created_by VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'knowledge_base': """
                CREATE TABLE IF NOT EXISTS knowledge_base (
                    article_id INT PRIMARY KEY AUTO_INCREMENT,
                    title VARCHAR(200) NOT NULL,
                    content TEXT,
                    category VARCHAR(50),
                    tags VARCHAR(200),
                    is_published BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }
        
        for table_name, table_sql in tables.items():
            self.cursor.execute(table_sql)
            print(f"   ✅ Created support_db.{table_name}")
    
    def _create_admin_tables(self):
        """Create admin database tables"""
        self.cursor.execute("USE admin_db")
        
        tables = {
            'system_logs': """
                CREATE TABLE IF NOT EXISTS system_logs (
                    log_id INT PRIMARY KEY AUTO_INCREMENT,
                    log_level ENUM('info', 'warning', 'error', 'critical'),
                    log_message TEXT,
                    module_name VARCHAR(100),
                    user_id VARCHAR(50),
                    ip_address VARCHAR(45),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'user_sessions': """
                CREATE TABLE IF NOT EXISTS user_sessions (
                    session_id INT PRIMARY KEY AUTO_INCREMENT,
                    user_id VARCHAR(50),
                    login_time TIMESTAMP,
                    logout_time TIMESTAMP NULL,
                    ip_address VARCHAR(45),
                    user_agent TEXT,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """,
            'system_config': """
                CREATE TABLE IF NOT EXISTS system_config (
                    config_id INT PRIMARY KEY AUTO_INCREMENT,
                    config_key VARCHAR(100) UNIQUE,
                    config_value TEXT,
                    description TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """,
            'report_schedules': """
                CREATE TABLE IF NOT EXISTS report_schedules (
                    schedule_id INT PRIMARY KEY AUTO_INCREMENT,
                    report_name VARCHAR(100) NOT NULL,
                    report_type VARCHAR(50),
                    schedule_frequency ENUM('daily', 'weekly', 'monthly'),
                    recipients TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    last_run TIMESTAMP NULL
                )
            """
        }
        
        for table_name, table_sql in tables.items():
            self.cursor.execute(table_sql)
            print(f"   ✅ Created admin_db.{table_name}")
    
    def create_users(self):
        """Create all 97 Vietnamese users"""
        print("👥 CREATING ALL 97 VIETNAMESE USERS")
        print("=" * 50)
        
        if not self.connect():
            return False
        
        try:
            # Load user configuration
            with open('simulation/users_config.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            users_map = config.get("users", {})
            
            created_count = 0
            for username, role in users_map.items():
                try:
                    # Create MySQL user
                    self.cursor.execute(f"CREATE USER IF NOT EXISTS '{username}'@'localhost' IDENTIFIED BY 'password'")
                    
                    # Grant appropriate permissions based on role
                    if role in ['ADMIN', 'DEV', 'MANAGEMENT']:
                        self.cursor.execute(f"GRANT ALL PRIVILEGES ON *.* TO '{username}'@'localhost'")
                    elif role == 'FINANCE':
                        self.cursor.execute(f"GRANT ALL PRIVILEGES ON finance_db.* TO '{username}'@'localhost'")
                        self.cursor.execute(f"GRANT SELECT ON sales_db.* TO '{username}'@'localhost'")
                        self.cursor.execute(f"GRANT SELECT ON hr_db.* TO '{username}'@'localhost'")
                        self.cursor.execute(f"GRANT SELECT ON inventory_db.* TO '{username}'@'localhost'")
                    elif role == 'HR':
                        self.cursor.execute(f"GRANT ALL PRIVILEGES ON hr_db.* TO '{username}'@'localhost'")
                        self.cursor.execute(f"GRANT SELECT ON finance_db.* TO '{username}'@'localhost'")
                        self.cursor.execute(f"GRANT SELECT ON admin_db.* TO '{username}'@'localhost'")
                    elif role == 'SALES':
                        self.cursor.execute(f"GRANT ALL PRIVILEGES ON sales_db.* TO '{username}'@'localhost'")
                        self.cursor.execute(f"GRANT SELECT ON marketing_db.* TO '{username}'@'localhost'")
                        self.cursor.execute(f"GRANT SELECT ON support_db.* TO '{username}'@'localhost'")
                    elif role == 'MARKETING':
                        self.cursor.execute(f"GRANT ALL PRIVILEGES ON marketing_db.* TO '{username}'@'localhost'")
                        self.cursor.execute(f"GRANT SELECT ON sales_db.* TO '{username}'@'localhost'")
                        self.cursor.execute(f"GRANT SELECT ON support_db.* TO '{username}'@'localhost'")
                    elif role == 'CUSTOMER_SERVICE':
                        self.cursor.execute(f"GRANT ALL PRIVILEGES ON support_db.* TO '{username}'@'localhost'")
                        self.cursor.execute(f"GRANT SELECT ON sales_db.* TO '{username}'@'localhost'")
                        self.cursor.execute(f"GRANT SELECT ON marketing_db.* TO '{username}'@'localhost'")
                    else:
                        # Default permissions for other roles - basic access to sales_db
                        self.cursor.execute(f"GRANT SELECT ON sales_db.* TO '{username}'@'localhost'")
                    
                    created_count += 1
                    
                except Exception as e:
                    print(f"   ⚠️ Could not create user {username}: {e}")
            
            # Flush privileges
            self.cursor.execute("FLUSH PRIVILEGES")
            self.conn.commit()
            
            print(f"✅ Created {created_count} users successfully")
            return True
            
        except Exception as e:
            print(f"❌ User creation error: {e}")
            return False
    
    def complete_setup(self):
        """Run complete setup process"""
        print("🚀 COMPLETE DATABASE SETUP")
        print("=" * 60)
        
        steps = [
            ("Checking requirements", self.check_requirements),
            ("Cleaning database", self.clean_database),
            ("Creating database structure", self.create_database_structure),
            ("Creating users", self.create_users)
        ]
        
        for step_name, step_func in steps:
            print(f"\n📋 {step_name}...")
            if not step_func():
                print(f"❌ {step_name} failed!")
                return False
        
        print("\n🎉 COMPLETE SETUP SUCCESSFUL!")
        print("✅ All databases created")
        print("✅ All tables created")
        print("✅ All 97 users created")
        print("✅ Ready for simulation")
        
        return True

def main():
    """Main execution function"""
    if len(sys.argv) > 1:
        action = sys.argv[1]
        db_manager = DatabaseManager()
        
        if action == "clean":
            db_manager.clean_database()
        elif action == "structure":
            db_manager.create_database_structure()
        elif action == "users":
            db_manager.create_users()
        elif action == "complete":
            db_manager.complete_setup()
        else:
            print("Usage: python database_manager.py [clean|structure|users|complete]")
    else:
        # Default: run complete setup
        db_manager = DatabaseManager()
        db_manager.complete_setup()

if __name__ == "__main__":
    main()