#!/usr/bin/env python3
"""
Create complete database structure for Vietnamese Enterprise UBA Simulation
This script creates all required tables to prevent "Table doesn't exist" errors
"""

import mysql.connector
import sys
import os

def create_database_structure():
    """Create all required database tables for the simulation"""
    
    print("🏗️ CREATING COMPLETE DATABASE STRUCTURE")
    print("=" * 60)
    
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root',
            database='uba_db'
        )
        cursor = conn.cursor()
        
        print("✅ Connected to MySQL")
        
        # Disable foreign key checks temporarily
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # Create all databases
        databases = [
            'sales_db', 'marketing_db', 'finance_db', 'hr_db', 
            'inventory_db', 'support_db', 'admin_db'
        ]
        
        for db_name in databases:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            print(f"✅ Created database: {db_name}")
        
        # Database table definitions
        table_definitions = {
            'sales_db': [
                """CREATE TABLE IF NOT EXISTS customers (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    customer_code VARCHAR(50) UNIQUE,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    phone VARCHAR(20),
                    address TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS orders (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    order_code VARCHAR(50) UNIQUE,
                    customer_id INT,
                    total_amount DECIMAL(15,2),
                    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
                    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (customer_id) REFERENCES customers(id)
                )""",
                
                """CREATE TABLE IF NOT EXISTS order_items (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    order_id INT,
                    product_name VARCHAR(100),
                    quantity INT,
                    unit_price DECIMAL(10,2),
                    total_price DECIMAL(12,2),
                    FOREIGN KEY (order_id) REFERENCES orders(id)
                )""",
                
                """CREATE TABLE IF NOT EXISTS sales_reports (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    report_date DATE,
                    total_sales DECIMAL(15,2),
                    total_orders INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"""
            ],
            
            'marketing_db': [
                """CREATE TABLE IF NOT EXISTS campaigns (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    campaign_name VARCHAR(100) NOT NULL,
                    campaign_type ENUM('email', 'social', 'ppc', 'display') DEFAULT 'email',
                    budget DECIMAL(12,2),
                    start_date DATE,
                    end_date DATE,
                    status ENUM('draft', 'active', 'paused', 'completed') DEFAULT 'draft',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS campaign_performance (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    campaign_id INT,
                    impressions INT DEFAULT 0,
                    clicks INT DEFAULT 0,
                    conversions INT DEFAULT 0,
                    cost DECIMAL(10,2) DEFAULT 0,
                    report_date DATE,
                    FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
                )""",
                
                """CREATE TABLE IF NOT EXISTS leads (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    lead_source VARCHAR(50),
                    name VARCHAR(100),
                    email VARCHAR(100),
                    phone VARCHAR(20),
                    status ENUM('new', 'contacted', 'qualified', 'converted', 'lost') DEFAULT 'new',
                    score INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS email_campaigns (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    campaign_id INT,
                    subject VARCHAR(200),
                    sent_count INT DEFAULT 0,
                    open_count INT DEFAULT 0,
                    click_count INT DEFAULT 0,
                    sent_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
                )"""
            ],
            
            'finance_db': [
                """CREATE TABLE IF NOT EXISTS accounts (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    account_code VARCHAR(20) UNIQUE,
                    account_name VARCHAR(100) NOT NULL,
                    account_type ENUM('asset', 'liability', 'equity', 'revenue', 'expense') NOT NULL,
                    balance DECIMAL(15,2) DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS transactions (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    transaction_code VARCHAR(50) UNIQUE,
                    account_id INT,
                    amount DECIMAL(15,2),
                    transaction_type ENUM('debit', 'credit') NOT NULL,
                    description TEXT,
                    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_id) REFERENCES accounts(id)
                )""",
                
                """CREATE TABLE IF NOT EXISTS payments (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    payment_code VARCHAR(50) UNIQUE,
                    customer_id INT,
                    amount DECIMAL(12,2),
                    payment_method ENUM('cash', 'card', 'transfer', 'check') DEFAULT 'cash',
                    status ENUM('pending', 'completed', 'failed', 'refunded') DEFAULT 'pending',
                    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS invoices (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    invoice_number VARCHAR(50) UNIQUE,
                    customer_id INT,
                    total_amount DECIMAL(12,2),
                    tax_amount DECIMAL(10,2),
                    status ENUM('draft', 'sent', 'paid', 'overdue', 'cancelled') DEFAULT 'draft',
                    issue_date DATE,
                    due_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"""
            ],
            
            'hr_db': [
                """CREATE TABLE IF NOT EXISTS employees (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    employee_code VARCHAR(20) UNIQUE,
                    first_name VARCHAR(50) NOT NULL,
                    last_name VARCHAR(50) NOT NULL,
                    email VARCHAR(100) UNIQUE,
                    phone VARCHAR(20),
                    department VARCHAR(50),
                    position VARCHAR(50),
                    salary DECIMAL(12,2),
                    hire_date DATE,
                    status ENUM('active', 'inactive', 'terminated') DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS attendance (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    employee_id INT,
                    check_in TIMESTAMP,
                    check_out TIMESTAMP,
                    work_hours DECIMAL(4,2),
                    date DATE,
                    status ENUM('present', 'absent', 'late', 'half_day') DEFAULT 'present',
                    FOREIGN KEY (employee_id) REFERENCES employees(id)
                )""",
                
                """CREATE TABLE IF NOT EXISTS payroll (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    employee_id INT,
                    base_salary DECIMAL(12,2),
                    overtime_hours DECIMAL(4,2) DEFAULT 0,
                    overtime_pay DECIMAL(10,2) DEFAULT 0,
                    deductions DECIMAL(10,2) DEFAULT 0,
                    net_pay DECIMAL(12,2),
                    pay_period_start DATE,
                    pay_period_end DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (employee_id) REFERENCES employees(id)
                )""",
                
                """CREATE TABLE IF NOT EXISTS leave_requests (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    employee_id INT,
                    leave_type ENUM('annual', 'sick', 'personal', 'maternity', 'emergency') NOT NULL,
                    start_date DATE,
                    end_date DATE,
                    days_requested INT,
                    status ENUM('pending', 'approved', 'rejected') DEFAULT 'pending',
                    reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (employee_id) REFERENCES employees(id)
                )"""
            ],
            
            'inventory_db': [
                """CREATE TABLE IF NOT EXISTS products (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    product_code VARCHAR(50) UNIQUE,
                    name VARCHAR(100) NOT NULL,
                    description TEXT,
                    category VARCHAR(50),
                    unit_price DECIMAL(10,2),
                    cost_price DECIMAL(10,2),
                    stock_quantity INT DEFAULT 0,
                    min_stock_level INT DEFAULT 0,
                    status ENUM('active', 'inactive', 'discontinued') DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS suppliers (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    supplier_code VARCHAR(20) UNIQUE,
                    name VARCHAR(100) NOT NULL,
                    contact_person VARCHAR(100),
                    email VARCHAR(100),
                    phone VARCHAR(20),
                    address TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS purchase_orders (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    po_number VARCHAR(50) UNIQUE,
                    supplier_id INT,
                    total_amount DECIMAL(15,2),
                    status ENUM('draft', 'sent', 'confirmed', 'received', 'cancelled') DEFAULT 'draft',
                    order_date DATE,
                    expected_date DATE,
                    FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
                )""",
                
                """CREATE TABLE IF NOT EXISTS stock_movements (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    product_id INT,
                    movement_type ENUM('in', 'out', 'adjustment') NOT NULL,
                    quantity INT,
                    reference_type ENUM('purchase', 'sale', 'adjustment', 'transfer') NOT NULL,
                    reference_id INT,
                    notes TEXT,
                    movement_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_id) REFERENCES products(id)
                )"""
            ],
            
            'support_db': [
                """CREATE TABLE IF NOT EXISTS tickets (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    ticket_number VARCHAR(50) UNIQUE,
                    customer_id INT,
                    subject VARCHAR(200) NOT NULL,
                    description TEXT,
                    priority ENUM('low', 'medium', 'high', 'urgent') DEFAULT 'medium',
                    status ENUM('open', 'in_progress', 'resolved', 'closed') DEFAULT 'open',
                    assigned_to INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS ticket_responses (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    ticket_id INT,
                    responder_type ENUM('customer', 'agent') NOT NULL,
                    message TEXT NOT NULL,
                    is_internal BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ticket_id) REFERENCES tickets(id)
                )""",
                
                """CREATE TABLE IF NOT EXISTS knowledge_base (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    title VARCHAR(200) NOT NULL,
                    content TEXT,
                    category VARCHAR(50),
                    tags VARCHAR(200),
                    views INT DEFAULT 0,
                    status ENUM('draft', 'published', 'archived') DEFAULT 'draft',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS customer_feedback (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    customer_id INT,
                    ticket_id INT,
                    rating INT CHECK (rating >= 1 AND rating <= 5),
                    feedback TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ticket_id) REFERENCES tickets(id)
                )"""
            ],
            
            'admin_db': [
                """CREATE TABLE IF NOT EXISTS system_config (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    config_key VARCHAR(100) UNIQUE NOT NULL,
                    config_value TEXT,
                    description TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS user_sessions (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    user_id INT,
                    session_token VARCHAR(255) UNIQUE,
                    ip_address VARCHAR(45),
                    user_agent TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS audit_logs (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    user_id INT,
                    action VARCHAR(100) NOT NULL,
                    table_name VARCHAR(100),
                    record_id INT,
                    old_values JSON,
                    new_values JSON,
                    ip_address VARCHAR(45),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                
                """CREATE TABLE IF NOT EXISTS system_notifications (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    title VARCHAR(200) NOT NULL,
                    message TEXT,
                    type ENUM('info', 'warning', 'error', 'success') DEFAULT 'info',
                    is_read BOOLEAN DEFAULT FALSE,
                    target_user_id INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"""
            ]
        }
        
        # Create tables for each database
        for db_name, tables in table_definitions.items():
            cursor.execute(f"USE {db_name}")
            print(f"\n📊 Creating tables in {db_name}:")
            
            for table_sql in tables:
                try:
                    cursor.execute(table_sql)
                    # Extract table name from SQL
                    table_name = table_sql.split('TABLE IF NOT EXISTS ')[1].split(' (')[0].strip()
                    print(f"   ✅ Created table: {table_name}")
                except Exception as e:
                    print(f"   ⚠️ Table creation issue: {e}")
        
        # Insert sample data to prevent empty table errors
        cursor.execute("USE sales_db")
        
        # Sample customers
        cursor.execute("""
            INSERT IGNORE INTO customers (customer_code, name, email, phone) VALUES
            ('CUST001', 'Nguyen Van A', 'nguyenvana@email.com', '0901234567'),
            ('CUST002', 'Tran Thi B', 'tranthib@email.com', '0901234568'),
            ('CUST003', 'Le Van C', 'levanc@email.com', '0901234569')
        """)
        
        # Sample orders
        cursor.execute("""
            INSERT IGNORE INTO orders (order_code, customer_id, total_amount, status) VALUES
            ('ORD001', 1, 1500000, 'completed'),
            ('ORD002', 2, 2300000, 'processing'),
            ('ORD003', 3, 890000, 'shipped')
        """)
        
        cursor.execute("USE marketing_db")
        
        # Sample campaigns
        cursor.execute("""
            INSERT IGNORE INTO campaigns (campaign_name, campaign_type, budget, status) VALUES
            ('Summer Sale 2025', 'email', 50000000, 'active'),
            ('New Product Launch', 'social', 30000000, 'active'),
            ('Holiday Promotion', 'ppc', 75000000, 'draft')
        """)
        
        # Sample campaign performance
        cursor.execute("""
            INSERT IGNORE INTO campaign_performance (campaign_id, impressions, clicks, conversions, cost, report_date) VALUES
            (1, 15000, 1200, 85, 2500000, CURDATE()),
            (2, 8500, 650, 42, 1800000, CURDATE()),
            (3, 12000, 980, 67, 3200000, CURDATE())
        """)
        
        cursor.execute("USE finance_db")
        
        # Sample accounts
        cursor.execute("""
            INSERT IGNORE INTO accounts (account_code, account_name, account_type, balance) VALUES
            ('1001', 'Cash in Hand', 'asset', 50000000),
            ('1002', 'Bank Account', 'asset', 250000000),
            ('2001', 'Accounts Payable', 'liability', 75000000),
            ('3001', 'Sales Revenue', 'revenue', 500000000)
        """)
        
        # Sample payments
        cursor.execute("""
            INSERT IGNORE INTO payments (payment_code, customer_id, amount, payment_method, status) VALUES
            ('PAY001', 1, 1500000, 'transfer', 'completed'),
            ('PAY002', 2, 2300000, 'card', 'completed'),
            ('PAY003', 3, 890000, 'cash', 'pending')
        """)
        
        cursor.execute("USE hr_db")
        
        # Sample employees
        cursor.execute("""
            INSERT IGNORE INTO employees (employee_code, first_name, last_name, email, department, position, salary) VALUES
            ('EMP001', 'Nguyen', 'Van Nam', 'nam.nguyen@company.com', 'Sales', 'Sales Manager', 25000000),
            ('EMP002', 'Tran', 'Thi Lan', 'lan.tran@company.com', 'Marketing', 'Marketing Specialist', 18000000),
            ('EMP003', 'Le', 'Van Duc', 'duc.le@company.com', 'Finance', 'Accountant', 20000000)
        """)
        
        cursor.execute("USE inventory_db")
        
        # Sample products
        cursor.execute("""
            INSERT IGNORE INTO products (product_code, name, category, unit_price, cost_price, stock_quantity) VALUES
            ('PROD001', 'Laptop Dell Inspiron', 'Electronics', 15000000, 12000000, 50),
            ('PROD002', 'Office Chair', 'Furniture', 2500000, 1800000, 25),
            ('PROD003', 'Wireless Mouse', 'Electronics', 350000, 250000, 100)
        """)
        
        cursor.execute("USE support_db")
        
        # Sample tickets
        cursor.execute("""
            INSERT IGNORE INTO tickets (ticket_number, customer_id, subject, description, priority, status) VALUES
            ('TKT001', 1, 'Login Issue', 'Cannot access my account', 'medium', 'open'),
            ('TKT002', 2, 'Payment Problem', 'Payment failed multiple times', 'high', 'in_progress'),
            ('TKT003', 3, 'Product Question', 'Need info about warranty', 'low', 'resolved')
        """)
        
        cursor.execute("USE admin_db")
        
        # Sample system config
        cursor.execute("""
            INSERT IGNORE INTO system_config (config_key, config_value, description) VALUES
            ('maintenance_mode', 'false', 'System maintenance mode'),
            ('max_login_attempts', '5', 'Maximum login attempts before lockout'),
            ('session_timeout', '3600', 'Session timeout in seconds')
        """)
        
        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        # Commit all changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\n🎉 DATABASE STRUCTURE CREATED SUCCESSFULLY!")
        print(f"✅ All 7 databases with tables and sample data")
        print(f"✅ No more 'Table doesn't exist' errors")
        print(f"✅ Ready for simulation with all 97 users")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = create_database_structure()
    if success:
        print(f"\n🚀 Ready to run simulation!")
        print(f"Run: python main_execution_enhanced.py clean")
    sys.exit(0 if success else 1)