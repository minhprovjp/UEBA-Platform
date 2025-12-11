#!/usr/bin/env python3
"""
Fix remaining database issues after initial setup
"""

import mysql.connector
import sys

def fix_remaining_issues():
    """Fix foreign key constraints and sample data issues"""
    
    print("🔧 FIXING REMAINING DATABASE ISSUES")
    print("=" * 50)
    
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
        
        # Fix marketing_db tables
        cursor.execute("USE marketing_db")
        
        # Drop and recreate campaign_performance without foreign key
        cursor.execute("DROP TABLE IF EXISTS campaign_performance")
        cursor.execute("""
            CREATE TABLE campaign_performance (
                id INT PRIMARY KEY AUTO_INCREMENT,
                campaign_id INT,
                impressions INT DEFAULT 0,
                clicks INT DEFAULT 0,
                conversions INT DEFAULT 0,
                cost DECIMAL(10,2) DEFAULT 0,
                report_date DATE
            )
        """)
        print("✅ Fixed campaign_performance table")
        
        # Drop and recreate email_campaigns without foreign key
        cursor.execute("DROP TABLE IF EXISTS email_campaigns")
        cursor.execute("""
            CREATE TABLE email_campaigns (
                id INT PRIMARY KEY AUTO_INCREMENT,
                campaign_id INT,
                subject VARCHAR(200),
                sent_count INT DEFAULT 0,
                open_count INT DEFAULT 0,
                click_count INT DEFAULT 0,
                sent_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Fixed email_campaigns table")
        
        # Fix finance_db tables
        cursor.execute("USE finance_db")
        
        # Drop and recreate transactions without foreign key
        cursor.execute("DROP TABLE IF EXISTS transactions")
        cursor.execute("""
            CREATE TABLE transactions (
                id INT PRIMARY KEY AUTO_INCREMENT,
                transaction_code VARCHAR(50) UNIQUE,
                account_id INT,
                amount DECIMAL(15,2),
                transaction_type ENUM('debit', 'credit') NOT NULL,
                description TEXT,
                transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Fixed transactions table")
        
        # Fix hr_db tables
        cursor.execute("USE hr_db")
        
        # Drop and recreate payroll without foreign key
        cursor.execute("DROP TABLE IF EXISTS payroll")
        cursor.execute("""
            CREATE TABLE payroll (
                id INT PRIMARY KEY AUTO_INCREMENT,
                employee_id INT,
                base_salary DECIMAL(12,2),
                overtime_hours DECIMAL(4,2) DEFAULT 0,
                overtime_pay DECIMAL(10,2) DEFAULT 0,
                deductions DECIMAL(10,2) DEFAULT 0,
                net_pay DECIMAL(12,2),
                pay_period_start DATE,
                pay_period_end DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Fixed payroll table")
        
        # Drop and recreate leave_requests without foreign key
        cursor.execute("DROP TABLE IF EXISTS leave_requests")
        cursor.execute("""
            CREATE TABLE leave_requests (
                id INT PRIMARY KEY AUTO_INCREMENT,
                employee_id INT,
                leave_type ENUM('annual', 'sick', 'personal', 'maternity', 'emergency') NOT NULL,
                start_date DATE,
                end_date DATE,
                days_requested INT,
                status ENUM('pending', 'approved', 'rejected') DEFAULT 'pending',
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Fixed leave_requests table")
        
        # Add sample data with correct column names
        cursor.execute("USE sales_db")
        
        # Clear existing data first
        cursor.execute("DELETE FROM customers")
        cursor.execute("DELETE FROM orders")
        
        # Insert sample customers with correct columns
        cursor.execute("""
            INSERT INTO customers (customer_code, name, email, phone) VALUES
            ('CUST001', 'Nguyen Van A', 'nguyenvana@email.com', '0901234567'),
            ('CUST002', 'Tran Thi B', 'tranthib@email.com', '0901234568'),
            ('CUST003', 'Le Van C', 'levanc@email.com', '0901234569')
        """)
        print("✅ Added sample customers")
        
        # Insert sample orders
        cursor.execute("""
            INSERT INTO orders (order_code, customer_id, total_amount, status) VALUES
            ('ORD001', 1, 1500000, 'delivered'),
            ('ORD002', 2, 2300000, 'processing'),
            ('ORD003', 3, 890000, 'shipped')
        """)
        print("✅ Added sample orders")
        
        # Add sample data to other key tables
        cursor.execute("USE marketing_db")
        cursor.execute("DELETE FROM campaigns")
        cursor.execute("DELETE FROM campaign_performance")
        
        cursor.execute("""
            INSERT INTO campaigns (campaign_name, campaign_type, budget, status) VALUES
            ('Summer Sale 2025', 'email', 50000000, 'active'),
            ('New Product Launch', 'social', 30000000, 'active'),
            ('Holiday Promotion', 'ppc', 75000000, 'draft')
        """)
        
        cursor.execute("""
            INSERT INTO campaign_performance (campaign_id, impressions, clicks, conversions, cost, report_date) VALUES
            (1, 15000, 1200, 85, 2500000, CURDATE()),
            (2, 8500, 650, 42, 1800000, CURDATE()),
            (3, 12000, 980, 67, 3200000, CURDATE())
        """)
        print("✅ Added sample marketing data")
        
        cursor.execute("USE finance_db")
        cursor.execute("DELETE FROM payments")
        
        cursor.execute("""
            INSERT INTO payments (payment_code, customer_id, amount, payment_method, status) VALUES
            ('PAY001', 1, 1500000, 'transfer', 'completed'),
            ('PAY002', 2, 2300000, 'card', 'completed'),
            ('PAY003', 3, 890000, 'cash', 'pending')
        """)
        print("✅ Added sample payment data")
        
        cursor.execute("USE admin_db")
        cursor.execute("DELETE FROM system_config")
        
        cursor.execute("""
            INSERT INTO system_config (config_key, config_value, description) VALUES
            ('maintenance_mode', 'false', 'System maintenance mode'),
            ('max_login_attempts', '5', 'Maximum login attempts before lockout'),
            ('session_timeout', '3600', 'Session timeout in seconds')
        """)
        print("✅ Added sample admin config")
        
        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        # Commit all changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\n🎉 ALL ISSUES FIXED SUCCESSFULLY!")
        print(f"✅ Foreign key constraints resolved")
        print(f"✅ Sample data added correctly")
        print(f"✅ All tables ready for simulation")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = fix_remaining_issues()
    if success:
        print(f"\n🚀 Database is now fully ready!")
        print(f"Run: python main_execution_enhanced.py clean")
    sys.exit(0 if success else 1)