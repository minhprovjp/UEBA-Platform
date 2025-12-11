#!/usr/bin/env python3
"""
Create the specific missing tables identified in the analysis
"""

import mysql.connector
import sys

def create_missing_tables():
    """Create the specific missing tables causing errors"""
    
    print("🔧 CREATING MISSING TABLES")
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
        
        # Create missing tables based on the error analysis
        
        # support_db.ticket_categories (28 errors)
        cursor.execute("USE support_db")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ticket_categories (
                id INT PRIMARY KEY AUTO_INCREMENT,
                category_name VARCHAR(100) NOT NULL,
                description TEXT,
                priority_level ENUM('low', 'medium', 'high') DEFAULT 'medium',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert sample data
        cursor.execute("""
            INSERT IGNORE INTO ticket_categories (category_name, description, priority_level) VALUES
            ('Technical Support', 'Technical issues and troubleshooting', 'high'),
            ('Billing', 'Payment and billing related queries', 'medium'),
            ('General Inquiry', 'General questions and information', 'low'),
            ('Bug Report', 'Software bugs and issues', 'high'),
            ('Feature Request', 'New feature suggestions', 'low')
        """)
        print("✅ Created support_db.ticket_categories")
        
        # marketing_db.lead_sources (22 errors)
        cursor.execute("USE marketing_db")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lead_sources (
                id INT PRIMARY KEY AUTO_INCREMENT,
                source_name VARCHAR(100) NOT NULL,
                source_type ENUM('website', 'social', 'email', 'referral', 'advertising') NOT NULL,
                cost_per_lead DECIMAL(10,2) DEFAULT 0,
                conversion_rate DECIMAL(5,2) DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert sample data
        cursor.execute("""
            INSERT IGNORE INTO lead_sources (source_name, source_type, cost_per_lead, conversion_rate) VALUES
            ('Google Ads', 'advertising', 25000, 3.5),
            ('Facebook', 'social', 15000, 2.8),
            ('Website Contact Form', 'website', 0, 8.2),
            ('Email Newsletter', 'email', 5000, 4.1),
            ('Referral Program', 'referral', 10000, 12.5)
        """)
        print("✅ Created marketing_db.lead_sources")
        
        # inventory_db.inventory_adjustments (4 errors)
        cursor.execute("USE inventory_db")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_adjustments (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_id INT,
                adjustment_type ENUM('increase', 'decrease', 'correction') NOT NULL,
                quantity_change INT NOT NULL,
                reason VARCHAR(200),
                adjusted_by VARCHAR(100),
                adjustment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT
            )
        """)
        
        # Insert sample data
        cursor.execute("""
            INSERT IGNORE INTO inventory_adjustments (product_id, adjustment_type, quantity_change, reason, adjusted_by) VALUES
            (1, 'increase', 50, 'New stock arrival', 'warehouse_manager'),
            (2, 'decrease', -10, 'Damaged goods', 'quality_control'),
            (3, 'correction', 5, 'Count discrepancy', 'inventory_clerk')
        """)
        print("✅ Created inventory_db.inventory_adjustments")
        
        # finance_db.invoice_items (3 errors)
        cursor.execute("USE finance_db")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS invoice_items (
                id INT PRIMARY KEY AUTO_INCREMENT,
                invoice_id INT,
                product_name VARCHAR(100),
                quantity INT,
                unit_price DECIMAL(10,2),
                total_price DECIMAL(12,2),
                tax_rate DECIMAL(5,2) DEFAULT 10.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert sample data
        cursor.execute("""
            INSERT IGNORE INTO invoice_items (invoice_id, product_name, quantity, unit_price, total_price) VALUES
            (1, 'Laptop Dell Inspiron', 2, 15000000, 30000000),
            (2, 'Office Chair', 5, 2500000, 12500000),
            (3, 'Wireless Mouse', 10, 350000, 3500000)
        """)
        print("✅ Created finance_db.invoice_items")
        
        # hr_db.employee_benefits (2 errors)
        cursor.execute("USE hr_db")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS employee_benefits (
                id INT PRIMARY KEY AUTO_INCREMENT,
                employee_id INT,
                benefit_type ENUM('health_insurance', 'dental', 'retirement', 'vacation', 'bonus') NOT NULL,
                benefit_value DECIMAL(12,2),
                start_date DATE,
                end_date DATE,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert sample data
        cursor.execute("""
            INSERT IGNORE INTO employee_benefits (employee_id, benefit_type, benefit_value, start_date) VALUES
            (1, 'health_insurance', 5000000, '2025-01-01'),
            (2, 'retirement', 3000000, '2025-01-01'),
            (3, 'vacation', 0, '2025-01-01')
        """)
        print("✅ Created hr_db.employee_benefits")
        
        # Commit all changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\n🎉 ALL MISSING TABLES CREATED!")
        print(f"✅ Fixed support_db.ticket_categories (28 errors)")
        print(f"✅ Fixed marketing_db.lead_sources (22 errors)")
        print(f"✅ Fixed inventory_db.inventory_adjustments (4 errors)")
        print(f"✅ Fixed finance_db.invoice_items (3 errors)")
        print(f"✅ Fixed hr_db.employee_benefits (2 errors)")
        print(f"✅ Should now have 100% success rate!")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ MySQL Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = create_missing_tables()
    if success:
        print(f"\n🚀 Database is now complete!")
        print(f"Run the simulation again for 100% success rate!")
    sys.exit(0 if success else 1)