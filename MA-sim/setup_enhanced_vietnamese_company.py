#!/usr/bin/env python3
"""
Enhanced Database Structure for Vietnamese Medium-Sized Sales Company
Creates realistic database architecture for 97-employee company
"""

import mysql.connector
from faker import Faker
import random
import json
import os
from datetime import datetime, timedelta

# Configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root", 
    "password": "root"
}

# Vietnamese Faker
fake_vn = Faker('vi_VN')

def get_conn(db=None):
    cfg = DB_CONFIG.copy()
    if db: 
        cfg["database"] = db
    return mysql.connector.connect(**cfg, autocommit=True)

def setup_enhanced_database_structure():
    """
    Create realistic database structure for Vietnamese medium-sized sales company
    """
    print("🏢 CREATING ENHANCED VIETNAMESE COMPANY DATABASE STRUCTURE")
    print("=" * 70)
    
    conn = get_conn()
    cursor = conn.cursor()
    
    # Define databases for medium-sized Vietnamese sales company
    databases = {
        'sales_db': 'Hệ thống bán hàng và khách hàng',
        'hr_db': 'Hệ thống nhân sự và lương',
        'inventory_db': 'Hệ thống kho và logistics', 
        'finance_db': 'Hệ thống tài chính và kế toán',
        'marketing_db': 'Hệ thống marketing và CRM',
        'support_db': 'Hệ thống chăm sóc khách hàng',
        'admin_db': 'Hệ thống quản trị và báo cáo'
    }
    
    print(f"📊 Creating {len(databases)} specialized databases:")
    for db_name, description in databases.items():
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print(f"   ✅ {db_name}: {description}")
    
    # 1. SALES DATABASE - Core sales operations
    print(f"\n💰 Setting up SALES_DB (Core Sales Operations)...")
    conn.close()
    conn = get_conn("sales_db")
    cursor = conn.cursor()
    
    # Drop existing tables (handle foreign key constraints)
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    tables_to_drop = ["reviews", "order_payments", "order_items", "orders", "customer_contacts", "customers", "inventory", "products", "product_categories", "marketing_campaigns"]
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    
    cursor.execute("""
        CREATE TABLE product_categories (
            category_id INT AUTO_INCREMENT PRIMARY KEY,
            category_name VARCHAR(100) NOT NULL,
            parent_category_id INT,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_parent (parent_category_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE products (
            product_id INT AUTO_INCREMENT PRIMARY KEY,
            product_name VARCHAR(200) NOT NULL,
            category_id INT,
            sku VARCHAR(50) UNIQUE,
            price DECIMAL(15,2),
            cost_price DECIMAL(15,2),
            supplier_name VARCHAR(100),
            description TEXT,
            status ENUM('active', 'inactive', 'discontinued') DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES product_categories(category_id),
            INDEX idx_sku (sku),
            INDEX idx_category (category_id),
            INDEX idx_status (status)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE customers (
            customer_id INT AUTO_INCREMENT PRIMARY KEY,
            customer_code VARCHAR(20) UNIQUE,
            company_name VARCHAR(200),
            contact_person VARCHAR(100),
            email VARCHAR(100),
            phone VARCHAR(20),
            address TEXT,
            city VARCHAR(50),
            province VARCHAR(50),
            customer_type ENUM('individual', 'business', 'enterprise') DEFAULT 'individual',
            credit_limit DECIMAL(15,2) DEFAULT 0,
            payment_terms INT DEFAULT 30,
            status ENUM('active', 'inactive', 'blacklisted') DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_code (customer_code),
            INDEX idx_type (customer_type),
            INDEX idx_city (city)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE customer_contacts (
            contact_id INT AUTO_INCREMENT PRIMARY KEY,
            customer_id INT,
            contact_name VARCHAR(100),
            position VARCHAR(100),
            email VARCHAR(100),
            phone VARCHAR(20),
            is_primary BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            INDEX idx_customer (customer_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE orders (
            order_id INT AUTO_INCREMENT PRIMARY KEY,
            order_number VARCHAR(30) UNIQUE,
            customer_id INT,
            order_date DATE,
            delivery_date DATE,
            subtotal DECIMAL(15,2),
            tax_amount DECIMAL(15,2),
            discount_amount DECIMAL(15,2) DEFAULT 0,
            total_amount DECIMAL(15,2),
            status ENUM('draft', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'draft',
            payment_status ENUM('pending', 'partial', 'paid', 'overdue') DEFAULT 'pending',
            sales_person VARCHAR(50),
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            INDEX idx_order_number (order_number),
            INDEX idx_customer (customer_id),
            INDEX idx_date (order_date),
            INDEX idx_status (status),
            INDEX idx_sales_person (sales_person)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE order_items (
            item_id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT,
            product_id INT,
            quantity INT,
            unit_price DECIMAL(15,2),
            discount_percent DECIMAL(5,2) DEFAULT 0,
            line_total DECIMAL(15,2),
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            INDEX idx_order (order_id),
            INDEX idx_product (product_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE order_payments (
            payment_id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT,
            payment_date DATE,
            amount DECIMAL(15,2),
            payment_method ENUM('cash', 'bank_transfer', 'credit_card', 'check', 'momo', 'zalopay') DEFAULT 'bank_transfer',
            reference_number VARCHAR(50),
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            INDEX idx_order (order_id),
            INDEX idx_date (payment_date)
        )
    """)
    
    # 2. INVENTORY DATABASE - Warehouse and logistics
    print(f"\n📦 Setting up INVENTORY_DB (Warehouse & Logistics)...")
    conn.close()
    conn = get_conn("inventory_db")
    cursor = conn.cursor()
    
    tables_to_drop = ["stock_movements", "inventory_adjustments", "warehouse_locations", "inventory_levels"]
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    cursor.execute("""
        CREATE TABLE warehouse_locations (
            location_id INT AUTO_INCREMENT PRIMARY KEY,
            warehouse_code VARCHAR(20) UNIQUE,
            warehouse_name VARCHAR(100),
            address TEXT,
            city VARCHAR(50),
            manager_name VARCHAR(100),
            capacity_sqm INT,
            status ENUM('active', 'inactive') DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE inventory_levels (
            inventory_id INT AUTO_INCREMENT PRIMARY KEY,
            product_id INT,
            location_id INT,
            current_stock INT DEFAULT 0,
            reserved_stock INT DEFAULT 0,
            available_stock INT GENERATED ALWAYS AS (current_stock - reserved_stock) STORED,
            min_stock_level INT DEFAULT 0,
            max_stock_level INT DEFAULT 1000,
            last_count_date DATE,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (location_id) REFERENCES warehouse_locations(location_id),
            UNIQUE KEY unique_product_location (product_id, location_id),
            INDEX idx_product (product_id),
            INDEX idx_location (location_id),
            INDEX idx_stock_level (current_stock)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE stock_movements (
            movement_id INT AUTO_INCREMENT PRIMARY KEY,
            product_id INT,
            location_id INT,
            movement_type ENUM('in', 'out', 'transfer', 'adjustment') NOT NULL,
            quantity INT NOT NULL,
            reference_type ENUM('purchase', 'sale', 'transfer', 'adjustment', 'return') NOT NULL,
            reference_id INT,
            unit_cost DECIMAL(15,2),
            movement_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            notes TEXT,
            created_by VARCHAR(50),
            FOREIGN KEY (location_id) REFERENCES warehouse_locations(location_id),
            INDEX idx_product (product_id),
            INDEX idx_location (location_id),
            INDEX idx_date (movement_date),
            INDEX idx_type (movement_type)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE inventory_adjustments (
            adjustment_id INT AUTO_INCREMENT PRIMARY KEY,
            product_id INT,
            location_id INT,
            old_quantity INT,
            new_quantity INT,
            adjustment_quantity INT GENERATED ALWAYS AS (new_quantity - old_quantity) STORED,
            reason ENUM('damaged', 'expired', 'theft', 'count_error', 'other') NOT NULL,
            adjustment_date DATE,
            approved_by VARCHAR(50),
            notes TEXT,
            FOREIGN KEY (location_id) REFERENCES warehouse_locations(location_id),
            INDEX idx_product (product_id),
            INDEX idx_date (adjustment_date)
        )
    """)
    
    # 3. FINANCE DATABASE - Accounting and financial management
    print(f"\n💳 Setting up FINANCE_DB (Accounting & Financial Management)...")
    conn.close()
    conn = get_conn("finance_db")
    cursor = conn.cursor()
    
    tables_to_drop = ["journal_entries", "accounts", "invoices", "expense_reports", "budget_plans"]
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    cursor.execute("""
        CREATE TABLE accounts (
            account_id INT AUTO_INCREMENT PRIMARY KEY,
            account_code VARCHAR(20) UNIQUE,
            account_name VARCHAR(100),
            account_type ENUM('asset', 'liability', 'equity', 'revenue', 'expense') NOT NULL,
            parent_account_id INT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_code (account_code),
            INDEX idx_type (account_type)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE invoices (
            invoice_id INT AUTO_INCREMENT PRIMARY KEY,
            invoice_number VARCHAR(30) UNIQUE,
            customer_id INT,
            order_id INT,
            invoice_date DATE,
            due_date DATE,
            subtotal DECIMAL(15,2),
            tax_amount DECIMAL(15,2),
            total_amount DECIMAL(15,2),
            paid_amount DECIMAL(15,2) DEFAULT 0,
            balance DECIMAL(15,2) GENERATED ALWAYS AS (total_amount - paid_amount) STORED,
            status ENUM('draft', 'sent', 'paid', 'overdue', 'cancelled') DEFAULT 'draft',
            payment_terms INT DEFAULT 30,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_number (invoice_number),
            INDEX idx_customer (customer_id),
            INDEX idx_date (invoice_date),
            INDEX idx_status (status)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE expense_reports (
            expense_id INT AUTO_INCREMENT PRIMARY KEY,
            employee_id VARCHAR(50),
            expense_date DATE,
            category ENUM('travel', 'meals', 'office_supplies', 'marketing', 'training', 'other') NOT NULL,
            amount DECIMAL(15,2),
            description TEXT,
            receipt_attached BOOLEAN DEFAULT FALSE,
            status ENUM('draft', 'submitted', 'approved', 'rejected', 'paid') DEFAULT 'draft',
            approved_by VARCHAR(50),
            approved_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_employee (employee_id),
            INDEX idx_date (expense_date),
            INDEX idx_status (status)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE budget_plans (
            budget_id INT AUTO_INCREMENT PRIMARY KEY,
            department VARCHAR(50),
            budget_year YEAR,
            budget_month TINYINT,
            category VARCHAR(50),
            planned_amount DECIMAL(15,2),
            actual_amount DECIMAL(15,2) DEFAULT 0,
            variance DECIMAL(15,2) GENERATED ALWAYS AS (actual_amount - planned_amount) STORED,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_dept_year (department, budget_year),
            INDEX idx_category (category)
        )
    """)
    
    # 4. MARKETING DATABASE - CRM and marketing campaigns
    print(f"\n📢 Setting up MARKETING_DB (CRM & Marketing Campaigns)...")
    conn.close()
    conn = get_conn("marketing_db")
    cursor = conn.cursor()
    
    tables_to_drop = ["campaign_results", "lead_activities", "leads", "campaigns"]
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    cursor.execute("""
        CREATE TABLE campaigns (
            campaign_id INT AUTO_INCREMENT PRIMARY KEY,
            campaign_name VARCHAR(200),
            campaign_type ENUM('email', 'social_media', 'google_ads', 'facebook_ads', 'event', 'telemarketing') NOT NULL,
            start_date DATE,
            end_date DATE,
            budget DECIMAL(15,2),
            target_audience TEXT,
            status ENUM('planning', 'active', 'paused', 'completed', 'cancelled') DEFAULT 'planning',
            created_by VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_type (campaign_type),
            INDEX idx_status (status),
            INDEX idx_dates (start_date, end_date)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE leads (
            lead_id INT AUTO_INCREMENT PRIMARY KEY,
            lead_source ENUM('website', 'referral', 'cold_call', 'social_media', 'event', 'advertisement') NOT NULL,
            company_name VARCHAR(200),
            contact_name VARCHAR(100),
            email VARCHAR(100),
            phone VARCHAR(20),
            position VARCHAR(100),
            industry VARCHAR(100),
            lead_score INT DEFAULT 0,
            status ENUM('new', 'contacted', 'qualified', 'proposal', 'negotiation', 'won', 'lost') DEFAULT 'new',
            assigned_to VARCHAR(50),
            estimated_value DECIMAL(15,2),
            probability_percent INT DEFAULT 0,
            expected_close_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_status (status),
            INDEX idx_assigned (assigned_to),
            INDEX idx_source (lead_source),
            INDEX idx_score (lead_score)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE lead_activities (
            activity_id INT AUTO_INCREMENT PRIMARY KEY,
            lead_id INT,
            activity_type ENUM('call', 'email', 'meeting', 'demo', 'proposal', 'follow_up') NOT NULL,
            activity_date DATETIME,
            duration_minutes INT,
            notes TEXT,
            outcome ENUM('positive', 'neutral', 'negative') DEFAULT 'neutral',
            next_action TEXT,
            created_by VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (lead_id) REFERENCES leads(lead_id),
            INDEX idx_lead (lead_id),
            INDEX idx_date (activity_date),
            INDEX idx_type (activity_type)
        )
    """)
    
    # 5. SUPPORT DATABASE - Customer service and support
    print(f"\n🎧 Setting up SUPPORT_DB (Customer Service & Support)...")
    conn.close()
    conn = get_conn("support_db")
    cursor = conn.cursor()
    
    tables_to_drop = ["ticket_responses", "support_tickets", "knowledge_base"]
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    cursor.execute("""
        CREATE TABLE support_tickets (
            ticket_id INT AUTO_INCREMENT PRIMARY KEY,
            ticket_number VARCHAR(20) UNIQUE,
            customer_id INT,
            subject VARCHAR(200),
            description TEXT,
            priority ENUM('low', 'medium', 'high', 'urgent') DEFAULT 'medium',
            status ENUM('open', 'in_progress', 'waiting_customer', 'resolved', 'closed') DEFAULT 'open',
            category ENUM('technical', 'billing', 'product_info', 'complaint', 'feature_request') NOT NULL,
            assigned_to VARCHAR(50),
            created_by VARCHAR(50),
            resolution TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP NULL,
            INDEX idx_number (ticket_number),
            INDEX idx_customer (customer_id),
            INDEX idx_status (status),
            INDEX idx_priority (priority),
            INDEX idx_assigned (assigned_to)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE ticket_responses (
            response_id INT AUTO_INCREMENT PRIMARY KEY,
            ticket_id INT,
            response_text TEXT,
            is_internal BOOLEAN DEFAULT FALSE,
            created_by VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (ticket_id) REFERENCES support_tickets(ticket_id),
            INDEX idx_ticket (ticket_id),
            INDEX idx_date (created_at)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE knowledge_base (
            article_id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(200),
            content TEXT,
            category VARCHAR(100),
            tags VARCHAR(500),
            view_count INT DEFAULT 0,
            is_published BOOLEAN DEFAULT FALSE,
            created_by VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_category (category),
            INDEX idx_published (is_published)
        )
    """)
    
    # 6. ADMIN DATABASE - System administration and reporting
    print(f"\n⚙️ Setting up ADMIN_DB (System Administration & Reporting)...")
    conn.close()
    conn = get_conn("admin_db")
    cursor = conn.cursor()
    
    tables_to_drop = ["system_logs", "user_sessions", "report_schedules"]
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    cursor.execute("""
        CREATE TABLE system_logs (
            log_id INT AUTO_INCREMENT PRIMARY KEY,
            log_level ENUM('info', 'warning', 'error', 'critical') NOT NULL,
            module VARCHAR(50),
            message TEXT,
            user_id VARCHAR(50),
            ip_address VARCHAR(45),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_level (log_level),
            INDEX idx_module (module),
            INDEX idx_user (user_id),
            INDEX idx_date (created_at)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE user_sessions (
            session_id VARCHAR(128) PRIMARY KEY,
            user_id VARCHAR(50),
            ip_address VARCHAR(45),
            user_agent TEXT,
            login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            INDEX idx_user (user_id),
            INDEX idx_active (is_active),
            INDEX idx_login_time (login_time)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE report_schedules (
            schedule_id INT AUTO_INCREMENT PRIMARY KEY,
            report_name VARCHAR(100),
            report_type ENUM('sales', 'inventory', 'financial', 'hr', 'marketing') NOT NULL,
            schedule_frequency ENUM('daily', 'weekly', 'monthly', 'quarterly') NOT NULL,
            recipients TEXT,
            parameters JSON,
            last_run TIMESTAMP NULL,
            next_run TIMESTAMP NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_type (report_type),
            INDEX idx_next_run (next_run),
            INDEX idx_active (is_active)
        )
    """)
    
    conn.close()
    
    print(f"\n✅ ENHANCED DATABASE STRUCTURE COMPLETED")
    print(f"📊 Created {len(databases)} specialized databases with realistic table structure")
    print(f"🏢 Ready for Vietnamese medium-sized sales company simulation")
    
    return databases

def update_user_permissions_for_enhanced_structure():
    """
    Update user permissions to work with the enhanced database structure
    """
    print(f"\n🔐 UPDATING USER PERMISSIONS FOR ENHANCED STRUCTURE")
    print("=" * 60)
    
    # Enhanced role permissions for medium-sized company
    enhanced_permissions = {
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
            "description": "Tài khoản dễ bị tấn công"
        }
    }
    
    # Update the users config file
    config_file = "simulation/users_config.json"
    if os.path.exists(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # Update company info
        config["company_info"]["databases"] = 7
        config["company_info"]["database_list"] = [
            "sales_db", "hr_db", "inventory_db", "finance_db", 
            "marketing_db", "support_db", "admin_db"
        ]
        
        # Update roles with enhanced permissions
        config["roles"] = enhanced_permissions
        
        # Save updated config
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Updated user permissions configuration")
        print(f"📊 Enhanced permissions for {len(enhanced_permissions)} roles")
        print(f"🗄️ Covering 7 specialized databases")
    else:
        print(f"⚠️ Users config file not found: {config_file}")

if __name__ == "__main__":
    databases = setup_enhanced_database_structure()
    update_user_permissions_for_enhanced_structure()
    
    print(f"\n🎯 ENHANCED VIETNAMESE COMPANY DATABASE SUMMARY:")
    print(f"   🏢 7 specialized databases for medium-sized enterprise")
    print(f"   📊 Realistic table structure with proper relationships")
    print(f"   🔐 Enhanced role-based permissions")
    print(f"   🇻🇳 Ready for Vietnamese business simulation")
    print(f"   👥 Supports 97-employee company operations")