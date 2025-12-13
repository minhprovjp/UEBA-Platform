#!/usr/bin/env python3
"""
Populate Sample Data for Vietnamese Medium-Sized Sales Company
Creates realistic sample data to ensure UPDATE queries have rows to match
"""

import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta

# Configuration
DB_CONFIG = {"host": "localhost", "port": 3306, "user": "root", "password": "root"}

# Vietnamese Faker
fake_vn = Faker('vi_VN')

def get_conn(db=None):
    cfg = DB_CONFIG.copy()
    if db: 
        cfg["database"] = db
    return mysql.connector.connect(**cfg, autocommit=True)

def populate_sales_db():
    """Populate sales database with sample data"""
    print("💰 Populating SALES_DB with sample data...")
    conn = get_conn("sales_db")
    cursor = conn.cursor()
    
    # Product Categories
    categories = [
        ("Điện tử", "Thiết bị điện tử và công nghệ"),
        ("Nội thất", "Đồ nội thất gia đình và văn phòng"),
        ("Thời trang", "Quần áo và phụ kiện thời trang"),
        ("Đồ gia dụng", "Dụng cụ và thiết bị gia đình"),
        ("Thực phẩm", "Thực phẩm và đồ uống")
    ]
    
    for cat_name, description in categories:
        cursor.execute("""
            INSERT IGNORE INTO product_categories (category_name, description) 
            VALUES (%s, %s)
        """, (cat_name, description))
    
    # Products
    products_data = []
    for i in range(1, 101):  # 100 products
        category_id = random.randint(1, 5)
        product_name = f"Sản phẩm {fake_vn.word().title()} {i}"
        sku = f"SP{i:04d}"
        price = random.randint(50000, 5000000)  # 50k to 5M VND
        cost_price = price * 0.7  # 70% of selling price
        supplier = f"Nhà cung cấp {fake_vn.company()}"
        
        products_data.append((product_name, category_id, sku, price, cost_price, supplier))
    
    cursor.executemany("""
        INSERT IGNORE INTO products (product_name, category_id, sku, price, cost_price, supplier_name)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, products_data)
    
    # Customers
    customers_data = []
    vietnamese_cities = ["Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ", "Nha Trang", "Vũng Tàu"]
    
    for i in range(1, 101):  # 100 customers
        customer_code = f"KH{i:04d}"
        company_name = fake_vn.company()
        contact_person = fake_vn.name()
        email = fake_vn.email()
        phone = fake_vn.phone_number()[:15]
        address = fake_vn.address()
        city = random.choice(vietnamese_cities)
        province = city  # Simplified
        customer_type = random.choice(['individual', 'business', 'enterprise'])
        credit_limit = random.randint(1000000, 50000000)  # 1M to 50M VND
        
        customers_data.append((customer_code, company_name, contact_person, email, phone, 
                             address, city, province, customer_type, credit_limit))
    
    cursor.executemany("""
        INSERT IGNORE INTO customers (customer_code, company_name, contact_person, email, phone, 
                                    address, city, province, customer_type, credit_limit)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, customers_data)
    
    # Orders
    orders_data = []
    for i in range(1, 201):  # 200 orders
        order_number = f"DH{i:06d}"
        customer_id = random.randint(1, 100)
        order_date = fake_vn.date_between(start_date='-90d', end_date='today')
        delivery_date = order_date + timedelta(days=random.randint(1, 14))
        subtotal = random.randint(500000, 10000000)  # 500k to 10M VND
        tax_amount = subtotal * 0.1  # 10% VAT
        discount_amount = subtotal * random.uniform(0, 0.1)  # 0-10% discount
        total_amount = subtotal + tax_amount - discount_amount
        status = random.choice(['confirmed', 'processing', 'shipped', 'delivered'])
        payment_status = random.choice(['pending', 'partial', 'paid'])
        sales_person = f"sales_{random.randint(1, 10)}"
        
        orders_data.append((order_number, customer_id, order_date, delivery_date, 
                          subtotal, tax_amount, discount_amount, total_amount, 
                          status, payment_status, sales_person))
    
    cursor.executemany("""
        INSERT IGNORE INTO orders (order_number, customer_id, order_date, delivery_date,
                                 subtotal, tax_amount, discount_amount, total_amount,
                                 status, payment_status, sales_person)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, orders_data)
    
    conn.close()
    print(f"  ✅ Added 100 customers, 100 products, 200 orders")

def populate_hr_db():
    """Populate HR database with sample data"""
    print("👥 Populating HR_DB with sample data...")
    conn = get_conn("hr_db")
    cursor = conn.cursor()
    
    # Departments
    departments = [
        ("Phòng Kinh Doanh", "SALES", 50000000),
        ("Phòng Marketing", "MKT", 30000000),
        ("Phòng Nhân Sự", "HR", 20000000),
        ("Phòng Tài Chính", "FIN", 25000000),
        ("Phòng IT", "IT", 40000000),
        ("Ban Giám Đốc", "MGT", 100000000)
    ]
    
    for dept_name, dept_code, budget in departments:
        cursor.execute("""
            INSERT IGNORE INTO departments (dept_name, dept_code, budget)
            VALUES (%s, %s, %s)
        """, (dept_name, dept_code, budget))
    
    # Employees
    employees_data = []
    for i in range(1, 101):  # 100 employees
        name = fake_vn.name()
        email = fake_vn.email()
        position = random.choice(['Nhân viên', 'Trưởng nhóm', 'Phó phòng', 'Trưởng phòng'])
        dept_id = random.randint(1, 6)
        salary = random.randint(8000000, 50000000)  # 8M to 50M VND
        hire_date = fake_vn.date_between(start_date='-5y', end_date='-1m')
        phone = fake_vn.phone_number()[:15]
        address = fake_vn.address()
        
        employees_data.append((i, name, email, position, dept_id, salary, hire_date, phone, address))
    
    cursor.executemany("""
        INSERT IGNORE INTO employees (id, name, email, position, dept_id, salary, hire_date, phone, address)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, employees_data)
    
    # Attendance records
    attendance_data = []
    for employee_id in range(1, 101):
        # Generate attendance for last 30 days
        for days_ago in range(30):
            date = datetime.now().date() - timedelta(days=days_ago)
            # Skip weekends
            if date.weekday() < 5:  # Monday = 0, Sunday = 6
                status = random.choices(
                    ['present', 'absent', 'late', 'sick_leave'], 
                    weights=[85, 5, 8, 2]
                )[0]
                check_in = "08:00:00" if status == 'present' else None
                check_out = "17:00:00" if status == 'present' else None
                
                attendance_data.append((employee_id, date, check_in, check_out, status))
    
    cursor.executemany("""
        INSERT IGNORE INTO attendance (employee_id, date, check_in_time, check_out_time, status)
        VALUES (%s, %s, %s, %s, %s)
    """, attendance_data)
    
    # Salary records
    salary_data = []
    for employee_id in range(1, 101):
        # Generate salary for last 3 months
        for month_ago in range(3):
            payment_date = datetime.now().date().replace(day=25) - timedelta(days=30*month_ago)
            # Get employee salary
            cursor.execute("SELECT salary FROM employees WHERE id = %s", (employee_id,))
            base_salary = cursor.fetchone()[0]
            
            bonus = random.randint(0, int(float(base_salary) * 0.2))  # 0-20% bonus
            deductions = random.randint(0, int(float(base_salary) * 0.05))  # 0-5% deductions
            
            salary_data.append((employee_id, base_salary, bonus, deductions, 
                              payment_date, payment_date.month, payment_date.year, 'paid'))
    
    cursor.executemany("""
        INSERT IGNORE INTO salaries (employee_id, amount, bonus, deductions, 
                                    payment_date, salary_month, salary_year, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, salary_data)
    
    conn.close()
    print(f"  ✅ Added 100 employees, 6 departments, attendance & salary records")

def populate_other_databases():
    """Populate other databases with minimal sample data"""
    
    # Marketing DB
    print("📢 Populating MARKETING_DB...")
    conn = get_conn("marketing_db")
    cursor = conn.cursor()
    
    # Campaigns
    campaigns_data = []
    for i in range(1, 51):  # 50 campaigns
        campaign_name = f"Chiến dịch {fake_vn.word().title()} {i}"
        campaign_type = random.choice(['email', 'social_media', 'google_ads', 'facebook_ads'])
        start_date = fake_vn.date_between(start_date='-6m', end_date='today')
        end_date = start_date + timedelta(days=random.randint(7, 90))
        budget = random.randint(5000000, 100000000)  # 5M to 100M VND
        status = random.choice(['active', 'completed', 'paused'])
        
        campaigns_data.append((campaign_name, campaign_type, start_date, end_date, budget, status, 'marketing_team'))
    
    cursor.executemany("""
        INSERT IGNORE INTO campaigns (campaign_name, campaign_type, start_date, end_date, budget, status, created_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, campaigns_data)
    
    # Leads
    leads_data = []
    for i in range(1, 101):  # 100 leads
        lead_source = random.choice(['website', 'referral', 'cold_call', 'social_media'])
        company_name = fake_vn.company()
        contact_name = fake_vn.name()
        email = fake_vn.email()
        phone = fake_vn.phone_number()[:15]
        status = random.choice(['new', 'contacted', 'qualified', 'proposal', 'won', 'lost'])
        estimated_value = random.randint(1000000, 50000000)
        
        leads_data.append((lead_source, company_name, contact_name, email, phone, status, estimated_value))
    
    cursor.executemany("""
        INSERT IGNORE INTO leads (lead_source, company_name, contact_name, email, phone, status, estimated_value)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, leads_data)
    
    conn.close()
    print(f"  ✅ Added 50 campaigns, 100 leads")
    
    # Support DB
    print("🎧 Populating SUPPORT_DB...")
    conn = get_conn("support_db")
    cursor = conn.cursor()
    
    # Support Tickets
    tickets_data = []
    for i in range(1, 101):  # 100 tickets
        ticket_number = f"TK{i:06d}"
        customer_id = random.randint(1, 100)
        subject = f"Vấn đề {fake_vn.word()} - {i}"
        description = fake_vn.text(max_nb_chars=200)
        priority = random.choice(['low', 'medium', 'high', 'urgent'])
        status = random.choice(['open', 'in_progress', 'resolved', 'closed'])
        category = random.choice(['technical', 'billing', 'product_info', 'complaint'])
        assigned_to = f"support_{random.randint(1, 5)}"
        
        tickets_data.append((ticket_number, customer_id, subject, description, priority, status, category, assigned_to))
    
    cursor.executemany("""
        INSERT IGNORE INTO support_tickets (ticket_number, customer_id, subject, description, priority, status, category, assigned_to)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, tickets_data)
    
    conn.close()
    print(f"  ✅ Added 100 support tickets")

def main():
    """Main function to populate all databases"""
    print("🏢 POPULATING VIETNAMESE COMPANY DATABASES WITH SAMPLE DATA")
    print("=" * 70)
    
    try:
        populate_sales_db()
        populate_hr_db()
        populate_other_databases()
        
        print(f"\n✅ SAMPLE DATA POPULATION COMPLETED")
        print(f"📊 All databases now have realistic sample data")
        print(f"🔄 UPDATE queries will now find matching rows")
        
    except Exception as e:
        print(f"❌ Error populating data: {e}")

if __name__ == "__main__":
    main()