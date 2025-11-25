# simulation\step1_generate_queries.py
import json, requests, re, ast, sys

# --- CẤU HÌNH ---
OLLAMA_URL = "http://100.92.147.73:11434/api/generate"
MODEL_NAME = "qwen2.5-coder:latest" 

# SCHEMA MỚI (Khớp 100% với setup_final_ultimate.py)
SCHEMA = """
SALES_DB:
- customers (customer_id, name, email, phone, address, city, segment, created_at)
- products (id, name, category, price, sku, supplier, created_at)
- inventory (product_id, stock_quantity, warehouse_location, last_restock_date) -- JOIN products ON product_id
- orders (order_id, customer_id, order_date, total_amount, status, payment_method)
- order_items (item_id, order_id, product_id, quantity, unit_price)
- marketing_campaigns (campaign_id, name, type, status, budget, start_date, end_date)
- reviews (review_id, product_id, customer_id, rating, comment, review_date)

HR_DB:
- departments (dept_id, dept_name, location)
- employees (employee_id, name, email, position, dept_id, hire_date, salary)
- salaries (salary_id, employee_id, amount, bonus, payment_date)
- attendance (record_id, employee_id, date, check_in, check_out, status)
"""

def clean_and_parse_json(content):
    match = re.search(r'\[.*\]', content, re.DOTALL)
    if match:
        text = match.group(0)
        try: return json.loads(text)
        except: 
            try: return ast.literal_eval(text)
            except: pass
    return []

def ask_ai(prompt_type):
    print(f"   ... Đang hỏi AI về: {prompt_type}")
    prompt = f"""
    Schema:
    {SCHEMA}
    
    Rules:
    1. ALWAYS use fully qualified table names (e.g., sales_db.products, hr_db.employees).
    2. Correct Joins: sales_db.order_items JOIN sales_db.products ON order_items.product_id = products.id.
    3. Use 'id' for products table PK, not 'product_id'.
    
    Task: Generate 20 SQL queries for: {prompt_type}
    Format: Python list of strings. 
    Use placeholders: {{id}}, {{customer_id}}, {{product_id}}, {{dept_id}}, {{name}}, {{sku}}, {{date}}, {{status}}, {{city}}, {{number}}.
    """
    try:
        resp = requests.post(OLLAMA_URL, json={"model": MODEL_NAME, "prompt": prompt, "stream": False})
        content = resp.json().get('response', '')
        return clean_and_parse_json(content)
    except Exception as e:
        print(f"⚠️ Lỗi AI: {e}")
        return []

def main():
    library = {}
    print("🤖 STEP 1: ĐANG TẠO KHO QUERY MỚI...")
    
    library["SALES"] = ask_ai("Sales Dept (Orders, Inventory check, Reviews, Campaign analysis)")
    if not library["SALES"]: library["SALES"] = ["SELECT * FROM sales_db.products"] 

    library["HR"] = ask_ai("HR Dept (Employee details, Salary calc, Attendance check, Dept listing)")
    library["DEV"] = ask_ai("Developers (Debug Orders, Check Inventory Logs, Optimize Joins)")
    
    library["ATTACK"] = [
        "SELECT * FROM hr_db.salaries", 
        "SELECT * FROM hr_db.employees WHERE employee_id = {id} UNION SELECT 1, user(), 3, 4, 5, 6, 7",
        "SELECT * FROM sales_db.customers INTO OUTFILE '/tmp/hack.csv'",
        "DROP TABLE sales_db.orders",
        "UPDATE hr_db.salaries SET amount = amount * 100 WHERE employee_id = {id}",
        "SELECT * FROM mysql.user",
        "SELECT * FROM information_schema.tables"
    ] + ask_ai("SQL Injection, Data Dumping, Privilege Escalation")

    with open("simulation/query_library.json", "w", encoding="utf-8") as f:
        json.dump(library, f, indent=2)
    print("✅ Đã tạo xong: simulation/query_library.json")

if __name__ == "__main__":
    main()