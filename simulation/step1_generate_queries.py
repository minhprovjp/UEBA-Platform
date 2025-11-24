import json, requests, re, ast, sys

# --- CẤU HÌNH ---
OLLAMA_URL = "http://100.92.147.73:11434/api/generate" # Đã fix lỗi 405
MODEL_NAME = "qwen2.5-coder:latest" 

SCHEMA = """
TABLE sales_db.orders (order_id, customer_id, amount, status, order_date)
TABLE sales_db.products (id, name, price, stock, category, sku)
TABLE sales_db.customers (customer_id, name, email, city)
TABLE sales_db.marketing_campaigns (campaign_id, name, budget, start_date, end_date)
TABLE hr_db.employees (employee_id, name, position, department, joined_date)
TABLE hr_db.salaries (id, employee_id, amount, bonus, last_changed)
"""

# Hàm lọc JSON mạnh mẽ (Đã fix lỗi Extra data)
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
    Schema: {SCHEMA}
    Task: Generate 15 DIVERSE SQL queries for: {prompt_type}
    Format: Python list of strings. 
    Use placeholders: {{id}}, {{name}}, {{date}}, {{sku}}, {{city}}, {{number}}.
    Example: ["SELECT * FROM sales_db.products WHERE id={{id}}"]
    NO explanations.
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
    print("🤖 BẮT ĐẦU TẠO KHO QUERY (STEP 1)...")
    
    library["SALES"] = ask_ai("Sales Dept (Orders, Customers, Products)")
    if not library["SALES"]: library["SALES"] = ["SELECT * FROM sales_db.products"] # Fallback

    library["HR"] = ask_ai("HR Dept (Employees, Salaries, Hiring)")
    library["DEV"] = ask_ai("Developers (Debug, Check Logs, Complex Joins)")
    
    # Query tấn công (Hardcode 1 phần để đảm bảo chất lượng)
    library["ATTACK"] = [
        "SELECT * FROM hr_db.salaries", 
        "SELECT * FROM hr_db.employees WHERE id = {id} UNION SELECT 1, user(), 3, 4, 5",
        "SELECT * FROM sales_db.customers INTO OUTFILE '/tmp/hack.csv'",
        "DROP TABLE sales_db.orders",
        "UPDATE hr_db.salaries SET amount = amount * 100 WHERE employee_id = {id}",
        "SELECT * FROM mysql.user"
    ] + ask_ai("SQL Injection, Data Dumping, Privilege Escalation")

    with open("simulation/query_library.json", "w", encoding="utf-8") as f:
        json.dump(library, f, indent=2)
    print("✅ Đã tạo xong kho query: simulation/query_library.json")

if __name__ == "__main__":
    main()