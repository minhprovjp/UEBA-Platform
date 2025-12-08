# simulation_v2/translator.py
import random
from faker import Faker
from sql_templates import SQL_TEMPLATES

fake = Faker()

class SQLTranslator:
    def __init__(self, db_state):
        self.db_state = db_state # Dùng để lấy random city, product_id nếu cần

    def translate(self, intent):
        """
        Input: Intent dict {'role': 'SALES', 'action': 'VIEW_ORDER', 'params': {'order_id': 105}}
        Output: String SQL "SELECT * FROM ... WHERE order_id = 105"
        """
        role = intent['role']
        action = intent['action']
        params = intent['params'].copy() # Copy để không ảnh hưởng dict gốc
        
        # 1. Lấy Template
        templates = SQL_TEMPLATES.get(role, {}).get(action)
        if not templates:
            return f"SELECT 'Missing template for {action}'"
        
        # Nếu có nhiều mẫu query cho 1 hành động, chọn ngẫu nhiên
        if isinstance(templates, list):
            sql_template = random.choice(templates)
        else:
            sql_template = templates

        # 2. Bổ sung dữ liệu còn thiếu (Filler Data)
        # Agent chỉ quan tâm ID để giữ Context, còn các dữ liệu như 'city', 'amount'
        # không ảnh hưởng đến logic luồng nên ta có thể random tại đây.
        
        if "{city}" in sql_template and "city" not in params:
            # Lấy city thực tế từ DB State để query có kết quả
            cities = self.db_state.get("cities", ["Hanoi"])
            params["city"] = random.choice(cities)
            
        if "{name}" in sql_template and "name" not in params:
            params["name"] = fake.first_name()
            
        if "{phone}" in sql_template:
            params["phone"] = fake.phone_number()[:20]
            
        if "{amount}" in sql_template:
            params["amount"] = round(random.uniform(100, 5000), 2)
            
        if "{price}" in sql_template:
            params["price"] = round(random.uniform(10, 500), 2)
            
        if "{quantity}" in sql_template:
            params["quantity"] = random.randint(1, 10)
            
        if "{bonus}" in sql_template:
            params["bonus"] = random.randint(100, 1000)
            
        if "{status}" in sql_template:
             params["status"] = random.choice(['Pending', 'Completed', 'Cancelled'])
             
        if "{date}" in sql_template:
            params["date"] = "2025-01-01"
            
        if "{dept_id}" in sql_template and "dept_id" not in params:
             params["dept_id"] = random.choice(self.db_state.get("dept_ids", [1]))
             
        # Nếu template cần product_id mà agent chưa đưa (ví dụ ADD_ITEM)
        if "{product_id}" in sql_template and "product_id" not in params:
             params["product_id"] = random.choice(self.db_state.get("product_ids", [1]))

        # 3. Format chuỗi SQL
        try:
            final_sql = sql_template.format(**params)
            return final_sql
        except KeyError as e:
            return f"SELECT 'Error formatting SQL: Missing param {e}'"