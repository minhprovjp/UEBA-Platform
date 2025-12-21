
import json
import random
import requests
import time
from typing import Dict, List, Any

# CONFIG
OLLAMA_URL = "http://100.92.147.73:11434/api/generate"
MODEL = "uba-sqlgen"
OUTPUT_FILE = "dynamic_sql_generation/ai_query_pool.json"
NUM_QUERIES_PER_INTENT = 10  # Start small for testing, generally 10-20

# TARGETS
TARGETS = {
    "sales_db": [
        "SEARCH_CUSTOMER", "VIEW_ORDER", "CHECK_INVENTORY", "UPDATE_STATUS"
    ],
    "hr_db": [
        "VIEW_PROFILE", "SEARCH_EMPLOYEE", "CHECK_SALARY"
    ],
    "marketing_db": [
        "VIEW_CAMPAIGN", "SEARCH_LEAD", "UPDATE_LEAD_STATUS"
    ],
    "finance_db": [
        "VIEW_INVOICE", "CHECK_PAYMENT", "GENERATE_REPORT"
    ],
    "support_db": [
        "VIEW_TICKET", "SEARCH_TICKET", "UPDATE_TICKET"
    ],
    "inventory_db": [
        "CHECK_STOCK", "VIEW_PRODUCT", "UPDATE_STOCK"
    ],
    "admin_db": [
        "CHECK_LOGS", "VIEW_SESSIONS"
    ]
}

def generate_query(database: str, intent: str) -> str:
    prompt = f"Generate a MySQL query for {database}. "
    prompt += f"Intent: {intent}. "
    prompt += "Use valid schema. "
    
    # Specific Schema hints to guide the model (Lightweight context)
    if database == 'hr_db': prompt += "Table: employees. Columns: id, name, email, dept_id. Table: departments. Column: dept_name (NOT department_name). "
    if database == 'sales_db': prompt += "Table: customers. Columns: customer_id, company_name, contact_person. Table: products (in sales_db). "
    if database == 'marketing_db': prompt += "Table: campaigns. Columns: campaign_id, campaign_name, status, campaign_type. NO lead_assignments table. "
    if database == 'finance_db': prompt += "Table: invoices. Columns: invoice_id, customer_id, total_amount, status (ENUM), invoice_date. NO account_id/code columns. NO 'customers'/'products'/'orders' tables in finance_db (use sales_db.customers, sales_db.products). "
    if database == 'support_db': prompt += "Table: support_tickets. Columns: ticket_id, subject, status (ENUM, not a table). NO support_ticket_status/enum_status tables. "
    if database == 'inventory_db': prompt += "Table: inventory_levels. Columns: product_id, current_stock. "
    if database == 'admin_db': prompt += "Table: system_logs. Columns: log_id, message. "

    prompt += "Return ONLY the SQL query, no markdown. Ends with semicolon. NO LIMIT inside IN() subqueries. NO '[database name]' placeholders."

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.7, # Higher temp for variety since we validate offline
                    "stop": [";"]
                }
            },
            timeout=30
        )
        if response.status_code == 200:
            return response.json().get('response', '').strip()
    except Exception as e:
        print(f"Error generating for {database}:{intent}: {e}")
    return None

import re

def validate_query(query: str, database: str) -> bool:
    if not query: return False
    q_upper = query.upper()
    
    # 1. Basic SQL Validation
    if "SELECT" not in q_upper and "UPDATE" not in q_upper: return False
    if "..." in query: return False # Hallucination logic
    if "[DATABASE NAME]" in q_upper: return False # Placeholder
    
    # 2. Reject Hyphens in Table Names (e.g. order-items)
    # Handle backticks: `order-items`
    if re.search(r'[`\'"]?[a-zA-Z0-9_]+-[a-zA-Z0-9_]+[`\'"]?', query):
        return False

    # 3. Reject Known Hallucinations per DB
    if database == 'finance_db':
        # No products or customers table in finance_db
        if re.search(r'finance_db\.(products|customers|orders)', query, re.IGNORECASE): return False
        if re.search(r'\b(products|customers|orders)\b', query, re.IGNORECASE) and "sales_db" not in query:
             pass 

    if database == 'support_db':
        if "support_ticket_status" in query: return False
        if "enum_status" in query: return False
        if "lead_activities" in query: return False

    if database == 'marketing_db':
        if "lead_assignments" in query: return False
        if "lead_activity" in query: return False

    if database == 'sales_db':
        if "order_item " in query: return False # Singular is wrong
        if "ORDER_ITEM " in query: return False

    # 4. Reject MySQL 1093 Pattern (Update target in FROM clause)
    if "UPDATE" in q_upper and "SELECT" in q_upper:
        match = re.search(r'UPDATE\s+([a-zA-Z0-9_.]+)', query, re.IGNORECASE)
        if match:
            table_name = match.group(1).split('.')[-1]
            if re.search(r'FROM\s+([a-zA-Z0-9_.]+\.)?' + re.escape(table_name) + r'\b', query, re.IGNORECASE):
                return False

    # 5. Bad Joins (heuristic)
    # order_items does NOT have customer_id.
    # Catch: order_items.customer_id, oi.customer_id, etc.
    if re.search(r'\b(order_items|oi)\.customer_id\b', query, re.IGNORECASE): return False
    if "customers.customer_id" in query and "order_items.customer_id" in query: return False

    return True

def main():
    print("🚀 Starting Offline AI SQL Generation...")
    
    # Load existing pool if available
    pool = {}
    total_existing = 0
    try:
        import os
        if os.path.exists(OUTPUT_FILE):
            print(f"📖 Loading existing pool from {OUTPUT_FILE}...")
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                pool = json.load(f)
                
            # Count existing
            for db, intents in pool.items():
                for intent, queries in intents.items():
                    total_existing += len(queries)
            print(f"   Found {total_existing} existing queries.")
    except Exception as e:
        print(f"⚠️ Could not load existing pool: {e}")
        pool = {}

    for db, intents in TARGETS.items():
        if db not in pool:
            pool[db] = {}
            
        print(f"\n📂 Database: {db}")
        for intent in intents:
            if intent not in pool[db]:
                pool[db][intent] = []
                
            current_count = len(pool[db][intent])
            if current_count >= NUM_QUERIES_PER_INTENT:
                print(f"  Skipping {intent} (Already has {current_count} queries)")
                continue
                
            print(f"  Targeting: {intent} (Current: {current_count})", end="", flush=True)
            
            attempts = 0
            # We want to reach NUM_QUERIES_PER_INTENT total
            needed = NUM_QUERIES_PER_INTENT - current_count
            
            while len(pool[db][intent]) < NUM_QUERIES_PER_INTENT and attempts < needed * 3:
                attempts += 1
                q = generate_query(db, intent)
                if q and validate_query(q, db):
                    # Clean up
                    q = q.replace('```sql', '').replace('```', '').strip()
                    if not q.endswith(';'): q += ";"
                    
                    if q not in pool[db][intent]:
                        pool[db][intent].append(q)
                        print(".", end="", flush=True)
                    else:
                        # Duplicate found
                        pass
                else:
                    print("x", end="", flush=True)
            
            print(f" [Total: {len(pool[db][intent])}]")

    print(f"\n💾 Saving pool to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(pool, f, indent=4)
    print("✅ Done!")

if __name__ == "__main__":
    main()
