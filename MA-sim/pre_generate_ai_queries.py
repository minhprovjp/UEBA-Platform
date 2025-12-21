
import json
import random
import requests
import time
from typing import Dict, List, Any
import mysql.connector

# DB Config (matching established simulation config)
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root"
}

def dry_run_query(query: str, database: str) -> bool:
    """
    Execute query in a transaction and rollback. 
    Returns True if valid, False if error.
    """
    conn = None
    try:
        # Check for specific database specification in query if consistent with target
        # (The query might cross-join, but primary connection is 'database')
        
        config = DB_CONFIG.copy()
        config["database"] = database
        
        conn = mysql.connector.connect(**config)
        conn.start_transaction()
        cursor = conn.cursor()
        
        # We only want to test execution, not fetch huge results
        # Adding LIMIT 0 is a safe optimization for SELECTs to check syntax/validity 
        # without load, but strict SQL might fail if logic relies on data. 
        # For simulation queries like "UPDATE ... WHERE id IN (SELECT ...)" we should just run it.
        # But we MUST rollback.
        
        cursor.execute(query)
        
        # If we got here, syntax and tables are correct.
        conn.rollback()
        return True
        
    except mysql.connector.Error as err:
        # print(f"    [MySQL Error] {err}") # Optional: debug output
        return False
    except Exception as e:
        # print(f"    [Error] {e}")
        return False
    finally:
        if conn and conn.is_connected():
            conn.rollback() # Double safety
            conn.close()

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
    if database == 'sales_db': 
        prompt += "Table: customers. Columns: customer_id, company_name, contact_person. Table: products (in sales_db). Table: order_items (item_id, order_id, product_id, quantity, unit_price). NO customer_id in order_items (join via orders). "
        if intent == "CHECK_INVENTORY": prompt += "To check stock, JOIN inventory_db.inventory_levels i ON p.product_id = i.product_id. "
        if intent == "UPDATE_STATUS": prompt += "Target table: orders. Update 'status' column. Valid statuses: 'draft','confirmed','processing','shipped','delivered','cancelled'. Avoid subqueries on the same table. "

    if database == 'marketing_db': 
        prompt += "Table: campaigns. Columns: campaign_id, campaign_name, status, campaign_type. NO lead_assignments table. "
        if intent == "UPDATE_LEAD_STATUS": prompt += "Target table: marketing_db.leads. Column: status. Valid values: 'new','contacted','qualified','proposal','negotiation','won','lost'. "
        if intent == "SEARCH_LEAD": prompt += "Table: marketing_db.leads. Columns: lead_id, company_name, contact_name, status. "

    if database == 'finance_db': 
        prompt += "Table: invoices. Columns: invoice_id, customer_id, total_amount, status (ENUM), invoice_date. NO account_id/code columns. NO 'customers'/'products'/'orders' tables in finance_db (use sales_db.customers, sales_db.products). "
        if intent == "GENERATE_REPORT": prompt += "Select straightforward aggregation (SUM/COUNT) from invoices. Do NOT use complex window functions. "
        if intent == "CHECK_PAYMENT": prompt += "Table: invoices. Check 'paid_amount' and 'status'. "

    if database == 'support_db': 
        prompt += "Table: support_tickets. Columns: ticket_id, subject, status (ENUM, not a table). NO support_ticket_status/enum_status tables. "
        if intent == "VIEW_TICKET": prompt += "JOIN with sales_db.customers is optional but allowed. "

    if database == 'inventory_db': 
        prompt += "Table: inventory_levels. Columns: product_id, current_stock, available_stock. Table: products ONLY in sales_db (join sales_db.products p ON i.product_id = p.product_id). "
        if intent == "UPDATE_STOCK": prompt += "Target table: inventory_levels. Update 'current_stock' or 'available_stock'. "
        if intent == "CHECK_STOCK": prompt += "Select product_id and current_stock from inventory_levels. "

    if database == 'admin_db': prompt += "Table: system_logs. Columns: log_id, message. Table: user_sessions. "

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
        if "enumeration_support_status" in query: return False
        if "ticket_types" in query: return False
        if "lead_activities" in query: return False

    if database == 'marketing_db':
        if "lead_assignments" in query: return False
        if "lead_activity" in query: return False
        if re.search(r'\blead\b', query, re.IGNORECASE) and "leads" not in query: return False # "lead" table does not exist, only "leads"

    if database == 'sales_db':
        if "order_item " in query: return False # Singular is wrong
        if "ORDER_ITEM " in query: return False
        # Catch customer_name in orders
        if re.search(r'\b(orders|o)\.customer_name\b', query, re.IGNORECASE): return False

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
            
            while len(pool[db][intent]) < NUM_QUERIES_PER_INTENT and attempts < needed * 5:
                attempts += 1
                q = generate_query(db, intent)
                if q and validate_query(q, db):
                    # Clean up
                    q = q.replace('```sql', '').replace('```', '').strip()
                    if not q.endswith(';'): q += ";"
                    
                    if q not in pool[db][intent]:
                        # FINAL CHECK: Dry run against actual DB
                        if dry_run_query(q, db):
                            pool[db][intent].append(q)
                            print(".", end="", flush=True)
                        else:
                            # print(f"Invalid SQL: {q}")
                            print("x", end="", flush=True)
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
