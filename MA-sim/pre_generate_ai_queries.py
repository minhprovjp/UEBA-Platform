
import json
import random
import requests
import time
from typing import Dict, List, Any

# CONFIG
OLLAMA_URL = "http://100.92.147.73:11434/api/generate"
MODEL = "uba-sqlgen"
OUTPUT_FILE = "dynamic_sql_generation/ai_query_pool.json"
NUM_QUERIES_PER_INTENT = 20  # Start small for testing, generally 10-20

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
    if database == 'hr_db': prompt += "Table: employees. Columns: id, name, email, dept_id. "
    if database == 'sales_db': prompt += "Table: customers. Columns: customer_id, company_name, contact_name. "
    if database == 'marketing_db': prompt += "Table: campaigns. Columns: campaign_id, campaign_name, status. "
    if database == 'finance_db': prompt += "Table: invoices. Columns: invoice_id, customer_id, total_amount. "
    if database == 'support_db': prompt += "Table: support_tickets. Columns: ticket_id, subject, status. "
    if database == 'inventory_db': prompt += "Table: inventory_levels. Columns: product_id, current_stock. "
    if database == 'admin_db': prompt += "Table: system_logs. Columns: log_id, message. "

    prompt += "Return ONLY the SQL query, no markdown. Ends with semicolon."

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

def validate_query(query: str, database: str) -> bool:
    if not query: return False
    if "SELECT" not in query.upper() and "UPDATE" not in query.upper(): return False
    if "..." in query: return False # Hallucination
    if database == 'hr_db' and 'full_name' in query: return False # Bad schema
    return True

def main():
    print("🚀 Starting Offline AI SQL Generation...")
    pool = {}

    for db, intents in TARGETS.items():
        pool[db] = {}
        print(f"\n📂 Database: {db}")
        for intent in intents:
            pool[db][intent] = []
            print(f"  Targeting: {intent}", end="", flush=True)
            
            attempts = 0
            while len(pool[db][intent]) < NUM_QUERIES_PER_INTENT and attempts < NUM_QUERIES_PER_INTENT * 2:
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
                    print("x", end="", flush=True)
            
            print(f" [Done: {len(pool[db][intent])}]")

    print(f"\n💾 Saving pool to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(pool, f, indent=4)
    print("✅ Done!")

if __name__ == "__main__":
    main()
