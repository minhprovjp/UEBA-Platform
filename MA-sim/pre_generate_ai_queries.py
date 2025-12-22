
import json
import random
import requests
import time
from typing import Dict, List, Any
import mysql.connector
import re
import os

# DB Config (matching established simulation config)
DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root"
}

# DUMMY VALUES FOR DRY RUN VALIDATION
DUMMY_VALUES = {
    "{customer_id}": "1",
    "{product_id}": "1",
    "{order_id}": "1",
    "{item_id}": "1",
    "{category_id}": "1",
    "{payment_id}": "1",
    "{employee_id}": "1",
    "{dept_id}": "1",
    "{ticket_id}": "1",
    "{location_id}": "1",
    "{lead_id}": "1",
    "{campaign_id}": "1",
    "{invoice_id}": "1",
    "{log_id}": "1",
    "{start_date}": "2025-01-01",
    "{end_date}": "2025-12-31",
    "{status}": "active",
    "{year}": "2025",
    "{limit}": "10",
    "{order_status}": "shipped",
    "{min_stock_level}": "10",
    "{min_stock_threshold}": "5",
    "{search_term}": "test",
    "{lead_status}": "new",
    "{ticket_status}": "open",
    "{customer_code}": "C001",
    "{email}": "test@example.com",
    "{phone}": "1234567890",
    "{is_active}": "1",
    "{report_type}": "daily",
    "{user_id}": "1",
    "{frequency}": "daily",
    "{quantity}": "5",
    "{amount}": "100",
    "{number}": "10",
    "{bonus}": "10",
    "{rating}": "5",
    "{stock_quantity}": "10"
}

def normalize_sql(sql: str) -> str:
    """
    Normalize SQL query for duplicate detection.
    
    Normalization steps:
    1. Convert to lowercase
    2. Remove SQL comments (-- and /* ... */)
    3. Replace multiple spaces/newlines with single space
    4. Replace all placeholders {xxx} with {PLACEHOLDER} for comparison
    5. Strip leading/trailing whitespace
    
    This ensures that queries that are semantically identical but differ
    in formatting or placeholder names are recognized as duplicates.
    """
    if not sql:
        return ""
    
    # Remove single-line comments (-- comments)
    sql = re.sub(r'--[^\n]*', '', sql)
    
    # Remove multi-line comments (/* ... */)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    
    # Convert to lowercase
    sql = sql.lower()
    
    # Replace all placeholders with standardized token
    # This ensures {customer_id} and {user_id} are treated as the same
    sql = re.sub(r'\{[^}]+\}', '{PLACEHOLDER}', sql)
    
    # Replace multiple whitespace (spaces, tabs, newlines) with single space
    sql = re.sub(r'\s+', ' ', sql)
    
    # Strip leading and trailing whitespace
    sql = sql.strip()
    
    return sql

def dry_run_query(query_template: str, database: str) -> (bool, str): # type: ignore
    """
    Hydrate template with DUMMY values and execute in a transaction.
    Returns (True, "") if valid, (False, error_msg) if error.
    """
    # Hydrate with dummy values
    test_query = query_template
    for placeholder, val in DUMMY_VALUES.items():
        test_query = test_query.replace(placeholder, val)
    
    conn = None
    try:
        config = DB_CONFIG.copy()
        config["database"] = database
        
        conn = mysql.connector.connect(**config)
        conn.start_transaction()
        cursor = conn.cursor()
        
        cursor.execute(test_query)
        
        conn.rollback()
        return True, ""
        
    except mysql.connector.Error as err:
        return False, f"MySQL Error: {err} (Test Query: {test_query})"
    except Exception as e:
        return False, f"Execution Error: {e}"
    finally:
        if conn and conn.is_connected():
            conn.rollback() # Double safety
            conn.close()

# CONFIG
OLLAMA_URL = "http://100.92.147.73:11434/api/generate"
MODEL = "uba-sqlgen"
OUTPUT_FILE = "dynamic_sql_generation/ai_query_pool.json"
NUM_QUERIES_PER_INTENT = 0  # Increased for templates
ATTACK_QUERIES_PER_INTENT = 50

# TARGETS
# TARGETS
TARGETS = {
    "sales_db": [
        "SEARCH_CUSTOMER", "VIEW_ORDER", "CHECK_INVENTORY", "UPDATE_STATUS", "SALES_REPORT"
    ],
    "hr_db": [
        "VIEW_PROFILE", "SEARCH_EMPLOYEE", "CHECK_SALARY", "UPDATE_INFO"
    ],
    "marketing_db": [
        "VIEW_CAMPAIGN", "SEARCH_LEAD", "UPDATE_LEAD_STATUS", "CAMPAIGN_ROI"
    ],
    "finance_db": [
        "VIEW_INVOICE", "CHECK_PAYMENT", "GENERATE_REPORT", "OVERDUE_CHECK"
    ],
    "support_db": [
        "VIEW_TICKET", "SEARCH_TICKET", "UPDATE_TICKET", "MY_TICKETS"
    ],
    "inventory_db": [
        "CHECK_STOCK", "VIEW_PRODUCT", "UPDATE_STOCK", "LOW_STOCK_ALERT"
    ],
    "admin_db": [
        "CHECK_LOGS", "VIEW_SESSIONS", "SYSTEM_HEALTH"
    ]
}

# ATTACK/ANOMALY TARGETS - for generating malicious query patterns
ATTACK_TARGETS = {
    "sales_db": [
        "SQL_INJECTION", "DATA_EXFILTRATION", "UNION_ATTACK", "PRIVILEGE_ESCALATION"
    ],
    "hr_db": [
        "SALARY_DATA_THEFT", "EMPLOYEE_INFO_EXTRACTION", "SQL_INJECTION"
    ],
    "marketing_db": [
        "CAMPAIGN_DATA_THEFT", "LEAD_EXTRACTION", "SQL_INJECTION"
    ],
    "finance_db": [
        "FINANCIAL_DATA_THEFT", "INVOICE_MANIPULATION", "UNION_ATTACK"
    ],
    "support_db": [
        "TICKET_DATA_EXTRACTION", "SQL_INJECTION"
    ],
    "inventory_db": [
        "STOCK_DATA_THEFT", "PRICE_MANIPULATION"
    ],
    "admin_db": [
        "LOG_TAMPERING", "SESSION_HIJACKING", "PRIVILEGE_ESCALATION"
    ]
}

def generate_query(database: str, intent: str) -> str:
    # Detect if this is an attack/anomaly intent
    is_attack = intent in [
        "SQL_INJECTION", "DATA_EXFILTRATION", "UNION_ATTACK", "PRIVILEGE_ESCALATION",
        "SALARY_DATA_THEFT", "EMPLOYEE_INFO_EXTRACTION", "CAMPAIGN_DATA_THEFT",
        "LEAD_EXTRACTION", "FINANCIAL_DATA_THEFT", "INVOICE_MANIPULATION",
        "TICKET_DATA_EXTRACTION", "STOCK_DATA_THEFT", "PRICE_MANIPULATION",
        "LOG_TAMPERING", "SESSION_HIJACKING"
    ]
    
    # Force simple queries for UPDATE intents to ensure tag compliance
    is_update = intent.startswith('UPDATE_')
    
    if is_attack:
        # Attack queries should be moderate to complex for detection training
        complexity_level = random.choice(['moderate', 'complex', 'complex'])  # 33% moderate, 67% complex
        use_joins = random.random() < 0.6  # 60% chance
        use_aggregation = random.random() < 0.4  # 40% chance
        use_sorting = False
        use_grouping = False
        use_limit = random.random() < 0.7  # 70% chance for large data extraction
    elif is_update:
        # UPDATEs must be simple - no enrichments to avoid tag failures
        complexity_level = 'simple'
        use_joins = False
        use_aggregation = False
        use_sorting = False
        use_grouping = False
        use_limit = False
    else:
        # Randomly choose complexity level for variety
        complexity_level = random.choice(['simple', 'simple', 'moderate', 'complex'])  # 50% simple, 25% moderate, 25% complex
        
        # Randomly choose enrichment options
        use_joins = random.random() < 0.4  # 40% chance of JOINs
        use_aggregation = random.random() < 0.3  # 30% chance of aggregations
        use_sorting = random.random() < 0.5  # 50% chance of ORDER BY
        use_grouping = random.random() < 0.2  # 20% chance of GROUP BY
        use_limit = random.random() < 0.6  # 60% chance of LIMIT
    
    # Special prompts for attack queries
    if is_attack:
        prompt = "YOU MUST wrap your response in [SQL] and [/SQL] tags. "
        prompt += f"Generate a MALICIOUS MySQL query TEMPLATE for {database}. "
        prompt += f"Attack Type: {intent}. "
        prompt += "This is for anomaly detection training - create realistic attack patterns. "
        
        # Attack-specific instructions
        if intent == "SQL_INJECTION":
            prompt += "Pattern: Use UNION SELECT, OR 1=1, comment injection (--), or string concatenation vulnerabilities. "
            prompt += "Example: SELECT * FROM users WHERE username = '{username}' OR '1'='1' --; "
        elif intent == "CAMPAIGN_DATA_THEFT":
            prompt += "Pattern: Extract ALL campaign data including budgets and sensitive info. "
            prompt += "Example: SELECT campaign_id, campaign_name, budget, status FROM campaigns LIMIT 10000; "
        elif intent == "LEAD_EXTRACTION":
            prompt += "Pattern: Extract ALL leads with contact information for unauthorized use. "
            prompt += "Example: SELECT lead_id, email, phone, lead_source, status FROM leads WHERE status = '{status}' LIMIT 5000; "
        elif intent == "DATA_EXFILTRATION" or "THEFT" in intent or "EXTRACTION" in intent:
            prompt += "Pattern: Extract large amounts of sensitive data using SELECT without proper WHERE clauses, or JOIN multiple tables to combine sensitive info. "
            prompt += "Use excessive LIMIT values for bulk extraction. "
        elif intent == "UNION_ATTACK":
            prompt += "Pattern: Use UNION SELECT to combine results from multiple tables or expose schema. "
            prompt += "Example: SELECT * FROM products UNION SELECT table_name, null, null FROM information_schema.tables; "
        elif intent == "PRIVILEGE_ESCALATION":
            prompt += "Pattern: Attempt to access system tables, modify permissions, or query information_schema. "
        elif intent == "LOG_TAMPERING":
            prompt += "Pattern: DELETE or UPDATE system logs to hide tracks. "
        elif intent == "PRICE_MANIPULATION" or intent == "INVOICE_MANIPULATION":
            prompt += "Pattern: UPDATE prices or financial data to fraudulent values. "
        
        # Database-specific schema hints for attack queries
        if database == 'marketing_db':
            prompt += "Schema: campaigns (campaign_id, campaign_name, budget, status), leads (lead_id, lead_source, status, email). "
            if "CAMPAIGN" in intent or "LEAD" in intent:
                prompt += "Target sensitive campaign budgets and lead contact data. "
        elif database == 'hr_db':
            prompt += "Schema: employees (id, name, email, salary, dept_id, status). "
            if "SALARY" in intent or "EMPLOYEE" in intent:
                prompt += "Target salary and personal employee information. "
        elif database == 'finance_db':
            prompt += "Schema: invoices (invoice_id, amount, status, customer_id). "
        elif database == 'support_db':
            prompt += "Schema: support_tickets (ticket_id, customer_id, issue_details, status). "
        elif database == 'inventory_db':
            prompt += "Schema: inventory_levels (product_id, current_stock, price, location_id). "
        elif database == 'admin_db':
            prompt += "Schema: system_logs (log_id, user_id, action, timestamp), user_sessions (session_id, user_id, login_time). "
        elif database == 'sales_db':
            prompt += "Schema: customers (customer_id, company_name, email), orders (order_id, customer_id, total_amount, status). "
        
        
        prompt += "IMPORTANT: Use placeholders {placeholder_name} for dynamic values. "
        prompt += "Make the query look suspicious but syntactically valid. "
        prompt += "\n\nFORMAT REQUIREMENT (CRITICAL):\n"
        prompt += "You MUST return ONLY: [SQL] your_query_here; [/SQL]\n"
        prompt += "Example: [SQL] SELECT * FROM table WHERE id = {id}; [/SQL]\n"
        prompt += "NO explanations. NO markdown. NO extra text. JUST the [SQL] tags with query inside."
    else:
        # Normal query prompts
        prompt = f"Generate a MySQL query TEMPLATE for {database}. "
        prompt += f"Intent: {intent}. "
        prompt += "Use valid schema. "
        
        # Template Instructions
        prompt += "IMPORTANT: Use placeholders {placeholder_name} for dynamic values. "
        prompt += "Supported placeholders: {customer_id}, {product_id}, {order_id}, {lead_id}, {ticket_id}, {employee_id}, {start_date}, {end_date}, {status}. "
        prompt += "Do NOT use concrete IDs. "
    
    # Complexity-based instructions
    if complexity_level == 'simple':
        prompt += "Create a SIMPLE query (1-2 lines). Single table, basic WHERE clause. "
    elif complexity_level == 'moderate':
        prompt += "Create a MODERATE complexity query (2-3 lines). "
        if use_joins:
            prompt += "Use JOINs to combine related tables. "
        if use_aggregation:
            prompt += "Include aggregation functions (COUNT, SUM, AVG). "
    else:  # complex
        prompt += "Create a COMPLEX query (3-5 lines). "
        if use_joins:
            prompt += "Use multiple JOINs to combine 2-3 related tables. "
        if use_aggregation:
            prompt += "Include aggregation functions (COUNT, SUM, AVG, MIN, MAX). "
        if use_grouping:
            prompt += "Use GROUP BY for grouping results. "
    
    # Enrichment options
    enrichments = []
    if use_sorting:
        enrichments.append("Add ORDER BY for sorting")
    if use_limit:
        enrichments.append("Add LIMIT {limit} to limit results")
    
    if enrichments:
        prompt += f"Enrich the query: {', '.join(enrichments)}. "
    
    # Important constraints - strengthened for UPDATE intents
    prompt += "IMPORTANT: Enclose string and date placeholders in single quotes (e.g. '{status}', '{start_date}'). "
    if is_update:
        prompt += "CRITICAL: This is an UPDATE query. You MUST wrap it in [SQL] and [/SQL] tags. Example: [SQL] UPDATE table SET col = 'value' WHERE id = 1; [/SQL]. "
        prompt += "Return ONLY the wrapped SQL. NO explanations, NO markdown, NO extra text. "
    else:
        prompt += "CRITICAL: Wrap the SQL template in [SQL] and [/SQL] tags. Example: [SQL] SELECT * FROM table; [/SQL]. "
    prompt += "Return ONLY the SQL template wrapped in tags. Ends with semicolon. NO markdown."

    # Specific Schema hints with flexibility
    if database == 'hr_db': 
        prompt += "Tables: employees (id, name, email, dept_id, status, created_at, updated_at). "
        if complexity_level != 'simple':
            prompt += "Can JOIN with departments, attendance, or other HR tables. "
    
    if database == 'sales_db': 
        prompt += "Tables: customers, products, orders, order_items. "
        prompt += "Relationships: orders -> order_items -> products, orders -> customers. "
        prompt += "order_items has NO status. orders has status. "
        
        if intent == "UPDATE_STATUS": 
            # Provide example instead of exact template for variety
            prompt += "Example pattern: UPDATE orders SET status = '{order_status}' WHERE order_id = {order_id}; "
            prompt += "VARY the query: add date conditions, multiple SET clauses, or combine with other columns. "
            prompt += "Use '{order_status}' NOT '{status}'. "
        elif intent == "CHECK_INVENTORY": 
            # CRITICAL: inventory_levels is in inventory_db, NOT sales_db
            prompt += "CRITICAL: inventory_levels table is in inventory_db. "
            prompt += "Use inventory_db.inventory_levels NOT sales_db.inventory_levels. "
            prompt += "inventory_levels has NO 'status' column - do NOT use il.status or i.status. "
            if complexity_level == 'simple':
                prompt += "Simple: SELECT available_stock FROM inventory_db.inventory_levels WHERE product_id = {product_id}. "
            else:
                prompt += "For JOINs: JOIN inventory_db.inventory_levels il ON ... "
                prompt += "Example: FROM sales_db.products p JOIN inventory_db.inventory_levels il ON p.product_id = il.product_id. "

    if database == 'marketing_db': 
        prompt += "Tables: campaigns (campaign_id, campaign_name, start_date, end_date, budget, status), leads (lead_id, status, lead_source, created_at). "
        prompt += "Note: NO direct link between leads/campaigns. "
        if intent == "UPDATE_LEAD_STATUS": 
            prompt += "Example: UPDATE leads SET status = '{lead_status}' WHERE lead_id = {lead_id}; "
            prompt += "VARY: add date filters, update multiple columns, or add conditions. Use '{lead_status}' NOT '{status}'. "

    if database == 'finance_db': 
        prompt += "Table: invoices. NO 'products'/'customers' tables (use sales_db). "

    if database == 'support_db': 
        prompt += "Tables: support_tickets (ticket_id, customer_id, status, priority, created_at, updated_at). "
        if complexity_level != 'simple':
            prompt += "Can JOIN with customers for customer details. "
        if intent == "UPDATE_TICKET":
            # Provide multiple example patterns for variety
            patterns = [
                "UPDATE support_tickets SET status = '{ticket_status}' WHERE ticket_id = {ticket_id};",
                "UPDATE support_tickets SET priority = 'high', status = '{ticket_status}' WHERE ticket_id = {ticket_id};",
                "UPDATE support_tickets SET status = '{ticket_status}', updated_at = NOW() WHERE ticket_id = {ticket_id};"
            ]
            prompt += f"Use one of these patterns: {'; '.join(patterns[:2])} "
            prompt += "MUST use '{ticket_status}' NOT '{status}'. Vary the query slightly. "

    if database == 'inventory_db': 
        prompt += "Tables: inventory_levels (product_id, available_stock, current_stock, min_stock_level, max_stock_level, location_id). "
        prompt += "NO 'status' column, NO 'quantity_on_hand'. "
        
        if intent == "CHECK_STOCK":
            if complexity_level == 'simple':
                prompt += "Simple: SELECT available_stock WHERE product_id. "
            else:
                prompt += "Can include location_id, stock levels, reorder indicators. "
        elif intent == "UPDATE_STOCK":
            # Provide multiple example patterns with {quantity} placeholder
            patterns = [
                "UPDATE inventory_levels SET current_stock = current_stock + {quantity} WHERE product_id = {product_id} AND location_id = {location_id};",
                "UPDATE inventory_levels SET available_stock = available_stock + {quantity} WHERE product_id = {product_id};",
                "UPDATE inventory_levels SET current_stock = current_stock - {quantity}, available_stock = available_stock - {quantity} WHERE product_id = {product_id} AND location_id = {location_id};"
            ]
            prompt += f"Use one of these patterns: {patterns[0]} OR {patterns[1]} "
            prompt += "Use {quantity} placeholder for amount. Choose + or - for arithmetic. "
        elif intent == "LOW_STOCK_ALERT":
            if complexity_level == 'simple':
                prompt += "Simple: SELECT * WHERE available_stock < {min_stock_level}. "
            else:
                prompt += "Include calculations, ordering by stock level, location filtering. "
             
    if database == 'admin_db':
        prompt += "Table: report_schedules, user_sessions. "
        prompt += "report_schedules has 'is_active' (int), NO 'status'. Use {is_active} placeholder. "
        prompt += "user_sessions has 'is_active' (int), 'login_time', 'last_activity'. NO 'created_at', NO 'status'. "
        
        if intent == "CHECK_LOGS" or intent == "SYSTEM_HEALTH":
            prompt += "Action: SELECT * FROM report_schedules WHERE is_active = {is_active}; "
        if intent == "VIEW_SESSIONS":
            prompt += "Action: SELECT * FROM user_sessions WHERE is_active = {is_active} AND login_time BETWEEN '{start_date}' AND '{end_date}'; "

    prompt += "Return ONLY the SQL template. Ends with semicolon. NO markdown."

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.9,  # Increased from 0.7 for more variety
                    "top_p": 0.95,       # Allow broader token selection
                    "top_k": 50          # Consider more alternatives
                }
            },
            timeout=90
        )
        if response.status_code == 200:
            content = response.json().get('response', '').strip()
            
            # Remove <think> blocks if present
            if '</think>' in content:
                content = content.split('</think>')[-1].strip()
            elif '<think>' in content:
                # Unclosed think block - risky, try to salvage or fail
                # Try to find SQL start
                match = re.search(r'(SELECT|UPDATE|INSERT|DELETE)\s', content, re.IGNORECASE)
                if match:
                     # Check if match is "too early" (likely inside think text)
                     # Heuristic: If there is a lot of text before SELECT, it's probably chat.
                     # But valid SQL starts immediately.
                     pass 
                else: 
                     content = "" # invalid
            
            # Remove markdown code blocks
            if "```" in content:
                content = content.replace("```sql", "").replace("```", "").strip()
            # Check for [SQL] tags - try multiple patterns
            
            # Pattern 1: Standard [SQL]...[/SQL]
            tag_match = re.search(r'\[SQL\](.*?)\[/SQL\]', content, re.DOTALL | re.IGNORECASE)
            if tag_match:
                extracted_sql = tag_match.group(1).strip()
                return extracted_sql
            
            # Pattern 2: Only opening [SQL] tag (model didn't add closing)
            tag_match = re.search(r'\[SQL\](.+)', content, re.DOTALL | re.IGNORECASE)
            if tag_match:
                extracted_sql = tag_match.group(1).strip()
                if not extracted_sql.endswith(';'):
                    extracted_sql = extracted_sql.rstrip() + ';'
                return extracted_sql
            
            # 2. Model didn't use tags - use aggressive fallback extraction
            # Find where SQL starts
            match = re.search(r'(SELECT|UPDATE|INSERT|DELETE)\s', content, re.IGNORECASE)
            if match:
                # Extract from SQL keyword to end
                sql_start = match.start()
                content = content[sql_start:]
                
                # Remove common trailing explanatory text patterns
                # Look for newlines followed by explanatory text
                cleanup_patterns = [
                    r'\n\s*(Note|Explanation|This query|The query|Example)[:\s].*',  # Explanatory paragraphs
                   r'\n\s*(FORMAT|IMPORTANT|CRITICAL)[:\s].*',  # Instruction repeats
                    r'\n\s*--[^\n]*$',  # Trailing SQL comments at end
                ]
                for pattern in cleanup_patterns:
                    content = re.sub(pattern, '', content, flags=re.IGNORECASE | re.DOTALL)
                
                # Ensure ends with semicolon
                content = content.strip()
                if not content.endswith(';'):
                    # Find the last occurrence of a SQL keyword or closing paren
                    # and add semicolon after it
                    last_paren = content.rfind(')')
                    last_quote = max(content.rfind("'"), content.rfind('"'))
                    last_word_match = re.search(r'\S+\s*$', content)
                    
                    if last_paren > last_quote and last_paren > 0:
                        insert_pos = last_paren + 1
                    elif last_word_match:
                        insert_pos = last_word_match.end()
                    else:
                        insert_pos = len(content)
                    
                    content = content[:insert_pos].rstrip() + ';'
                
                # Final cleanup: remove multiple spaces/newlines
                content = re.sub(r'\s+', ' ', content)

            # FORCE FIX: status placeholders
            if intent == "UPDATE_STATUS": content = content.replace("'{status}'", "'{order_status}'")
            if intent == "UPDATE_LEAD_STATUS": content = content.replace("'{status}'", "'{lead_status}'")
            if intent == "UPDATE_TICKET": content = content.replace("'{status}'", "'{ticket_status}'")
                
            return content
    except Exception as e:
        print(f"Error generating for {database}:{intent}: {e}")
    return None

def validate_query(query: str, database: str) -> (bool, str): # type: ignore
    if not query: return False, "Empty query"
    q_upper = query.upper()
    
    # 1. Basic SQL Validation
    if "SELECT" not in q_upper and "UPDATE" not in q_upper: return False, "Missing SELECT/UPDATE"
    if "..." in query: return False, "Contains ellipsis hallucination"
    
    # 2. Check for Placeholders (We WANT them now)
    if "{" not in query or "}" not in query:
        # It's okay if query selects all, but warn if it looks like concrete ID
        if re.search(r"=\s*\d+", query):
            return False, "Used concrete ID instead of placeholder"

    # 3. Reject Known Hallucinations
    if database == 'sales_db' and "category_name" in query and "product_categories" not in query:
        return False, "Hallucinated products.category_name"
    if database == 'marketing_db' and "campaign" in query and "lead" in query:
         return False, "Invalid Join: campaigns <-> leads"

    # 4. Reject MySQL 1093 Pattern
    if "UPDATE" in q_upper and "SELECT" in q_upper:
        match = re.search(r'UPDATE\s+([a-zA-Z0-9_.]+)', query, re.IGNORECASE)
        if match:
            table_name = match.group(1).split('.')[-1]
            if re.search(r'FROM\s+([a-zA-Z0-9_.]+\.)?' + re.escape(table_name) + r'\b', query, re.IGNORECASE):
                return False, "MySQL 1093 Error Pattern"

    return True, ""

def main():
    print("🚀 Starting Offline AI SQL TEMPLATE Generation...")
    print("=" * 70)
    
    start_time = time.time()
    
    # Open failure log
    fail_log = open("generation_failures.txt", "w", encoding="utf-8")
    fail_log.write(f"Generation Session: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Statistics tracking
    stats = {
        'total_attempts': 0,
        'total_generated': 0,
        'total_duplicates': 0,
        'total_failures': 0,
        'by_database': {},
        'by_intent': {}
    }
    
    pool = {}
    if os.path.exists(OUTPUT_FILE):
        print(f"📂 Loading existing pool from {OUTPUT_FILE}...")
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Handle both old flat structure and new metadata structure
        if "metadata" in data and "queries" in data:
            # New format with metadata
            pool = data["queries"]
            print(f"   ℹ️  Loaded pool with metadata (generated: {data['metadata'].get('generated_at', 'unknown')})")
        else:
            # Old format - direct database->intent mapping
            pool = data
        
        # Global Deduplication of loaded pool using normalization
        print("🔍 Scrubbing duplicates from existing pool...")
        initial_total = 0
        final_total = 0
        for db in pool:
            for intent in pool[db]:
                original_len = len(pool[db][intent])
                initial_total += original_len
                
                # Use normalization for deduplication
                seen_normalized = set()
                unique_queries = []
                for query in pool[db][intent]:
                    normalized = normalize_sql(query)
                    if normalized not in seen_normalized:
                        seen_normalized.add(normalized)
                        unique_queries.append(query)
                
                pool[db][intent] = unique_queries
                final_total += len(unique_queries)
                
                if len(unique_queries) < original_len:
                    removed = original_len - len(unique_queries)
                    print(f"  ✓ Deduped {db}.{intent}: {original_len} → {len(unique_queries)} (-{removed})")
        
        if initial_total > final_total:
            print(f"  📊 Total duplicates removed from existing pool: {initial_total - final_total}")

    for db, intents in TARGETS.items():
        if db not in pool:
            pool[db] = {}
        if db not in stats['by_database']:
            stats['by_database'][db] = {'attempts': 0, 'generated': 0, 'duplicates': 0, 'failures': 0}
        
        print(f"\n📂 Database: {db}")
        
        for intent in intents:
            # Initialize with existing queries if any
            if intent not in pool[db]:
                pool[db][intent] = []
            
            # Build normalized set from existing queries to check for duplicates
            unique_normalized = set()
            for existing_query in pool[db][intent]:
                unique_normalized.add(normalize_sql(existing_query))
            
            existing_count = len(pool[db][intent])
            target_count = NUM_QUERIES_PER_INTENT
            queries_needed = max(0, target_count - existing_count)
            
            if intent not in stats['by_intent']:
                stats['by_intent'][intent] = {'attempts': 0, 'generated': 0, 'duplicates': 0, 'failures': 0}
            
            print(f"  🎯 Intent: {intent}", end="", flush=True)
            
            attempts = 0
            duplicates_this_intent = 0
            failures_this_intent = 0
            
            while len(pool[db][intent]) < NUM_QUERIES_PER_INTENT and attempts < NUM_QUERIES_PER_INTENT * 3:
                attempts += 1
                stats['total_attempts'] += 1
                stats['by_database'][db]['attempts'] += 1
                stats['by_intent'][intent]['attempts'] += 1
                
                q = generate_query(db, intent)
                
                # Cleanup
                if q:
                    q = q.replace('```sql', '').replace('```', '').strip()
                    if not q.endswith(';'):
                        q += ";"

                is_valid, reason = validate_query(q, db)

                if q and is_valid:
                    # STATIC VALIDATION PASSED
                    # NOW TEST HYDRATION AND EXECUTION
                    dry_valid, dry_reason = dry_run_query(q, db)
                        
                    if dry_valid:
                        # Check for duplicates using normalized SQL
                        normalized = normalize_sql(q)
                        if normalized not in unique_normalized:
                            pool[db][intent].append(q)
                            unique_normalized.add(normalized)
                            
                            stats['total_generated'] += 1
                            stats['by_database'][db]['generated'] += 1
                            stats['by_intent'][intent]['generated'] += 1
                            
                            print(".", end="", flush=True)
                        else:
                            # Duplicate detected
                            duplicates_this_intent += 1
                            stats['total_duplicates'] += 1
                            stats['by_database'][db]['duplicates'] += 1
                            stats['by_intent'][intent]['duplicates'] += 1
                            
                            print("d", end="", flush=True)
                    else:
                        failures_this_intent += 1
                        stats['total_failures'] += 1
                        stats['by_database'][db]['failures'] += 1
                        stats['by_intent'][intent]['failures'] += 1
                        
                        print("x", end="", flush=True)
                        fail_log.write(f"[{db}][{intent}] DRY RUN FAIL: {dry_reason}\nTemplate: {q}\n---\n")
                        fail_log.flush()
                else:
                    failures_this_intent += 1
                    stats['total_failures'] += 1
                    stats['by_database'][db]['failures'] += 1
                    stats['by_intent'][intent]['failures'] += 1
                    
                    print("x", end="", flush=True)
                    fail_log.write(f"[{db}][{intent}] STATIC FAIL: {reason}\nTemplate: {q}\n---\n")
                    fail_log.flush()
            
            success_count = len(pool[db][intent])
            print(f" [{success_count}/{NUM_QUERIES_PER_INTENT}]", end="")
            if duplicates_this_intent > 0:
                print(f" (🔄 {duplicates_this_intent} dups)", end="")
            if failures_this_intent > 0:
                print(f" (❌ {failures_this_intent} fails)", end="")
            print()

    # Generate attack/anomaly queries
    print("\n🔴 GENERATING ATTACK/ANOMALY QUERIES")
    print("="*70)
    for db, attack_intents in ATTACK_TARGETS.items():
        if db not in pool:
            pool[db] = {}
        if db not in stats['by_database']:
            stats['by_database'][db] = {'attempts': 0, 'generated': 0, 'duplicates': 0, 'failures': 0}
        
        print(f"\n📂 Database: {db}")
        
        for intent in attack_intents:
            pool[db][intent] = []
            unique_normalized = set()
            
            if intent not in stats['by_intent']:
                stats['by_intent'][intent] = {'attempts': 0, 'generated': 0, 'duplicates': 0, 'failures': 0}
            
            print(f"  🚨 Attack: {intent}", end="", flush=True)
            
            attempts = 0
            duplicates_this_intent = 0
            failures_this_intent = 0
            
            # Generate fewer attack queries (10 per intent)
            attack_query_count = ATTACK_QUERIES_PER_INTENT
            while len(pool[db][intent]) < attack_query_count and attempts < attack_query_count * 3:
                attempts += 1
                stats['total_attempts'] += 1
                stats['by_database'][db]['attempts'] += 1
                stats['by_intent'][intent]['attempts'] += 1
                
                q = generate_query(db, intent)
                
                # Cleanup
                if q:
                    q = q.replace('```sql', '').replace('```', '').strip()
                    if not q.endswith(';'):
                        q += ";"

                is_valid, reason = validate_query(q, db)

                if q and is_valid:
                    # Note: Attack queries might fail dry-run due to their malicious nature
                    # We'll skip dry-run for them to allow syntactically valid but suspicious queries
                    normalized = normalize_sql(q)
                    if normalized not in unique_normalized:
                        pool[db][intent].append(q)
                        unique_normalized.add(normalized)
                        
                        stats['total_generated'] += 1
                        stats['by_database'][db]['generated'] += 1
                        stats['by_intent'][intent]['generated'] += 1
                        
                        print(".", end="", flush=True)
                    else:
                        duplicates_this_intent += 1
                        stats['total_duplicates'] += 1
                        stats['by_database'][db]['duplicates'] += 1
                        stats['by_intent'][intent]['duplicates'] += 1
                        
                        print("d", end="", flush=True)
                else:
                    failures_this_intent += 1
                    stats['total_failures'] += 1
                    stats['by_database'][db]['failures'] += 1
                    stats['by_intent'][intent]['failures'] += 1
                    
                    print("x", end="", flush=True)
                    fail_log.write(f"[{db}][{intent}] STATIC FAIL: {reason}\nTemplate: {q}\n---\n")
                    fail_log.flush()
            
            success_count = len(pool[db][intent])
            print(f" [{success_count}/{attack_query_count}]", end="")
            if duplicates_this_intent > 0:
                print(f" (🔄 {duplicates_this_intent} dups)", end="")
            if failures_this_intent > 0:
                print(f" (❌ {failures_this_intent} fails)", end="")
            print()

    generation_time = time.time() - start_time
    
    # Save pool with metadata
    output_data = {
        "metadata": {
            "generated_at": time.strftime('%Y-%m-%d %H:%M:%S'),
            "generation_time_seconds": round(generation_time, 2),
            "total_queries": stats['total_generated'],
            "duplicates_removed": stats['total_duplicates'],
            "generator_version": "enhanced_v2.0_with_normalization"
        },
        "queries": pool
    }
    
    print(f"\n💾 Saving pool to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    
    # Save detailed statistics
    stats_file = "generation_statistics.json"
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)
    
    # Print comprehensive summary
    print("\n" + "=" * 70)
    print("📊 GENERATION SUMMARY")
    print("=" * 70)
    print(f"⏱️  Generation Time: {generation_time:.2f} seconds")
    print(f"📈 Total Attempts: {stats['total_attempts']}")
    print(f"✅ Queries Generated: {stats['total_generated']}")
    print(f"🔄 Duplicates Skipped: {stats['total_duplicates']}", end="")
    if stats['total_attempts'] > 0:
        dup_rate = (stats['total_duplicates'] / stats['total_attempts']) * 100
        print(f" ({dup_rate:.1f}%)")
    else:
        print()
    print(f"❌ Failures: {stats['total_failures']}", end="")
    if stats['total_attempts'] > 0:
        fail_rate = (stats['total_failures'] / stats['total_attempts']) * 100
        print(f" ({fail_rate:.1f}%)")
    else:
        print()
    
    # Success rate
    if stats['total_attempts'] > 0:
        success_rate = (stats['total_generated'] / stats['total_attempts']) * 100
        print(f"🎯 Success Rate: {success_rate:.1f}%")
    
    # Top duplicate intents
    print("\n🔝 Top Duplicate Intents:")
    sorted_intents = sorted(stats['by_intent'].items(), 
                           key=lambda x: x[1]['duplicates'], 
                           reverse=True)[:5]
    for intent, intent_stats in sorted_intents:
        if intent_stats['duplicates'] > 0:
            print(f"  • {intent}: {intent_stats['duplicates']} duplicates")
    
    # Database coverage
    print("\n📊 Database Coverage:")
    for db in sorted(stats['by_database'].keys()):
        db_stats = stats['by_database'][db]
        print(f"  • {db}: {db_stats['generated']} queries "
              f"({db_stats['duplicates']} dups, {db_stats['failures']} fails)")
    
    print("\n" + "=" * 70)
    fail_log.close()
    print(f"✅ Generation Complete!")
    print(f"📁 Output: {OUTPUT_FILE}")
    print(f"📊 Statistics: {stats_file}")
    print(f"📋 Failure Log: generation_failures.txt")

if __name__ == "__main__":
    main()
