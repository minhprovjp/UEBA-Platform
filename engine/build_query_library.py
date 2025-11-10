# build_query_library.py (VERSION 3.0 - Self-Validating)
import ollama
import json
import os
import logging
import sys
import yaml
import random
import mysql.connector
from typing import List, Dict

# ==============================================================================
# CONFIGURATION
# ==============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [QueryBuilder] - %(message)s")
log = logging.getLogger("QueryBuilder")

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from config_manager import load_config
except ImportError:
    log.error("Không thể import config_manager. Đảm bảo file tồn tại.")
    sys.exit(1)

engine_config = load_config()
llm_config = engine_config.get("llm_config", {})
OLLAMA_HOST = llm_config.get("ollama_host", "http://192.168.2.239:11434")
OLLAMA_MODEL = llm_config.get("ollama_model", "sqlcoder")

PERSONAS_CONFIG_PATH = "engine/personas.yaml"
OUTPUT_PATH = "engine/query_library.json"
NUM_VARIATIONS = 7

SYSTEM_PROMPT = """You are a MySQL expert. Respond ONLY with raw, valid MySQL queries based on the provided schema and task.
- NEVER add explanations, comments, or markdown formatting.
- Each query must be on a single line and end with a semicolon ';'.
- Use %s for variable values.
"""

# ==============================================================================
# OLLAMA CLIENT INITIALIZATION
# ==============================================================================
try:
    log.info(f"Connecting to Ollama host: {OLLAMA_HOST} (model: {OLLAMA_MODEL})")
    client = ollama.Client(host=OLLAMA_HOST)
    client.list() 
    log.info("Ollama connection successful.")
except Exception as e:
    log.critical(f"Fatal: Could not connect to Ollama at {OLLAMA_HOST}.")
    log.critical(f"Please ensure Ollama is running and accessible.")
    log.critical(f"Error details: {e}")
    sys.exit(1)

# ==============================================================================
# SQL VALIDATOR CLASS
# ==============================================================================
class SQLValidator:
    """Connects to the DB to validate generated SQL queries."""
    def __init__(self):
        self.conn = None
        try:
            self.conn = mysql.connector.connect(
                host=os.getenv("SANDBOX_DB_HOST", "localhost"),
                port=os.getenv("SANDBOX_DB_PORT", "3306"),
                user="root",
                password="root"
            )
            log.info("SQL Validator connected to MySQL successfully.")
        except mysql.connector.Error as err:
            log.critical(f"SQL Validator failed to connect: {err}")
            log.critical("Please provide root credentials in your .env file (MYSQL_ROOT_PASSWORD).")
            sys.exit(1)

    def is_valid(self, query: str, db_context: str) -> bool:
        """Checks if a query is syntactically valid by running EXPLAIN or a transaction."""
        if not self.conn:
            return False
        
        cursor = self.conn.cursor()
        is_valid = False
        
        # Replace python placeholders with dummy values for validation
        test_query = query.replace("%s", "1")
        
        try:
            cursor.execute(f"USE {db_context};")
            
            # Use EXPLAIN for SELECT/SHOW, Transaction for others
            if test_query.strip().upper().startswith(('SELECT', 'SHOW')):
                cursor.execute(f"EXPLAIN {test_query}")
            else:
                cursor.execute("START TRANSACTION;")
                cursor.execute(test_query)
                cursor.execute("ROLLBACK;")
            is_valid = True
        except mysql.connector.Error:
            # Any error during this process means the query is invalid
            is_valid = False
        finally:
            cursor.close()
            
        return is_valid

    def close(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()
            log.info("SQL Validator disconnected.")

# ==============================================================================
# MAIN SCRIPT LOGIC
# ==============================================================================
def _clean_and_parse_sql(raw_text: str) -> List[str]:
    cleaned_text = raw_text.replace('```sql', '').replace('```', '')
    queries = [q.strip() for q in cleaned_text.split(';') if q.strip()]
    return queries

def generate_and_validate_queries(validator: SQLValidator, persona_name: str, config: Dict) -> List[str]:
    log.info(f"--- Generating queries for persona: {persona_name} ---")
    schema = config.get("schema", "# No schema provided.")
    db_context = config.get("database", "mysql")
    tasks = config.get("tasks", [])
    
    validated_queries = []
    for task in tasks:
        prompt = f"Schema:\n{schema}\n\nTask for a '{persona_name}': \"{task}\"\n\nWrite {NUM_VARIATIONS} MySQL query variations for this task."
        
        try:
            response = client.chat(
                model=OLLAMA_MODEL, 
                messages=[{'role': 'system', 'content': SYSTEM_PROMPT}, {'role': 'user', 'content': prompt}]
            )
            raw_text = response['message']['content']
            candidate_queries = _clean_and_parse_sql(raw_text)
            
            if not candidate_queries:
                log.warning(f"  > Task '{task[:40]}...': LLM returned no queries.")
                continue

            log.info(f"  > Task '{task[:40]}...': Generated {len(candidate_queries)} candidates. Now validating...")
            for query in candidate_queries:
                if validator.is_valid(query, db_context):
                    validated_queries.append(query)
                    log.info(f"    [ACCEPTED] {query}")
                else:
                    log.warning(f"    [REJECTED] {query}")
        except Exception as e:
            log.error(f"  > LLM call failed for task '{task[:40]}...': {e}")
            
    return validated_queries

def build_library():
    try:
        with open(PERSONAS_CONFIG_PATH, 'r', encoding='utf-8') as f:
            personas_config = yaml.safe_load(f)
        log.info(f"Successfully loaded persona definitions from {PERSONAS_CONFIG_PATH}")
    except Exception as e:
        log.critical(f"Fatal: Could not read or parse '{PERSONAS_CONFIG_PATH}': {e}")
        sys.exit(1)

    validator = SQLValidator()
    if not validator.conn:
        return # Stop if validator failed to connect

    query_library: Dict[str, List[str]] = {}
    
    for persona_name, config in personas_config.items():
        query_library[persona_name] = generate_and_validate_queries(validator, persona_name, config)

    validator.close()

    # --- Save and Summarize ---
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(query_library, f, indent=2, ensure_ascii=False)
        
    log.info("\n" + "="*50)
    log.info("QUERY LIBRARY GENERATION SUMMARY")
    log.info("="*50)
    total_queries = 0
    for persona, queries in query_library.items():
        count = len(queries)
        total_queries += count
        log.info(f"{persona:<20} | {count:>5} queries validated")
    log.info("-"*50)
    log.info(f"{'TOTAL':<20} | {total_queries:>5} queries validated")
    log.info("="*50)
    log.info(f"\n✅ Success! High-quality query library saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    build_library()