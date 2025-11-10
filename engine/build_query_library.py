# build_query_library.py (VERSION 2.0 - Schema-Aware & Config-Driven)
import ollama
import json
import os
import logging
import sys
import yaml
import random
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [QueryBuilder] - %(message)s")
log = logging.getLogger("QueryBuilder")

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Thêm thư mục gốc để import config_manager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from config_manager import load_config
except ImportError:
    log.error("Không thể import config_manager. Đảm bảo file tồn tại.")
    sys.exit(1)

# Tải cấu hình LLM
engine_config = load_config()
llm_config = engine_config.get("llm_config", {})
OLLAMA_HOST = llm_config.get("ollama_host", "http://192.168.2.239:11434")
OLLAMA_MODEL = llm_config.get("ollama_model", "sqlcoder")

PERSONAS_CONFIG_PATH = "engine/personas.yaml"
OUTPUT_PATH = "engine/query_library.json"
NUM_VARIATIONS = 5 # Số lượng biến thể cho mỗi tác vụ

SYSTEM_PROMPT = """
You are an expert MySQL assistant. You ONLY respond with raw, valid MySQL queries based on the provided schema and task.
- Do NOT add any explanations, comments, or markdown formatting like ```sql.
- Each query must be on a new line.
- Each query must end with a semicolon ';'.
- Use %s for any variable parameter values as requested.
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
# HELPER FUNCTIONS
# ==============================================================================

def _clean_and_parse_sql(raw_text: str) -> List[str]:
    """Cleans the raw output from the LLM and parses it into a list of SQL queries."""
    # Remove markdown code blocks
    cleaned_text = raw_text.replace('```sql', '').replace('```', '')
    
    # Split by semicolon and clean up each query
    queries = [q.strip() for q in cleaned_text.split(';') if q.strip()]
    
    # Further filter to remove any potential non-query lines
    valid_queries = [q for q in queries if q.upper().startswith(('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'TRUNCATE', 'GRANT', 'CREATE', 'SHOW', 'SET', 'ANALYZE', 'OPTIMIZE', 'CHECK', 'TRUNCATE', 'DO', 'BENCHMARK'))]
    return valid_queries

def generate_queries_for_persona(persona_name: str, schema: str, tasks: List[str]) -> List[str]:
    """Generates a list of SQL queries for a given persona, schema, and tasks."""
    log.info(f"Generating queries for persona: {persona_name}...")
    all_queries = []
    
    for task in tasks:
        prompt = f"""
        Database Schema:
        {schema}

        Task for a '{persona_name}' user: "{task}"

        Based on the schema, write {NUM_VARIATIONS} diverse variations of MySQL queries to accomplish this task.
        - Occasionally use mixed case (e.g., 'SeLeCt') or add simple comments ('/* check */').
        - Use `SELECT *` sometimes.
        - Use %s for any variable parameter values if the task requires it.
        """
        
        try:
            response = client.chat(
                model=OLLAMA_MODEL, 
                messages=[
                    {'role': 'system', 'content': SYSTEM_PROMPT},
                    {'role': 'user', 'content': prompt}
                ]
            )
            raw_text = response['message']['content']
            queries = _clean_and_parse_sql(raw_text)
            
            if queries:
                all_queries.extend(queries)
                log.info(f"  > Generated {len(queries)} valid queries for task: '{task[:40]}...'")
            else:
                log.warning(f"  > No valid queries generated for task: '{task[:40]}...'")
                
        except Exception as e:
            log.error(f"  > LLM call failed for task '{task[:40]}...': {e}")
            
    return all_queries

# ==============================================================================
# MAIN BUILDER LOGIC
# ==============================================================================

def build_library():
    """Loads persona definitions, generates queries, and saves the library."""
    try:
        with open(PERSONAS_CONFIG_PATH, 'r', encoding='utf-8') as f:
            personas_config = yaml.safe_load(f)
        log.info(f"Successfully loaded persona definitions from {PERSONAS_CONFIG_PATH}")
    except FileNotFoundError:
        log.critical(f"Fatal: Persona configuration file not found at '{PERSONAS_CONFIG_PATH}'")
        sys.exit(1)
    except Exception as e:
        log.critical(f"Fatal: Error reading or parsing YAML file: {e}")
        sys.exit(1)

    query_library: Dict[str, List[str]] = {}
    
    for persona_name, config in personas_config.items():
        schema = config.get("schema", "# No schema provided.")
        tasks = config.get("tasks", [])
        
        if not tasks:
            log.warning(f"Persona '{persona_name}' has no tasks defined. Skipping.")
            continue
            
        generated_queries = generate_queries_for_persona(persona_name, schema, tasks)
        
        if generated_queries:
            query_library[persona_name] = generated_queries
            # Log some examples
            sample_size = min(3, len(generated_queries))
            log.info(f"  > Example queries for {persona_name}: {random.sample(generated_queries, sample_size)}")
        else:
            query_library[persona_name] = []
            log.warning(f"No queries were ultimately generated for persona: {persona_name}")

    # --- Save the library ---
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(query_library, f, indent=2, ensure_ascii=False)
        
    log.info("\n" + "="*50)
    log.info("QUERY LIBRARY GENERATION SUMMARY")
    log.info("="*50)
    for persona, queries in query_library.items():
        log.info(f"{persona:<20} | {len(queries):>5} queries generated")
    log.info("="*50)
    log.info(f"\n✅ Success! Query library saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    build_library()