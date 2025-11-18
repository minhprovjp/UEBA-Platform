# build_query_library.py
#
# PHIÊN BẢN NÂNG CẤP:
#   - Thiết kế lại hoàn toàn để sử dụng LLM Text-to-SQL (duckdb-nsql/sqlcoder)
#   - Sử dụng System Prompt chứa Schema (lược đồ) theo tài liệu [cite: 31-41]
#   - Sử dụng User Prompt là các câu hỏi ngôn ngữ tự nhiên (thay vì "nhiệm vụ")
#   - Lặp lại câu hỏi để tạo sự đa dạng
#   - Vẫn giữ bộ lọc "rác" (garbage filter) và "biên dịch chéo" (transpiler) sqlglot

import ollama
import json
import os
import logging
import sys
import re
from sqlglot import transpile, errors as sqlglot_errors

# === CẤU HÌNH LOGGING (CHO PHÉP DEBUG) ===
LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - [QueryBuilder] - %(message)s")
log = logging.getLogger("QueryBuilder")

# Thêm thư mục gốc để import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from engine.config_manager import load_config
    from config import ENGINE_DIR 
except ImportError:
    log.error("Không thể import config_manager hoặc ENGINE_DIR.")
    ENGINE_DIR = os.path.dirname(os.path.abspath(__file__)) # Fallback

# === TẢI CẤU HÌNH OLLAMA TỪ engine_config.json ===
engine_config = load_config() 
llm_config = engine_config.get("llm_config", {}) 
OLLAMA_HOST = "http://100.92.147.73:11434"
OLLAMA_MODEL = "codellama:13b" # <-- Sử dụng model mới của bạn
OLLAMA_TIMEOUT = 30

# === SỐ LƯỢNG BIẾN THỂ (VARIATIONS) MONG MUỐN CHO MỖI CÂU HỎI ===
NUM_VARIATIONS_PER_QUESTION = 5

log.info(f"Connecting to Ollama host: {OLLAMA_HOST} (model: {OLLAMA_MODEL}, timeout: {OLLAMA_TIMEOUT}s)")

try:
    client = ollama.Client(host=OLLAMA_HOST, timeout=OLLAMA_TIMEOUT)
    client.list() 
    log.info("Ollama connection successful.")
except Exception as e:
    log.error(f"FATAL: Could not connect to Ollama at {OLLAMA_HOST}.")
    log.error(f"Details: {e}")
    sys.exit(1)

# === BƯỚC 1: ĐỊNH NGHĨA SCHEMA (LƯỢC ĐỒ) ===
# (Đây là "bản đồ" mà LLM sẽ đọc, giống hệt tài liệu [cite: 31-38])
SANDBOX_SCHEMA = """
CREATE TABLE customers (
    id INT PRIMARY KEY, 
    name VARCHAR(100), 
    email VARCHAR(100)
);
CREATE TABLE orders (
    id INT PRIMARY KEY, 
    customer_id INT, 
    status VARCHAR(50)
);
CREATE TABLE products (
    sku VARCHAR(50) PRIMARY KEY, 
    name VARCHAR(100)
);
CREATE TABLE employees (
    id INT PRIMARY KEY, 
    name VARCHAR(100), 
    position VARCHAR(100)
);
CREATE TABLE salaries (
    id INT PRIMARY KEY, 
    employee_id INT, 
    salary FLOAT
);
CREATE TABLE mysql.user (
    User VARCHAR(100),
    Host VARCHAR(100),
    Password_hash TEXT
);
"""

# === BƯỚC 2: TẠO SYSTEM PROMPT (THEO TÀI LIỆU) ===
SYSTEM_PROMPT = f"""
Here is the database schema that the MYSQL query will run on:
{SANDBOX_SCHEMA}
Respond with ONLY the raw MYSQL query. Do not add any explanations or markdown.
"""

# === BƯỚC 3: ĐỊNH NGHĨA CÁC CÂU HỎI (QUESTIONS) ===
# (Đây là các user prompt,)
PERSONA_QUESTIONS = {
    "Sales_Normal": [
        "Get the 5 most recent orders for customer %s",
        "Add a new order for customer %s with status 'Pending'",
        "Update order %s to be 'Shipped'",
        "Find the customer named 'John Smith'",
        "List 10 products",
        "Show me customer %s and all their orders"
    ],
    "HR_Normal": [
        "Find employee profile for employee %s",
        "Show the salary for employee %s",
        "Insert a new employee named 'Jane Doe' in 'Sales'",
        "Count all employees in the 'Engineering' department"
    ],
    "ITAdmin_Normal": [
        "Run ANALYZE TABLE on 'orders'",
        "Run OPTIMIZE TABLE on 'customers'",
        "Show the top 5 users from the 'mysql.user' table",
        "Show all active database processes"
    ],
    "Insider_DataLeak": [
        "Get ALL data from the 'customers' table",
        "Select everything from the 'salaries' table where 1=1",
        "Dump all 'products' to an outfile '/tmp/products.csv'",
        "Show me all employees and their salaries together in one table",
        "Select all customer names and all their emails"
    ],
    "Insider_Sabotage": [
        "Delete all records from the 'orders' table immediately",
        "Update all employee 'salaries' to 0",
        "DROP the 'customers' table now",
        "Delete the employee with id %s"
    ],
    "Insider_PrivEsc": [
        "Create a new user named 'shadow_admin' with password '123'",
        "GRANT all privileges on all tables to 'shadow_admin'",
        "Create a function 'sys_exec' from 'malicious.so'",
        "Show all grants for my current user"
    ],
    "Insider_DOS": [
        "Run a query that sleeps for 5 seconds",
        "Run a query that sleeps for 10 seconds",
        "Run BENCHMARK 5000000 times on MD5('test')",
        "Show the cross join of 'orders' and 'customers'"
    ]
}

# === BƯỚC 4: BỘ LỌC "RÁC" (VẪN CẦN THIẾT) ===
REGEX_TO_EXTRACT_SQL = re.compile(
    r"(SELECT .*?;|INSERT .*?;|UPDATE .*?;|DELETE .*?;|CREATE .*?;|DROP .*?;|GRANT .*?;|ANALYZE .*?;|OPTIMIZE .*?;|SET .*?;|BENCHMARK\(.*?\);|SLEEP\(.*?\);|SHOW .*?;)",
    re.IGNORECASE | re.DOTALL | re.MULTILINE
)

def generate_queries(persona_key, questions):
    log.info(f"Generating queries for: {persona_key}...")
    all_queries = set() # Dùng SET để tự động loại bỏ các truy vấn trùng lặp
    
    # Lặp qua TỪNG CÂU HỎI
    for question_prompt in questions:
        log.info(f"  > Processing question: '{question_prompt[:40]}...'")
        
        # === NÂNG CẤP: LẶP LẠI CÂU HỎI ĐỂ TẠO BIẾN THỂ ===
        for i in range(NUM_VARIATIONS_PER_QUESTION):
            try:
                response = client.chat(
                    model=OLLAMA_MODEL, 
                    messages=[
                        {'role': 'system', 'content': SYSTEM_PROMPT},
                        {'role': 'user', 'content': question_prompt} # Prompt chỉ là câu hỏi
                    ],
                    # Thêm 'options' để tăng tính ngẫu nhiên (diversity)
                    options={
                        "temperature": 0.5 + (i * 0.1) # Tăng nhiệt độ mỗi lần lặp
                    }
                )
                raw_text = response['message']['content']
                log.debug(f"Ollama raw response for task '{question_prompt[:30]}...':\n---\n{raw_text}\n---")

                # Sử dụng Regex để trích xuất SQL sạch từ "rác"
                clean_queries = REGEX_TO_EXTRACT_SQL.findall(raw_text)
                
                if not clean_queries:
                    log.warning(f"    > LLM returned no valid SQL for question: '{question_prompt[:30]}...' (Attempt {i+1})")
                    log.debug(f"    > Raw response was: {raw_text}")
                    continue

                # Biên dịch chéo (Transpile) sang MySQL
                for q in clean_queries:
                    try:
                        # Đọc (read) là "duckdb" (hoặc "postgres")
                        # Viết (write) là "mysql" (cái chúng ta cần)
                        transpiled_q = transpile(q, read="duckdb", write="mysql")[0]
                        all_queries.add(transpiled_q) # Thêm vào SET
                    except sqlglot_errors.ParseError as pe:
                        log.warning(f"SQLglot parse error for query: '{q}'. Error: {pe}. DISCARDING.")
                    except Exception as e:
                        log.warning(f"Unexpected transpile error: {e}. DISCARDING query: '{q}'")

            except Exception as e:
                # (Bao gồm cả lỗi TimeoutError)
                log.error(f"LLM call failed for question '{question_prompt[:30]}...': {e}. Skipping this attempt.")
            
    log.info(f"  > Generated and Transpiled {len(all_queries)} total unique queries for {persona_key}")
    return list(all_queries) # Trả về 1 list

def build_library():
    query_library = {}
    for key, tasks in PERSONA_QUESTIONS.items():
        query_library[key] = generate_queries(key, tasks) 
    
    output_path = os.path.join(ENGINE_DIR, "query_library.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(query_library, f, indent=2, ensure_ascii=False)
        
    log.info(f"\n✅ Success! MySQL-compatible query library saved to: {output_path}")

if __name__ == "__main__":
    build_library()