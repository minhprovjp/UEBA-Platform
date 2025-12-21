# engine/debug_llm_output.py
#
# MỤC ĐÍCH:
# Script này dùng để kiểm tra trực tiếp output thô từ LLM cho các prompt
# được định nghĩa trong personas.yaml. Nó giúp xác định xem vấn đề
# nằm ở LLM hay ở code xử lý của chúng ta.

import ollama
import yaml
import os
import sys
import logging

# ==============================================================================
# CONFIGURATION (Giữ nguyên config của bạn)
# ==============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [LLM_Debugger] - %(message)s")
log = logging.getLogger("LLM_Debugger")

OLLAMA_HOST = "http://100.92.147.73:11434"
OLLAMA_MODEL = "sqlcoder"
SRC_DIALECT = "postgres" 

PERSONAS_CONFIG_PATH = "engine/personas.yaml"
SYSTEM_PROMPT = f"""You are a PostgreSQL expert. Your ONLY output is raw, valid {SRC_DIALECT.upper()} queries.
- NEVER write explanations or markdown.
- Adhere strictly to the provided table schema.
- Every single line of your output must be a valid query ending with a semicolon ';'.
- If you cannot generate a query, output nothing.
- Use %s for variable values.
"""

# ==============================================================================
# DEBUG SCRIPT
# ==============================================================================
def debug_llm_prompts():
    """Tải personas, tạo prompt, gửi đến LLM và in ra output thô."""
    
    # --- Kết nối đến Ollama ---
    try:
        log.info(f"Connecting to Ollama host: {OLLAMA_HOST} (model: {OLLAMA_MODEL})")
        client = ollama.Client(host=OLLAMA_HOST)
        client.list() 
        log.info("Ollama connection successful.")
    except Exception as e:
        log.critical(f"Fatal: Could not connect to Ollama at {OLLAMA_HOST}. Details: {e}")
        sys.exit(1)

    # --- Tải file personas.yaml ---
    try:
        with open(PERSONAS_CONFIG_PATH, 'r', encoding='utf-8') as f:
            personas_config = yaml.safe_load(f)
        log.info(f"Successfully loaded persona definitions from {PERSONAS_CONFIG_PATH}")
    except Exception as e:
        log.critical(f"Fatal: Could not read or parse '{PERSONAS_CONFIG_PATH}': {e}")
        sys.exit(1)

    # --- Lặp qua từng task và kiểm tra ---
    for persona_name, config in personas_config.items():
        schema = config.get("schema", "")
        tasks = config.get("tasks", [])
        
        print("\n" + "="*80)
        print(f"--- TESTING PERSONA: {persona_name} ---")
        print("="*80)
        
        if not tasks:
            log.warning("No tasks found for this persona.")
            continue

        for i, task in enumerate(tasks):
            prompt = f"Schema:\n{schema}\n\nTask: \"{task}\"\n\nGenerate 5 {SRC_DIALECT.upper()} query variations for this task."

            print(f"\n--- Task {i+1}/{len(tasks)}: {task[:60]}... ---")
            
            try:
                response = client.chat(
                    model=OLLAMA_MODEL, 
                    messages=[{'role': 'system', 'content': SYSTEM_PROMPT}, {'role': 'user', 'content': prompt}]
                )
                raw_text = response['message']['content']
                
                print("\n>>> RAW LLM OUTPUT START <<<\n")
                print(raw_text)
                print("\n>>> RAW LLM OUTPUT END <<<\n")

            except Exception as e:
                log.error(f"LLM call failed for this task: {e}")


if __name__ == "__main__":
    debug_llm_prompts()