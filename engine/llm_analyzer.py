# engine/llm_analyzer.py
import ollama
import json
import logging
import re
import time
from urllib.parse import urlparse
from typing import Optional, Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [LLM_UBA] - %(message)s')
logger = logging.getLogger(__name__)

def clean_json_text(text: str) -> str:
    """Làm sạch phản hồi từ LLM để lấy JSON chuẩn."""
    if not text: return "{}"
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL) # Bỏ suy nghĩ của DeepSeek
    match = re.search(r'```json\s*(.*?)\s*```', text, flags=re.DOTALL)
    if match: return match.group(1)
    match = re.search(r'```\s*(.*?)\s*```', text, flags=re.DOTALL)
    if match: return match.group(1)
    return text.strip()

class OllamaProvider:
    def __init__(self, host: str, timeout: int = 30):
        self.host = host
        self.timeout = timeout
        self.client = None
        self._connect()
    
    def _connect(self):
        try:
            self.client = ollama.Client(host=self.host, timeout=self.timeout)
        except Exception as e:
            logger.error(f"Ollama connect failed: {e}")

    def analyze(self, prompt: str, model: str, keep_alive: str = "30m") -> Dict[str, Any]:
        if not self.client: self._connect()
        try:
            # Gọi model với keep_alive từ config để giữ context trong RAM
            response = self.client.chat(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                format="json",
                keep_alive=keep_alive 
            )
            return response
        except Exception as e:
            logger.error(f"Ollama Analysis Error: {e}")
            raise e

def analyze_query_with_llm(
    anomaly_row: Dict[str, Any], 
    anomaly_type_from_system: str,
    llm_config: Dict[str, Any],
    rules_config: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    
    # 1. Cấu hình - Lấy từ config file, không hardcode fallback IP
    host = llm_config.get('ollama_host')
    model_name = llm_config.get('ollama_model')
    timeout = llm_config.get('ollama_timeout', 360)
    keep_alive_duration = llm_config.get('keep_alive', '30m')

    if not host or not model_name:
        logger.error("Missing Ollama host or model in configuration.")
        return None

    provider = OllamaProvider(host=host, timeout=timeout)

    # 2. CHUẨN BỊ DỮ LIỆU INPUT (ENRICHED LOG)
    # Thay vì chỉ gửi câu query, ta gửi toàn bộ ngữ cảnh kỹ thuật số
    # để model có thể check các luật như CPU, Time, Entropy...
    
    rich_context = {
        "identity": {
            "user": anomaly_row.get('user'),
            "ip": anomaly_row.get('client_ip'),
            "program": anomaly_row.get('program_name', 'unknown'), # Cần cho Rule 17
            "client_os": anomaly_row.get('client_os', 'unknown')   # Cần cho Rule 17
        },
        "timing": {
            "timestamp": str(anomaly_row.get('timestamp')),
            "is_late_night": bool(anomaly_row.get('is_late_night', False)), # Cần cho Rule 7
        },
        "query_content": {
            "sql": anomaly_row.get('query'),
            "command_type": anomaly_row.get('command_type'),
            "entropy": float(anomaly_row.get('query_entropy', 0)), # Cần cho Rule 16
        },
        "performance_metrics": {
            "execution_time_ms": float(anomaly_row.get('execution_time_ms', 0)), # Cần cho Rule 12
            "cpu_time_ms": float(anomaly_row.get('cpu_time_ms', 0)),             # Cần cho Rule 13 (quan trọng)
            "lock_time_ms": float(anomaly_row.get('lock_time_ms', 0)),           # Cần cho Rule 20
            "rows_returned": int(anomaly_row.get('rows_returned', 0)),           # Cần cho Rule 27 (Dump)
            "rows_examined": int(anomaly_row.get('rows_examined', 0)),
            "scan_efficiency": float(anomaly_row.get('scan_efficiency', 0))      # Cần cho Rule 14
        },
        "security_flags": {
            "is_admin_command": bool(anomaly_row.get('is_admin_command')),
            "is_risky_command": bool(anomaly_row.get('is_risky_command')),
            "has_comment": bool(anomaly_row.get('has_comment')),
            "error_count_5m": float(anomaly_row.get('error_count_5m', 0))
        },
        "system_detection": {
            "initial_flag": anomaly_type_from_system,
            "ml_score": float(anomaly_row.get('ml_anomaly_score', 0))
        }
    }

    # 3. TẠO PROMPT "SIÊU NGẮN" (Vì model đã nhớ luật)
    # Chúng ta chỉ cần đưa dữ liệu JSON vào và bảo model "Analyze this"
    
    prompt = f"""
    Analyze the following database transaction log against your internal Security Rules (Groups 1-5).
    
    INPUT LOG (JSON):
    ```json
    {json.dumps(rich_context, indent=2)}
    ```
    
    Task: Identify specific rule violations based on the metrics provided (e.g., check if cpu_time > 1000ms for Rule 13, or if entropy > 4.8 for Rule 16).
    """

    try:
        logger.info(f"Sending log to {model_name}...")
        start_t = time.time()
        
        # Gọi LLM (Sử dụng tham số từ config)
        res = provider.analyze(prompt, model=model_name, keep_alive=keep_alive_duration)
        
        duration = time.time() - start_t
        clean_text = clean_json_text(res['message']['content'])
        analysis_result = json.loads(clean_text)
        
        logger.info(f"Analysis done in {duration:.2f}s")

        return {
            "final_analysis": analysis_result,
            "providers_used": [model_name],
            "analysis_timestamp": time.time()
        }

    except Exception as e:
        logger.error(f"LLM Analysis Failed: {e}")
        return None