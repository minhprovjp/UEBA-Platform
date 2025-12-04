# llm_analyzer_dual.py
"""
================================================================================
MODULE PHÂN TÍCH BẰNG MÔ HÌNH NGÔN NGỮ LỚN (LLM) - DUAL PROVIDER SYSTEM
================================================================================
Module này chứa tất cả logic để tương tác với cả Local LLM (Ollama) và Third-party AI APIs.
Nó chịu trách nhiệm cho việc:
1.  Tạo kết nối đến Ollama server hoặc Third-party API.
2.  Xây dựng các câu lệnh prompt chi tiết, giàu ngữ cảnh.
3.  Gửi yêu cầu phân tích và nhận về kết quả dưới dạng JSON.
4.  Intelligent fallback giữa các providers.
"""

# Import các thư viện cần thiết
import ollama          # Thư viện chính để giao tiếp với Ollama
import pandas as pd    # Dùng để làm việc với đối tượng Series
import json            # Dùng để làm việc với định dạng dữ liệu JSON
from typing import Optional, Dict, Any, List, Union # Dùng cho type hinting
import logging
import os
import sys
import time
import requests
from urllib.parse import urlparse
from abc import ABC, abstractmethod

# Thêm thư mục gốc vào sys.path để có thể import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from engine.utils import extract_query_features

# Cấu hình logging cho module này
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [LLM_Analyzer_Dual] - %(message)s')
logger = logging.getLogger(__name__)

class LLMProvider(ABC):
    """Abstract base class for LLM providers"""
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if provider is available"""
        pass
    
    @abstractmethod
    def analyze(self, prompt: str, **kwargs) -> Dict[str, Any]:
        """Perform analysis with the provider"""
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """Get provider name"""
        pass

class OllamaProvider(LLMProvider):
    """Ollama local LLM provider"""
    
    def __init__(self, host: str = "http://100.92.147.73:11434", timeout: int = 3600):
        self.host = host
        self.timeout = timeout
        self.client = None
        self._connect()
    
    def _connect(self):
        """Establish connection to Ollama server"""
        try:
            # Validate host URL
            parsed_url = urlparse(self.host)
            if not parsed_url.scheme or not parsed_url.netloc:
                raise ValueError(f"Invalid host URL: {self.host}")
            
            logger.info(f"Attempting to connect to Ollama server at {self.host}")
            self.client = ollama.Client(host=self.host, timeout=self.timeout)
            
            # Test the connection immediately
            try:
                # Simple ping test
                response = self.client.list()
                logger.info(f"Successfully connected to Ollama server at {self.host}")
                logger.debug(f"Initial connection test response: {type(response)} - {response}")
            except Exception as test_error:
                logger.warning(f"Connection established but test failed: {test_error}")
                # Don't fail the connection, just log the warning
                
        except Exception as e:
            logger.error(f"Failed to connect to Ollama server at {self.host}: {e}")
            self.client = None
    
    def is_available(self) -> bool:
        """Check if Ollama is available"""
        try:
            if not self.client:
                return False
            
            # Try to list models to test connection
            models = self.client.list()
            
            # Handle different Ollama API response formats
            if 'models' in models:
                # New format: models['models'] is a list
                model_names = []
                for m in models['models']:
                    if isinstance(m, dict) and 'name' in m:
                        model_names.append(m['name'])
                    elif hasattr(m, 'name'):
                        model_names.append(m.name)
                    else:
                        # Fallback: try to get string representation
                        model_names.append(str(m))
                
                logger.info(f"Ollama connection test successful. Available models: {model_names}")
            else:
                # Alternative format: models might be a list directly
                model_names = []
                for m in models:
                    if isinstance(m, dict) and 'name' in m:
                        model_names.append(m['name'])
                    elif hasattr(m, 'name'):
                        model_names.append(m.name)
                    else:
                        model_names.append(str(m))
                
                logger.info(f"Ollama connection test successful. Available models: {model_names}")
            
            return True
        except Exception as e:
            logger.error(f"Ollama connection test failed: {e}")
            # Log the actual response structure for debugging
            try:
                if 'models' in models:
                    logger.debug(f"Models response structure: {type(models['models'])} - {models['models']}")
                else:
                    logger.debug(f"Models response structure: {type(models)} - {models}")
            except:
                pass
            
            # Try fallback health check
            logger.info("Attempting fallback health check...")
            health_status = self.health_check()
            if health_status.get("status") == "healthy":
                logger.info("Fallback health check successful - Ollama is available")
                return True
            else:
                logger.warning(f"Fallback health check failed: {health_status}")
                return False
    
    def health_check(self) -> Dict[str, Any]:
        """Simple health check without trying to list models"""
        try:
            if not self.client:
                return {"status": "disconnected", "error": "Client not initialized"}
            
            # Try a simple ping-like operation
            try:
                # Use the tags endpoint which is lighter than listing models
                response = requests.get(f"{self.host}/api/tags", timeout=5)
                if response.status_code == 200:
                    return {"status": "healthy", "endpoint": f"{self.host}/api/tags"}
                else:
                    return {"status": "unhealthy", "endpoint": f"{self.host}/api/tags", "status_code": response.status_code}
            except Exception as ping_error:
                return {"status": "unhealthy", "error": str(ping_error)}
                
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def analyze(self, prompt: str, model: str = "qwen2.5-coder:32b", **kwargs) -> Dict[str, Any]:
        """Send analysis request to Ollama"""
        if not self.client:
            raise ConnectionError("Ollama client not connected")
        
        max_retries = kwargs.get('max_retries', 3)
        retry_delay = kwargs.get('retry_delay', 2)
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Sending request to Ollama model {model} (attempt {attempt + 1})")
                response = self.client.chat(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    format="json"
                )
                logger.info(f"Successfully received response from Ollama model {model}")
                return response
            except Exception as e:
                logger.warning(f"Ollama attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"All {max_retries} Ollama attempts failed")
                    raise e
    
    def get_name(self) -> str:
        return "Ollama"

class OpenAIProvider(LLMProvider):
    """OpenAI API provider"""
    
    def __init__(self, api_key: str, model: str = "gpt-3.5-turbo", base_url: str = "https://api.openai.com/v1"):
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def is_available(self) -> bool:
        """Check if OpenAI API is available"""
        try:
            response = requests.get(f"{self.base_url}/models", headers=self.headers, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"OpenAI API availability check failed: {e}")
            return False
    
    def analyze(self, prompt: str, **kwargs) -> Dict[str, Any]:
        """Send analysis request to OpenAI"""
        try:
            model = kwargs.get('model', self.model)
            temperature = kwargs.get('temperature', 0.7)
            max_tokens = kwargs.get('max_tokens', 4096)
            
            payload = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": temperature,
                "max_tokens": max_tokens,
                "response_format": {"type": "json_object"}
            }
            
            logger.info(f"Sending request to OpenAI model {model}")
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=self.headers,
                json=payload,
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info("Successfully received response from OpenAI")
                return {
                    "message": {
                        "content": result["choices"][0]["message"]["content"]
                    }
                }
            else:
                raise Exception(f"OpenAI API error: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"OpenAI analysis failed: {e}")
            raise e
    
    def get_name(self) -> str:
        return "OpenAI"

class AnthropicProvider(LLMProvider):
    """Anthropic Claude API provider"""
    
    def __init__(self, api_key: str, model: str = "claude-3-sonnet-20240229"):
        self.api_key = api_key
        self.model = model
        self.headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01"
        }
    
    def is_available(self) -> bool:
        """Check if Anthropic API is available"""
        try:
            response = requests.get("https://api.anthropic.com/v1/models", headers=self.headers, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Anthropic API availability check failed: {e}")
            return False
    
    def analyze(self, prompt: str, **kwargs) -> Dict[str, Any]:
        """Send analysis request to Anthropic"""
        try:
            model = kwargs.get('model', self.model)
            max_tokens = kwargs.get('max_tokens', 4096)
            
            payload = {
                "model": model,
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
                "system": "You are a security analyst. Respond only with valid JSON."
            }
            
            logger.info(f"Sending request to Anthropic model {model}")
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=self.headers,
                json=payload,
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info("Successfully received response from Anthropic")
                return {
                    "message": {
                        "content": result["content"][0]["text"]
                    }
                }
            else:
                raise Exception(f"Anthropic API error: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Anthropic analysis failed: {e}")
            raise e
    
    def get_name(self) -> str:
        return "Anthropic"

class LLMManager:
    """Manager class for handling multiple LLM providers with fallback logic"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.providers: List[LLMProvider] = []
        self._initialize_providers()
    
    def _initialize_providers(self):
        """Initialize available LLM providers based on configuration"""
        llm_config = self.config.get('llm_config', {})
        
        # Initialize Ollama provider
        if llm_config.get('enable_ollama', True):
            try:
                ollama_provider = OllamaProvider(
                    host=llm_config.get('ollama_host', 'http://100.92.147.73:11434'),
                    timeout=llm_config.get('ollama_timeout', 3600)
                )
                self.providers.append(ollama_provider)
                logger.info("Ollama provider initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize Ollama provider: {e}")
        
        # Initialize OpenAI provider
        if llm_config.get('enable_openai', False) and llm_config.get('openai_api_key'):
            try:
                openai_provider = OpenAIProvider(
                    api_key=llm_config.get('openai_api_key'),
                    model=llm_config.get('openai_model', 'gpt-3.5-turbo'),
                    base_url=llm_config.get('openai_base_url', 'https://api.openai.com/v1')
                )
                self.providers.append(openai_provider)
                logger.info("OpenAI provider initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize OpenAI provider: {e}")
        
        # Initialize Anthropic provider
        if llm_config.get('enable_anthropic', False) and llm_config.get('anthropic_api_key'):
            try:
                anthropic_provider = AnthropicProvider(
                    api_key=llm_config.get('anthropic_api_key'),
                    model=llm_config.get('anthropic_model', 'claude-3-sonnet-20240229')
                )
                self.providers.append(anthropic_provider)
                logger.info("Anthropic provider initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize Anthropic provider: {e}")
        
        if not self.providers:
            logger.error("No LLM providers available!")
            raise RuntimeError("No LLM providers could be initialized")
        
        logger.info(f"Initialized {len(self.providers)} LLM providers: {[p.get_name() for p in self.providers]}")
    
    def get_available_providers(self) -> List[LLMProvider]:
        """Get list of currently available providers"""
        return [p for p in self.providers if p.is_available()]
    
    def analyze_with_fallback(self, prompt: str, **kwargs) -> Dict[str, Any]:
        """Analyze with automatic fallback between providers"""
        available_providers = self.get_available_providers()
        
        if not available_providers:
            raise RuntimeError("No LLM providers are currently available")
        
        # Try providers in order of preference
        for provider in available_providers:
            try:
                logger.info(f"Attempting analysis with {provider.get_name()}")
                result = provider.analyze(prompt, **kwargs)
                logger.info(f"Successfully analyzed with {provider.get_name()}")
                return {
                    "result": result,
                    "provider": provider.get_name(),
                    "success": True
                }
            except Exception as e:
                logger.warning(f"Analysis failed with {provider.get_name()}: {e}")
                continue
        
        # If all providers failed
        raise RuntimeError(f"All LLM providers failed: {[p.get_name() for p in available_providers]}")

def analyze_query_with_llm(
    anomaly_row: Dict[str, Any], 
    anomaly_type_from_system: str,
    llm_config: Dict[str, Any],
    rules_config: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Gửi một câu truy vấn (query) đến LLM với dual provider system:
    - Lần 1: Phân tích sơ bộ
    - Lần 2: Gửi lại kết quả lần 1 để LLM phân tích sâu hơn
    """
    
    logger.info("Initializing dual-provider LLM analysis")
    
    try:
        # Initialize LLM manager
        config = {
            "llm_config": llm_config
        }
        llm_manager = LLMManager(config)
        
        # Check available providers
        available_providers = llm_manager.get_available_providers()
        if not available_providers:
            raise RuntimeError("No LLM providers are available")
        
        logger.info(f"Available providers: {[p.get_name() for p in available_providers]}")
        
    except Exception as e:
        logger.error(f"Failed to initialize LLM manager: {e}")
        raise ConnectionError(f"Could not initialize LLM providers: {e}")
    
    query_to_analyze = anomaly_row.get('query', '')
    if not query_to_analyze:
        # Xử lý trường hợp query rỗng một cách an toàn
        return {
            "query_analyzed": "Empty Query", "is_anomalous": False, "confidence_score": 1.0,
            "anomaly_type": "Logic Error", "security_risk_level": "None", "performance_impact": "None",
            "summary": "Câu lệnh SQL đầu vào bị rỗng.",
            "detailed_analysis": "Không thể phân tích một chuỗi SQL rỗng. Đây có thể là một lỗi trong quá trình ghi log.",
            "recommendation": "Kiểm tra nguồn log để xác định tại sao một câu lệnh rỗng được ghi lại.",
            "tags": ["EMPTY_QUERY"]
        }
    
    # --- BƯỚC 1: TỔNG HỢP NGỮ CẢNH PHONG PHÚ ---
    
    # "Làm sạch" query để đảm bảo nó không phá vỡ cấu trúc JSON của prompt
    sanitized_query = query_to_analyze.replace('"', '\\"').replace('\n', ' ')

    # --- SỬA LỖI: Chuyển đổi tường minh các kiểu dữ liệu sang kiểu gốc của Python ---
    def get_as_native_type(value, default_val, target_type):
        """Hàm helper để lấy giá trị và chuyển đổi sang kiểu gốc, xử lý NaN."""
        if pd.isna(value):
            return default_val
        try:
            return target_type(value)
        except (ValueError, TypeError):
            return default_val

    # Xây dựng một dictionary chứa hồ sơ kỹ thuật của câu query
    # Sử dụng hàm helper để đảm bảo tất cả đều là kiểu Python gốc
    structural_profile = {
        "num_joins": get_as_native_type(anomaly_row.get('num_joins'), 0, int),
        "num_where_conditions": get_as_native_type(anomaly_row.get('num_where_conditions'), 0, int),
        "has_subquery": get_as_native_type(anomaly_row.get('has_subquery'), False, bool),
        "has_union": get_as_native_type(anomaly_row.get('has_union'), False, bool),
        "has_limit": get_as_native_type(anomaly_row.get('has_limit'), False, bool),
    }

    # Lấy cấu hình rule từ tham số với defaults
    sensitive_tables_list = rules_config.get('p_sensitive_tables', [])
    allowed_users_list = rules_config.get('p_allowed_users_sensitive', [])
    safe_start = rules_config.get('p_safe_hours_start', 8)
    safe_end = rules_config.get('p_safe_hours_end', 18)
    large_tables_list = rules_config.get('p_known_large_tables', [])
    time_window = rules_config.get('p_time_window_minutes', 5)
    distinct_tables_threshold = rules_config.get('p_min_distinct_tables', 3)

    # --- BƯỚC 2: TẠO PROMPT NÂNG CAO ---
    
    # --------------------- PROMPT LẦN 1 ---------------------
    prompt_round_1 = f"""
# ROLE AND GOAL
You are a world-class Senior Database Administrator (DBA) and a Cybersecurity Analyst, specialized in MySQL performance and security. Your primary goal is to conduct a forensic analysis of a given SQL query that has been flagged by an automated monitoring system. You must provide a comprehensive, structured analysis in JSON format ONLY.

# [SECURITY & COMPLIANCE POLICY]
Our system operates under the following dynamically configured security policies. Use these rules to inform your analysis:
- **Sensitive Tables:** Access to these tables is strictly monitored: `{", ".join(sensitive_tables_list)}`.
- **Authorized Users for Sensitive Data:** Only these users may access sensitive tables during safe hours: `{", ".join(allowed_users_list)}`. Any other user is a high-severity violation.
- **Safe Operating Hours:** Standard business hours are from {safe_start:02d}:00 to {safe_end:02d}:00, Monday to Friday.
- **Data Exfiltration:** Any query using `SELECT *` on a large table (e.g., `{", ".join(large_tables_list)}`) without a `WHERE` or `LIMIT` is a potential data dump.
- **Suspicious Scanning:** Accessing {distinct_tables_threshold} or more distinct tables within a {time_window}-minute window is considered suspicious.

# [CONTEXT OF THE INCIDENT]
The monitoring system provides you with the following context:
- **Initial Finding:** The system flagged this as a potential '{anomaly_type_from_system}'.
- **User Information:**
  - User: "{anomaly_row.get('user', 'N/A')}"
  - Source IP: "{anomaly_row.get('client_ip', 'N/A')}"
- **Time Information:**
  - Timestamp: "{anomaly_row.get('timestamp', 'N/A')}"
- **Technical Profile of the Query:**
  {json.dumps(structural_profile, indent=2)}

# [THE QUERY TO ANALYZE]
"{sanitized_query}"

# [INSTRUCTIONS]
Based on all the provided context and the query itself, perform the following analysis:
1.  **Policy Violation Check:** Explicitly state which policies (if any) this query violates.
2.  **Security Analysis:** Look for any signs of malicious intent, such as SQL Injection patterns (e.g., '1=1', 'OR 1=1'), data exfiltration attempts (e.g., 'SELECT ... INTO OUTFILE'), or unauthorized access patterns.
3.  **Performance Analysis:** Assess the potential performance impact. Does it lack a WHERE clause on a large table? Does it use wildcards ('*') inefficiently? Could it cause a full table scan?
4.  **Behavioral Analysis:** Does this query fit the user's typical behavior profile? Is it a logical business query or does it look out of place?
5.  **Final Output:** Synthesize your findings into the JSON structure below. Be concise but thorough. Do not add any text or explanation outside the JSON object.

# [JSON OUTPUT STRUCTURE]
{{
  "query_analyzed": "{sanitized_query}",
  "is_anomalous": boolean,
  "confidence_score": float,
  "anomaly_type": "string (e.g., 'Security Vulnerability', 'Performance Issue', 'Suspicious Behavior', 'Normal Operation')",
  "security_risk_level": "string ('None', 'Low', 'Medium', 'High', 'Critical')",
  "performance_impact": "string ('None', 'Low', 'Medium', 'High')",
  "summary": "A one-sentence, non-technical summary of your finding.",
  "detailed_analysis": "A detailed, technical explanation of WHY you reached your conclusion, referencing the context provided and the query structure.",
  "recommendation": "A clear, actionable next step for the human analyst (e.g., 'Block this query pattern immediately', 'Add an index on the 'user_id' column', 'Verify with the user's manager').",
  "tags": ["array", "of", "string", "keywords"]
}}
"""

    try:
        # Lần 1: Gửi prompt gốc
        logger.info("Sending first analysis request to LLM")
        res1 = llm_manager.analyze_with_fallback(prompt_round_1)
        first_analysis_str = res1["result"]['message']['content']
        first_analysis = json.loads(first_analysis_str)
        logger.info("First analysis completed successfully")

        # --------------------- PROMPT LẦN 2 ---------------------
        prompt_round_2 = f"""
# ROLE AND GOAL
You are now an **Expert Reviewer and Quality Assurance Specialist** for AI-driven security analysis. Your goal is to CRITIQUE and REFINE a preliminary analysis of a SQL query. You must act as a second pair of eyes to catch what the first AI might have missed. Your final output must be a refined, higher-quality JSON analysis.

# [SECURITY & COMPLIANCE POLICY]
Our system operates under the following dynamically configured security policies. Use these rules to inform your analysis:
- **Sensitive Tables:** Access to these tables is strictly monitored: `{", ".join(sensitive_tables_list)}`.
- **Authorized Users for Sensitive Data:** Only these users may access sensitive tables during safe hours: `{", ".join(allowed_users_list)}`. Any other user is a high-severity violation.
- **Safe Operating Hours:** Standard business hours are from {safe_start:02d}:00 to {safe_end:02d}:00, Monday to Friday.
- **Data Exfiltration:** Any query using `SELECT *` on a large table (e.g., `{", ".join(large_tables_list)}`) without a `WHERE` or `LIMIT` is a potential data dump.
- **Suspicious Scanning:** Accessing {distinct_tables_threshold} or more distinct tables within a {time_window}-minute window is considered suspicious.

# [ORIGINAL CONTEXT]
- **The Query:** `{sanitized_query}`
- **Initial System Flag:** '{anomaly_type_from_system}'
- **User:** "{anomaly_row.get('user', 'N/A')}" at IP "{anomaly_row.get('client_ip', 'N/A')}"
- **Technical Profile:** {json.dumps(structural_profile, indent=2)}

# [PRELIMINARY AI ANALYSIS - FOR YOUR REVIEW]
```json
{json.dumps(first_analysis, indent=2)}
```

# [INSTRUCTIONS FOR REFINEMENT]
Review the preliminary analysis critically. Your task is to produce a definitive, final analysis. Follow this thought process:
1.  **Policy Violation Check:** Explicitly state which policies (if any) this query violates.
2. **Re-evaluate is_anomalous flag:** Do you agree with the initial assessment? Correct it if it's a false positive or false negative.
3. **Challenge anomaly_type:** Is the initial classification correct? Be more specific. Could it be a combination of types (e.g., both a 'Performance Issue' and 'Suspicious Behavior')?
4. **Refine security_risk_level:** Is the risk level appropriate? A SELECT * on a public table is 'Low' risk, but on a customers table, it's 'High'.
5. **Deepen detailed_analysis:** Go beyond the obvious. Explain why it's a risk. Instead of "SQL Injection possible," say "The query contains a classic '1'='1' tautology, a textbook SQL injection pattern designed to bypass authentication."
6. **Strengthen recommendation:** Make it more specific and actionable. Instead of "Check with user," suggest "Immediately contact user '{anomaly_row.get('user', 'N/A')}' to verify business purpose. If unverified, consider temporarily disabling the account and review all recent activity from IP '{anomaly_row.get('client_ip', 'N/A')}'."
7. **Update confidence_score:** Your confidence in THIS (your final) analysis.

Final Output: Return ONLY the refined JSON object in the same, original format. Do not add any conversational text.
"""

        logger.info("Sending second analysis request to LLM")
        res2 = llm_manager.analyze_with_fallback(prompt_round_2)
        final_analysis_str = res2["result"]['message']['content']
        final_analysis = json.loads(final_analysis_str)
        logger.info("Second analysis completed successfully")

        # Nếu muốn so sánh 2 kết quả, có thể kiểm tra:
        discrepancy_warning = None
        if first_analysis != final_analysis:
            discrepancy_warning = "⚠️ Có sự khác biệt giữa lần phân tích đầu và lần phân tích sau. Hãy xem xét kỹ."
        
        return {
            "first_analysis": first_analysis,
            "final_analysis": final_analysis,
            "prompt_round_1": prompt_round_1,
            "prompt_round_2": prompt_round_2,
            "discrepancy_warning": discrepancy_warning,
            "providers_used": [res1["provider"], res2["provider"]],
            "analysis_timestamp": time.time()
        }

    except json.JSONDecodeError as e:
        logger.error(f"Lỗi: AI trả về JSON không hợp lệ. {e}")
        # Ném lỗi để tầng API có thể bắt và xử lý
        raise ValueError(f"AI response is not a valid JSON: {e}")
    except Exception as e:
        logger.error(f"Lỗi khi phân tích với LLM: {e}")
        raise ConnectionError(f"LLM analysis failed: {e}")


def analyze_session_with_llm(    
    session_anomaly_row: Dict[str, Any], 
    anomaly_type_from_system: str,
    llm_config: Dict[str, Any],
    rules_config: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Phân tích một session (một chuỗi các query) bằng cách tổng hợp các đặc trưng
    thống kê và gửi cho LLM với dual provider system.
    """
    logger.info("Initializing dual-provider session analysis")
    
    try:
        # Initialize LLM manager
        config = {
            "llm_config": llm_config
        }
        llm_manager = LLMManager(config)
        
        # Check available providers
        available_providers = llm_manager.get_available_providers()
        if not available_providers:
            raise RuntimeError("No LLM providers are available")
        
        logger.info(f"Available providers for session analysis: {[p.get_name() for p in available_providers]}")
        
    except Exception as e:
        logger.error(f"Failed to initialize LLM manager for session analysis: {e}")
        raise ConnectionError(f"Could not initialize LLM providers: {e}")
    
    # --- Bước 2: Trích xuất và tổng hợp dữ liệu từ session ---
    user = session_anomaly_row.get('user', 'N/A')
    start_time = session_anomaly_row.get('start_time', 'N/A')
    end_time = session_anomaly_row.get('end_time', 'N/A')
    distinct_tables_count = session_anomaly_row.get('distinct_tables_count', 0)
    tables_accessed_list = session_anomaly_row.get('tables_accessed_in_session', [])
    queries_details = session_anomaly_row.get('queries_details', [])
    
    if not queries_details:
        return {
            "session_summary": "Session không chứa câu query nào để phân tích.",
            "is_threatening_behavior": False,
            "confidence_score": 1.0,
            "behavior_type": "Empty Session",
            "detailed_analysis": "Không có dữ liệu query để phân tích hành vi.",
            "recommendation": "Kiểm tra nguồn log để xác định tại sao session này lại rỗng."
        }

    session_df = pd.DataFrame(queries_details)

    # --- Bước 3: Tính toán các đặc trưng thống kê cho cả session ---
    num_queries = len(session_df)
    query_types = session_df['query'].str.lower().str.split().str[0].value_counts().to_dict()
    
    try:
        features_df = session_df['query'].apply(extract_query_features).apply(pd.Series)
        session_df = pd.concat([session_df, features_df], axis=1)
        avg_joins = session_df['num_joins'].mean()
        total_subqueries = int(session_df['has_subquery'].sum())
        total_unions = int(session_df['has_union'].sum())
    except KeyError as e:
        # Thay thế st.warning bằng logging.warning
        logger.warning(f"Không thể tính toán đặc trưng độ phức tạp: {e}. Phân tích sẽ tiếp tục với thông tin cơ bản.")
        avg_joins, total_subqueries, total_unions = 0.0, 0, 0

    all_tables_touched = [table for sublist in session_df.get('tables_touched', []) for table in sublist]
    top_5_tables = pd.Series(all_tables_touched).value_counts().nlargest(5).to_dict()

    # --- Bước 4: Xây dựng Prompt chi tiết cho session ---
    prompt_round_1 = f"""
# ROLE AND GOAL
You are a Database Security Intelligence Analyst. Your task is to analyze a statistical summary of a user's database session. The system has flagged this session for accessing many distinct tables. Based on the aggregated data profile, determine if the session's behavior is indicative of a threat (like data reconnaissance or scanning) or a legitimate complex operation (like a batch job or analytics query). Provide a structured JSON analysis.

# SESSION PROFILE (Aggregated Data)
- User: "{user}"
- Session Window: From "{start_time}" to "{end_time}"
- Total Queries in Session: {num_queries}
- Distinct Tables Accessed: {distinct_tables_count}

# BEHAVIORAL STATISTICS
- Query Type Distribution: {json.dumps(query_types)}
- Top 5 Most Accessed Tables: {json.dumps(top_5_tables)}
- Average Number of JOINs per Query: {avg_joins:.2f}
- Total Queries with Subqueries: {total_subqueries}
- Total Queries with UNION: {total_unions}

# PROCESS
1.  **Analyze the Profile:** Does the distribution of query types (e.g., mostly SELECTs) and the complexity metrics (joins, subqueries) suggest analytics, or simple data extraction?
2.  **Evaluate Table Access:** Are the top accessed tables related, suggesting a logical business operation? Or are they unrelated, suggesting schema probing?
3.  **Synthesize a Conclusion:** Combine these insights to classify the overall behavior. A high number of simple SELECTs across unrelated tables is highly suspicious. A high number of complex JOINs across related tables is likely a legitimate report.
4.  **Final Output:** Provide your final assessment ONLY in the specified JSON format. Do not include any text, notes, or explanations outside of the JSON block.

# JSON OUTPUT STRUCTURE
{{
  "session_summary": "A one-sentence summary of the user's behavior based on the statistical profile.",
  "is_threatening_behavior": boolean, 
  "confidence_score": float,
  "behavior_type": "string (e.g., 'Data Reconnaissance', 'Complex Reporting/Analytics', 'ETL Process', 'Suspicious Probing')",
  "detailed_analysis": "Your detailed reasoning. Explain how the statistical profile led to your conclusion. For example: 'The high number of SELECT queries combined with access to unrelated tables like 'users', 'system_config', and 'product_logs' strongly suggests a reconnaissance attempt to map the database schema.'",
  "recommendation": "Actionable advice for the security team based on this profile."
}}
"""
    # --- Bước 5: Gửi yêu cầu đến LLM và xử lý kết quả ---
    try:
        # === THỰC HIỆN LẦN 1 ===
        logger.info("Sending first session analysis request to LLM")
        res1 = llm_manager.analyze_with_fallback(prompt_round_1)
        first_analysis_str = res1["result"]['message']['content']
        first_analysis = json.loads(first_analysis_str)
        logger.info("First session analysis completed successfully")

        # --- BƯỚC 5: XÂY DỰNG PROMPT LẦN 2 (NÂNG CẤP) ---
        prompt_round_2 = f"""
# ROLE AND GOAL
You are a Senior Security Operations Center (SOC) Analyst. Your task is to **verify and enrich** a preliminary analysis of a user's database session. You must act as a final checkpoint, using the initial findings as a starting point to provide a more definitive and actionable conclusion.

# [ORIGINAL SESSION PROFILE]
- User: "{user}"
- Session Window: From "{start_time}" to "{end_time}"
- Total Queries: {num_queries}, Distinct Tables Accessed: {distinct_tables_count}
- Key Statistics: Query types were {json.dumps(query_types)}; Top tables were {json.dumps(top_5_tables)}.

# [PRELIMINARY AI ANALYSIS - FOR YOUR REVIEW]
```json
{json.dumps(first_analysis, indent=2)}
```

# [INSTRUCTIONS FOR FINAL ASSESSMENT]
Based on both the session profile and the preliminary analysis, produce a final, improved assessment. Follow this critical thinking process:
1. **Validate the behavior_type:** Does the initial classification make sense? For example, if it says 'Data Reconnaissance' but the queries involve many INSERTs and UPDATEs, it might be an 'ETL Process' instead. Correct the classification if necessary.
2. **Assess the is_threatening_behavior flag:** Do you agree with this binary conclusion? A 'Complex Reporting' job is not a threat, but 'Suspicious Probing' is. Justify your final decision.
3. **Refine the detailed_analysis:** Add more depth. Connect the dots. For example: "The initial analysis correctly identified this as 'Data Reconnaissance'. I will add that the user accessed a mix of application tables (orders, products) and sensitive system tables (users, customers), which is a strong indicator of an attacker trying to map the entire database schema, not just performing a business function."
4. **Enhance the recommendation:** Make the advice more concrete. Instead of a generic recommendation, provide a tiered response. For example: "Immediate Action: Monitor user '{user}'s activities in real-time. Next Step: Review application code to see if this pattern is expected. Long-term: Implement stricter access controls to prevent application users from querying unrelated tables."
5. **Update confidence_score:** Provide your confidence level in this final, refined analysis.

Final Output: Return ONLY the refined JSON object in the same, original format. Do not add any conversational text.
"""
    
        # === THỰC HIỆN LẦN 2 ===
        logger.info("Sending second session analysis request to LLM")
        res2 = llm_manager.analyze_with_fallback(prompt_round_2)
        final_analysis_str = res2["result"]['message']['content']
        final_analysis = json.loads(final_analysis_str)
        logger.info("Second session analysis completed successfully")
    
        # Nếu muốn so sánh 2 kết quả, có thể kiểm tra:
        discrepancy_warning = None
        if first_analysis != final_analysis:
            discrepancy_warning = "⚠️ Có sự khác biệt giữa lần phân tích đầu và lần phân tích sau. Hãy xem xét kỹ."
    
        # --- Bước 6: Tổng hợp và trả về kết quả ---
        return {
            "first_analysis": first_analysis,
            "final_analysis": final_analysis,
            "prompt_round_1": prompt_round_1,
            "prompt_round_2": prompt_round_2,
            "discrepancy_warning": discrepancy_warning,
            "providers_used": [res1["provider"], res2["provider"]],
            "analysis_timestamp": time.time()
        }

    except json.JSONDecodeError as e:
        logger.error(f"Lỗi: AI trả về JSON không hợp lệ. {e}")
        # Ném lỗi để tầng API có thể bắt và xử lý
        raise ValueError(f"AI response is not a valid JSON: {e}")
    except Exception as e:
        logger.error(f"Lỗi khi phân tích session với LLM: {e}")
        raise ConnectionError(f"LLM session analysis failed: {e}")
