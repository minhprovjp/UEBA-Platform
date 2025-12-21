# engine/llm_analyzer.py
"""
================================================================================
MODULE PHÂN TÍCH LLM - OLLAMA ONLY (2 ROUNDS - ORIGINAL PROMPTS)
================================================================================
- Sử dụng Ollama (Local AI) duy nhất.
- Giữ nguyên cơ chế 2 vòng (Round 1: Phân tích -> Round 2: Review) để tăng độ chính xác.
- Đã thêm bộ lọc JSON để sửa lỗi hiển thị với DeepSeek-R1.
"""

import ollama
import pandas as pd
import json
import logging
import os
import sys
import time
import re
from urllib.parse import urlparse
from typing import Optional, Dict, Any

# Thêm đường dẫn để import utils từ thư mục cha
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from engine.utils import extract_query_features

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [LLM_Dual] - %(message)s')
logger = logging.getLogger(__name__)

# --- [FIX QUAN TRỌNG] HÀM LÀM SẠCH JSON TỪ DEEPSEEK ---
def clean_json_text(text: str) -> str:
    """Loại bỏ thẻ <think> và markdown để lấy JSON thuần."""
    if not text: return "{}"
    # Xóa thẻ suy nghĩ của DeepSeek (flags=re.DOTALL để match xuống dòng)
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    # Xóa markdown code block ```json ... ```
    match = re.search(r'```json\s*(.*?)\s*```', text, flags=re.DOTALL)
    if match: return match.group(1)
    match = re.search(r'```\s*(.*?)\s*```', text, flags=re.DOTALL)
    if match: return match.group(1)
    return text.strip()

class OllamaProvider:
    """Provider đơn giản hóa chỉ cho Ollama"""
    
    def __init__(self, host: str, timeout: int = 120):
        self.host = host
        self.timeout = timeout
        self.client = None
        self._connect()
    
    def _connect(self):
        try:
            parsed_url = urlparse(self.host)
            if not parsed_url.scheme or not parsed_url.netloc:
                raise ValueError(f"Invalid host URL: {self.host}")
            
            logger.info(f"Connecting to Ollama at {self.host}...")
            self.client = ollama.Client(host=self.host, timeout=self.timeout)
        except Exception as e:
            logger.error(f"Ollama connection failed: {e}")
            self.client = None

    def analyze(self, prompt: str, model: str = "deepseek-r1:1.5b", **kwargs) -> Dict[str, Any]:
        if not self.client:
            self._connect()
            if not self.client: raise ConnectionError("Ollama Disconnected")
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                response = self.client.chat(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    format="json", # Cố gắng ép format JSON
                    options={"temperature": 0.6}
                )
                return response
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                time.sleep(1)
        raise ConnectionError("Failed to get response from Ollama")

# --- LOGIC PHÂN TÍCH EVENT (QUERY) ---
def analyze_query_with_llm(
    anomaly_row: Dict[str, Any], 
    anomaly_type_from_system: str,
    llm_config: Dict[str, Any],
    rules_config: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    
    logger.info("Initializing Dual-Round Analysis (Ollama Only)...")
    
    # 1. Setup Provider
    host = llm_config.get('ollama_host', 'http://100.92.147.73:11434')
    # Timeout 120s để đảm bảo DeepSeek đủ thời gian suy nghĩ
    provider = OllamaProvider(host=host, timeout=120)
    model_name = llm_config.get('ollama_model', 'uba-expert:latest')

    # 2. Prepare Data
    query_to_analyze = anomaly_row.get('query', '')
    if not query_to_analyze:
        query_to_analyze = "N/A (Aggregated Session or Empty Query)"
    
    sanitized_query = query_to_analyze.replace('"', '\\"').replace('\n', ' ')

    # Helper chuyển đổi kiểu dữ liệu (Fix lỗi JSON serialization)
    def get_as_native_type(value, default_val, target_type):
        if pd.isna(value): return default_val
        try: return target_type(value)
        except: return default_val

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
  "risk_level": "string ('None', 'Low', 'Medium', 'High', 'Critical')",
  "performance_impact": "string ('None', 'Low', 'Medium', 'High')",
  "summary": "A one-sentence, non-technical summary of your finding.",
  "detailed_analysis": "A detailed, technical explanation of WHY you reached your conclusion, referencing the context provided and the query structure.",
  "recommendation": "A clear, actionable next step for the human analyst (e.g., 'Block this query pattern immediately', 'Add an index on the 'user_id' column', 'Verify with the user's manager').",
  "tags": ["array", "of", "string", "keywords"]
}}
"""

    try:
        # --- ROUND 1 EXECUTION ---
        logger.info("Sending Round 1 Request...")
        res1 = provider.analyze(prompt_round_1, model=model_name)
        
        # [FIX] Clean JSON
        raw_1 = res1['message']['content']
        clean_1 = clean_json_text(raw_1)
        try:
            first_analysis = json.loads(clean_1)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse Round 1 JSON. Raw: {clean_1}")
            first_analysis = {"summary": "Round 1 Parsing Error", "detailed_analysis": clean_1}
            
        logger.info("Round 1 Completed.")

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
4. **Refine risk_level:** Is the risk level appropriate? A SELECT * on a public table is 'Low' risk, but on a customers table, it's 'High'.
5. **Deepen detailed_analysis:** Go beyond the obvious. Explain why it's a risk. Instead of "SQL Injection possible," say "The query contains a classic '1'='1' tautology, a textbook SQL injection pattern designed to bypass authentication."
6. **Strengthen recommendation:** Make it more specific and actionable. Instead of "Check with user," suggest "Immediately contact user '{anomaly_row.get('user', 'N/A')}' to verify business purpose. If unverified, consider temporarily disabling the account and review all recent activity from IP '{anomaly_row.get('client_ip', 'N/A')}'."
7. **Update confidence_score:** Your confidence in THIS (your final) analysis.

Final Output: Return ONLY the refined JSON object in the same, original format. Do not add any conversational text.
"""

            # --- ROUND 2 EXECUTION ---
        logger.info("Sending Round 2 Request...")
        res2 = provider.analyze(prompt_round_2, model=model_name)
        
        # [FIX] Clean JSON
        raw_2 = res2['message']['content']
        clean_2 = clean_json_text(raw_2)
        final_analysis = json.loads(clean_2)
        logger.info("Round 2 Completed.")
        
        return {
            "first_analysis": first_analysis,
            "final_analysis": final_analysis,
            "providers_used": ["Ollama", "Ollama"],
            "analysis_timestamp": time.time()
        }

    except Exception as e:
        logger.error(f"Analysis Error: {e}")
        return {
            "final_analysis": {
                "summary": "AI Processing Failed",
                "detailed_analysis": f"Internal Error: {str(e)}",
                "is_anomalous": False,
                "confidence_score": 0.0,
                "risk_level": "Unknown",
                "recommendation": "Check backend logs."
            }
        }


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
    logger.info("Initializing Session Analysis (Dual-Round)...")

    host = llm_config.get('ollama_host', 'http://100.92.147.73:11434')
    provider = OllamaProvider(host=host, timeout=120)
    model_name = llm_config.get('ollama_model', 'uba-expert:latest')

    # Data Prep
    user = session_anomaly_row.get('user', 'N/A')
    start_time = session_anomaly_row.get('start_time', 'N/A')
    end_time = session_anomaly_row.get('end_time', 'N/A')
    distinct_tables_count = session_anomaly_row.get('distinct_tables_count', 0)
    queries_details = session_anomaly_row.get('queries_details', [])

    if not queries_details:
        return {"final_analysis": {"session_summary": "Empty Session", "is_threatening_behavior": False}}

    # Feature Extraction (Lite version)
    session_df = pd.DataFrame(queries_details)
    num_queries = len(session_df)
    try:
        query_types = session_df['query'].str.lower().str.split().str[0].value_counts().to_dict()
    except:
        query_types = {}

    # Try basic stats
    try:
        features_df = session_df['query'].apply(extract_query_features).apply(pd.Series)
        session_df = pd.concat([session_df, features_df], axis=1)
        avg_joins = session_df['num_joins'].mean()
        total_subqueries = int(session_df['has_subquery'].sum())
        total_unions = int(session_df['has_union'].sum())
        all_tables = [t for sub in session_df.get('tables_touched', []) for t in sub]
        top_5_tables = pd.Series(all_tables).value_counts().nlargest(5).to_dict()
    except:
        avg_joins, total_subqueries, total_unions = 0, 0, 0
        top_5_tables = {}

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
  "reasoning": "Your detailed reasoning. Explain how the statistical profile led to your conclusion. For example: 'The high number of SELECT queries combined with access to unrelated tables like 'users', 'system_config', and 'product_logs' strongly suggests a reconnaissance attempt to map the database schema.'",
  "recommendation": "Actionable advice for the security team based on this profile."
}}
"""
    # --- Bước 5: Gửi yêu cầu đến LLM và xử lý kết quả ---
    try:
        # Round 1
        logger.info("Sending Session Round 1 Request...")
        res1 = provider.analyze(prompt_round_1, model=model_name)
        first_analysis = json.loads(clean_json_text(res1['message']['content']))
        logger.info("Session Round 1 Completed.")

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
        logger.info("Sending Session Round 2 Request...")
        res2 = provider.analyze(prompt_round_2, model=model_name)
        final_analysis = json.loads(clean_json_text(res2['message']['content']))
        logger.info("Session Round 2 Completed.")
    
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
