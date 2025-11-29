# engine/features.py

import re
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Any
import logging
import math
from collections import Counter

# Thử import sqlglot, nếu chưa cài thì fallback sang regex đơn giản
try:
    import sqlglot
    import sqlglot.expressions as exp
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    logging.warning("Thư viện 'sqlglot' chưa được cài đặt. Đang dùng chế độ Regex cơ bản (kém chính xác hơn). Hãy chạy: pip install sqlglot")

logging.basicConfig(level=logging.INFO)

# Sensitive tables from your schema
SENSITIVE_TABLES = {
    "hr_db.salaries", "hr_db.employees", "hr_db.salary", "mysql.user",
    "information_schema.user_privileges", "performance_schema.accounts",
    "salaries", "employees", "users"
}

# High-risk commands
RISKY_COMMANDS = {"DELETE", "DROP", "TRUNCATE", "UPDATE", "INSERT", "GRANT", "REVOKE", "CREATE USER", "ALTER USER"}

SYSTEM_SCHEMAS = {"information_schema", "performance_schema", "mysql", "sys"}

def _shannon_entropy(text: str) -> float:
    """Tính độ hỗn loạn của chuỗi (cao -> mã hóa/random string)"""
    if not text: return 0.0
    freq = Counter(text)
    length = len(text)
    return -sum((count / length) * math.log2(count / length) for count in freq.values())

def safe_parse_sql(query: str):
    """Safely parse SQL with fallback"""
    if not SQLGLOT_AVAILABLE:
        return None
    if not query or not isinstance(query, str):
        return None
    query = query.strip()
    if len(query) > 10000:  # Truncate ultra-long queries
        query = query[:10000]
    try:
        # read=None tự động detect dialect, hoặc ép 'mysql'
        return sqlglot.parse_one(query, read="mysql")
    except Exception as e:
        # logging.debug(f"SQL parse failed: {e}")
        return None

def _extract_tables(expression) -> set:
    """Extract fully qualified table names from parsed SQL"""
    tables = set()
    if not expression: 
        return tables
    try:
        for table in expression.find_all(exp.Table):
            db = table.db or ""
            name = table.name or table.this if hasattr(table, "this") else ""
            full_name = f"{db}.{name}".lower() if db else name.lower()
            if full_name:
                tables.add(full_name)
    except Exception:
        pass
    return tables

def extract_query_features(row: pd.Series) -> Dict[str, Any]:
    """Trích xuất Static Features từ 1 dòng log"""
    query = str(row.get("query", "")).strip()
    
    q_upper = query.upper()

    # Trả về -1 nếu lỗi để phân biệt với 0 thực sự
    # Model sẽ học được rằng -1 là "Missing Value"
    def safe_int(val):
        try:
            if pd.isna(val) or val == '' or val is None: return -1
            f_val = float(val)
            if math.isnan(f_val) or math.isinf(f_val): return -1
            return int(f_val)
        except: return -1

    ts = row.get("timestamp")
    if isinstance(ts, str):
        try: ts = pd.to_datetime(ts).tz_localize(None)
        except: ts = None
    elif isinstance(ts, pd.Timestamp) and ts.tzinfo:
        ts = ts.tz_localize(None)

    hour = ts.hour if ts else 12
    weekday = ts.weekday() if ts else 0
    
    # Lấy giá trị đã tính sẵn từ Publisher nếu có
    pub_entropy = row.get("query_entropy")
    pub_length = row.get("query_length")
    
    # Hàm chuyển đổi int an toàn tuyệt đối
    def safe_int(val):
        try:
            if pd.isna(val) or val == '' or val is None: 
                return 0
            # Xử lý trường hợp float('inf') hoặc float('nan')
            f_val = float(val)
            if math.isnan(f_val) or math.isinf(f_val):
                return 0
            return int(f_val)
        except: 
            return 0
    
    # Lấy giá trị từ Publisher gửi sang 
    err_cnt = safe_int(row.get("error_count"))
    # Nếu safe_int trả về -1 (lỗi dữ liệu), ta coi như 0 để tính logic flag, nhưng giữ -1 ở feature
    err_cnt_logic = max(0, err_cnt) 
    has_err = safe_int(row.get("has_error"))
    
    # Nếu publisher chưa tính (trường hợp log cũ), tính fallback
    if "has_error" not in row:
        err_code = row.get("error_code")
        if err_cnt_logic > 0:
            has_err = 1
        elif err_code is not None:
            try:
                if int(float(err_code)) != 0: has_err = 1
            except: pass
    # 1. Base Features
    f = {
        # Ưu tiên dùng giá trị từ Publisher
        "query_length": pub_length if pd.notna(pub_length) else len(query),
        "query_entropy": pub_entropy if pd.notna(pub_entropy) else _shannon_entropy(query),
        "hour_sin": np.sin(2 * np.pi * hour / 24.0),
        "hour_cos": np.cos(2 * np.pi * hour / 24.0),
        "is_weekend": 1 if weekday >= 5 else 0,
        "is_late_night": 1 if 0 <= hour < 6 else 0,
        "is_work_hours": 1 if 8 <= hour <= 18 else 0,
        
        # Truyền lại các giá trị lỗi
        "error_count": err_cnt, # Có thể là -1
        "has_error": max(0, has_err) # Flag nên là 0/1
    }

    # 2. Performance Metrics
    exec_time = float(row.get("execution_time_ms", 0)) if pd.notna(row.get("execution_time_ms")) else 0.0
    
    rows_ret = safe_int(row.get("rows_returned"))
    
    f["execution_time_ms"] = exec_time
    f["rows_returned"] = rows_ret
    f["rows_affected"] = safe_int(row.get("rows_affected"))
    f["no_index_used"] = safe_int(row.get("no_index_used"))
    
    # Ratio: Rows per Time (Data Retrieval Speed) Xử lý chia cho 0 hoặc dữ liệu lỗi (-1)
    f["data_retrieval_speed"] = 0.0
    if rows_ret >= 0:
        f["data_retrieval_speed"] = rows_ret / (exec_time + 0.001)

    # 3. SQL Structure (Parser)
    parsed = safe_parse_sql(query)
    
    f["is_parse_failed"] = 1 if parsed is None else 0

    # Default values
    for k in ["num_tables", "num_joins", "num_where_conditions", "subquery_depth", 
              "is_sensitive_access", "is_system_access", "is_dcl", "is_ddl", 
              "has_comment", "has_hex", "is_risky_command", "is_admin_command",
              "is_select_star", "has_into_outfile", "has_load_data", "has_sleep_benchmark"]:
        f[k] = 0
    f["accessed_tables"] = []

    if parsed:
        cmd = parsed.key.upper() if hasattr(parsed, "key") else "UNKNOWN"
        f["command_type"] = cmd
        
        # Check commands via parser (More accurate)
        if cmd in RISKY_COMMANDS: f["is_risky_command"] = 1
        if cmd in {"GRANT", "REVOKE", "CREATE USER", "ALTER USER", "SET PASSWORD"}: f["is_admin_command"] = 1
        if cmd in {"GRANT", "REVOKE", "CREATE USER"}: f["is_dcl"] = 1
        if cmd in {"CREATE", "DROP", "ALTER", "TRUNCATE"}: f["is_ddl"] = 1

        # Elements
        f["num_tables"] = len(list(parsed.find_all(exp.Table)))
        f["num_joins"] = len(list(parsed.find_all(exp.Join)))
        f["num_where_conditions"] = len(list(parsed.find_all(exp.Where)))
        
        # Subquery Depth
        depth = 0
        node = parsed
        while node:
            if isinstance(node, exp.Subquery): depth += 1
            try: node = list(node.children.values())[0][0]
            except: break
        f["subquery_depth"] = min(depth, 10)

        tables_found = _extract_tables(parsed)
        for full in tables_found:
            t_name = full.split('.')[-1]
            if full in SENSITIVE_TABLES or t_name in SENSITIVE_TABLES: f["is_sensitive_access"] = 1
            db_part = full.split('.')[0] if '.' in full else ''
            if db_part in SYSTEM_SCHEMAS: f["is_system_access"] = 1
        f["accessed_tables"] = list(tables_found)

    # 4. Regex Heuristics (Luôn chạy để backup khi parser fail hoặc SQLi obfucated)
    if "--" in query or "/*" in query or "#" in query: f["has_comment"] = 1
    if "0X" in q_upper: f["has_hex"] = 1
    if "SELECT *" in q_upper: f["is_select_star"] = 1
    if "INTO OUTFILE" in q_upper: f["has_into_outfile"] = 1
    if "LOAD DATA" in q_upper: f["has_load_data"] = 1
    if re.search(r"SLEEP\s*\(", query, re.I) or re.search(r"BENCHMARK\s*\(", query, re.I):
        f["has_sleep_benchmark"] = 1
    
    # Fallback command type if parser fails
    if "command_type" not in f:
        if "SELECT" in q_upper: f["command_type"] = "SELECT"
        elif "INSERT" in q_upper: f["command_type"] = "INSERT"
        elif "UPDATE" in q_upper: f["command_type"] = "UPDATE"
        elif "DELETE" in q_upper: f["command_type"] = "DELETE"
        else: f["command_type"] = "UNKNOWN"

    # Error flags fallback
    f["is_access_denied"] = 1 if "access denied" in str(row.get("error_message", "")).lower() else 0

    return f


# ==============================================================================
# BATCH FEATURE ENHANCEMENT
# ==============================================================================
def enhance_features_batch(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Apply extract_query_features to entire DataFrame + add per-user behavioral baselines
    """
    if df.empty:
        return df, []

    # 1. Reset Index
    df = df.reset_index(drop=True)

    # 2. Extract Static Features
    logging.info("Generating Static Features...")
    feature_dicts = df.apply(extract_query_features, axis=1).tolist()
    features_df = pd.DataFrame(feature_dicts)
    
    # Xử lý duplicate columns
    cols_to_drop = [c for c in features_df.columns if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
        
    df_final = pd.concat([df, features_df], axis=1)

    # 3. Contextual Features (Rolling Windows)
    logging.info("Generating Contextual Features (Rolling Windows)...")
    
    if 'timestamp' in df_final.columns:
        # Chuyển về naive datetime
        df_final['timestamp'] = pd.to_datetime(df_final['timestamp']).dt.tz_localize(None)
        df_final.sort_values(by=['user', 'timestamp'], inplace=True)
        
        def calc_rolling(g):
            g = g.set_index('timestamp').sort_index()
            win = '5min'
            
            g['query_count_5m'] = g['query_length'].rolling(win).count()
            g['error_count_5m'] = g['has_error'].rolling(win).sum()
            g['total_rows_5m'] = g['rows_returned'].rolling(win).sum()
            # Thêm std để đo độ biến thiên hành vi
            g['query_len_std_5m'] = g['query_length'].rolling(win).std().fillna(0)
            
            return g.reset_index()

        try:
            # group_keys=True để giữ 'user' trong index kết quả
            df_rolled = df_final.groupby('user', group_keys=True).apply(calc_rolling, include_groups=False)
            
            # Kết quả df_rolled có MultiIndex (user, index_cũ). 
            # Ta reset_index để đưa 'user' quay lại thành cột.
            df_final = df_rolled.reset_index(level=0)
            
            # Reset index lần nữa để index trở lại dạng 0,1,2... và xóa index thừa nếu có
            df_final = df_final.reset_index(drop=True)
            
        except TypeError:
            # Fallback cho pandas bản cũ
            df_final = df_final.groupby('user', group_keys=False).apply(calc_rolling)

        pd.set_option('future.no_silent_downcasting', True)
        df_final = df_final.fillna(0).infer_objects(copy=False)

    # 4. Behavioral Deviation (Z-Score)
    logging.info("Computing per-user behavioral deviations...")
    
    # Đảm bảo cột user tồn tại trước khi groupby
    if 'user' not in df_final.columns:
         # Fallback cực đoan nếu mất user (hiếm khi xảy ra với fix trên)
         df_final['user'] = 'unknown'

    metrics = ["execution_time_ms", "rows_returned", "data_retrieval_speed"]
    for metric in metrics:
        if metric not in df_final.columns: 
            df_final[metric] = 0.0
            
        col_name = f"{metric}_zscore"
        
        def compute_zscore(x):
            if len(x) < 3: return pd.Series(0.0, index=x.index)
            std = x.std(ddof=0)
            if std == 0: return pd.Series(0.0, index=x.index)
            return (x - x.mean()) / std

        zscores = df_final.groupby('user', group_keys=False)[metric].apply(compute_zscore)
        df_final[col_name] = zscores.fillna(0.0)

    # 5. Feature Selection & Clean up
    cat_cols = ["user", "client_ip", "database", "command_type"]
    for col in cat_cols:
        if col in df_final.columns:
            df_final[col] = df_final[col].astype("str").astype("category")

    final_features = [
        "hour_sin", "hour_cos", "is_weekend", "is_late_night", "is_work_hours",
        "query_length", "query_entropy", "num_tables", "num_joins", "num_where_conditions",
        "num_subqueries", "has_limit", "has_order_by", "has_group_by", "has_union",
        "is_select_star", "has_into_outfile", "has_load_data", "has_sleep_benchmark",
        "is_risky_command", "is_admin_command", "accessed_sensitive_tables",
        "has_error", "error_count", "is_access_denied",
        "execution_time_ms", "rows_returned", "rows_affected",
        "execution_time_ms_zscore", "rows_returned_zscore",
        "query_count_5m", "error_count_5m", "total_rows_5m", "data_retrieval_speed"
    ]

    return df_final, final_features