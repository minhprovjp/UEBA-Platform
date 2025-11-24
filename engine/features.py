# engine/features.py
# FINAL CORRECTED & ENHANCED FEATURE ENGINEERING FOR MYSQL LOG ANOMALY DETECTION
# Based on: Ronao & Cho (2024), Zhang et al. (KDD 2023), Liu et al. (TNNLS 2024)

import re
import sqlglot
import sqlglot.expressions as exp
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Any
import logging

logging.basicConfig(level=logging.INFO)

# Sensitive tables from your schema
SENSITIVE_TABLES = {
    "hr_db.salaries", "hr_db.employees", "hr_db.salary", "mysql.user",
    "information_schema.user_privileges", "performance_schema.accounts"
}

# High-risk commands
RISKY_COMMANDS = {"DELETE", "DROP", "TRUNCATE", "UPDATE", "INSERT", "GRANT", "REVOKE", "CREATE USER", "ALTER USER"}

def safe_parse_sql(query: str) -> exp.Expression:
    """Safely parse SQL with fallback"""
    if not query or not isinstance(query, str):
        return None
    query = query.strip()
    if len(query) > 10000:  # Truncate ultra-long queries
        query = query[:10000]
    try:
        return sqlglot.parse_one(query, dialect="mysql")
    except Exception as e:
        logging.debug(f"SQL parse failed: {e}")
        return None

def extract_query_features(row: pd.Series) -> Dict[str, Any]:
    """Main feature extractor - called on each log row"""
    query = str(row.get("query", "")).strip()
    user = str(row.get("user", "unknown"))
    database = str(row.get("database", "unknown"))
    timestamp = row.get("timestamp")
    if isinstance(timestamp, str):
        timestamp = pd.to_datetime(timestamp, utc=True, errors='coerce')
    hour = timestamp.hour if timestamp and pd.notna(timestamp) else 12

    features = {
        # === 1. Temporal Features ===
        "hour": hour,
        "hour_sin": np.sin(2 * np.pi * hour / 24.0),
        "hour_cos": np.cos(2 * np.pi * hour / 24.0),
        "is_weekend": 1 if timestamp and timestamp.weekday() >= 5 else 0,
        "is_late_night": 1 if hour in (0,1,2,3,4,5) else 0,
        "is_work_hours": 1 if 7 <= hour <= 18 else 0,

        # === 2. Query Syntax & Structure ===
        "query_length": len(query),
        "query_entropy": _shannon_entropy(query),
        "num_tokens": len(query.split()),
        "has_semicolon": 1 if query.endswith(";") else 0,
        "is_commented": 1 if query.strip().startswith(("/*", "--", "#")) else 0,

        # === 3. SQL Parsing Features (using sqlglot) ===
        "num_tables": 0,
        "num_joins": 0,
        "num_where_conditions": 0,
        "num_subqueries": 0,
        "has_limit": 0,
        "has_order_by": 0,
        "has_group_by": 0,
        "has_having": 0,
        "has_union": 0,
        "command_type": "UNKNOWN",
        "accessed_tables": [],
        "accessed_sensitive_tables": 0,
        "is_risky_command": 0,
        "is_admin_command": 0,
        "is_select_star": 0,
        "has_into_outfile": 0,
        "has_load_data": 0,
        "has_sleep_benchmark": 0,
    }

    parsed = safe_parse_sql(query.upper())
    if parsed:
        cmd = parsed.key.upper() if hasattr(parsed, "key") else ""
        features["command_type"] = cmd

        if cmd in RISKY_COMMANDS:
            features["is_risky_command"] = 1
        if cmd in {"GRANT", "REVOKE", "CREATE USER", "ALTER USER", "SET PASSWORD"}:
            features["is_admin_command"] = 1

        # Extract tables
        tables = _extract_tables(parsed)
        features["accessed_tables"] = list(tables)
        features["num_tables"] = len(tables)
        sensitive_hits = len(tables.intersection(SENSITIVE_TABLES))
        features["accessed_sensitive_tables"] = sensitive_hits

        # Structural analysis
        features["num_joins"] = len(parsed.find_all(exp.Join))
        features["num_where_conditions"] = len(list(parsed.find_all(exp.Where)))
        features["num_subqueries"] = len(list(parsed.find_all(exp.Subquery)))
        features["has_limit"] = 1 if parsed.find(exp.Limit) else 0
        features["has_order_by"] = 1 if parsed.find(exp.Order) else 0
        features["has_group_by"] = 1 if parsed.find(exp.Group) else 0
        features["has_having"] = 1 if parsed.find(exp.Having) else 0
        features["has_union"] = 1 if parsed.find(exp.Union) else 0

        # Dangerous patterns
        if "SELECT *" in query.upper():
            features["is_select_star"] = 1
        if "INTO OUTFILE" in query.upper() or "INTO DUMPFILE" in query.upper():
            features["has_into_outfile"] = 1
        if "LOAD DATA" in query.upper():
            features["has_load_data"] = 1
        if re.search(r"SLEEP\s*\(", query, re.I) or re.search(r"BENCHMARK\s*\(", query, re.I):
            features["has_sleep_benchmark"] = 1

    # === 4. Performance & Result Features (from performance_schema) ===
    features["execution_time_ms"] = float(row.get("execution_time_ms", row.get("execution_time_sec", 0)) * 1000)
    features["rows_returned"] = int(row.get("rows_returned", 0))
    features["rows_affected"] = int(row.get("rows_affected", 0))
    features["has_error"] = 1 if row.get("error_code", 0) != 0 else 0
    features["is_access_denied"] = 1 if "access denied" in str(row.get("error_message", "")).lower() else 0

    # === 5. Behavioral Deviation (per-user baselines) ===
    # These will be filled later in data_processor.py using groupby
    features["exec_time_zscore"] = 0.0
    features["rows_returned_zscore"] = 0.0

    return features


def _extract_tables(expression) -> set:
    """Extract fully qualified table names from parsed SQL"""
    tables = set()
    for table in expression.find_all(exp.Table):
        db = table.db or ""
        name = table.name or table.this if hasattr(table, "this") else ""
        full_name = f"{db}.{name}".lower() if db else name.lower()
        if full_name:
            tables.add(full_name)
    return tables


def _shannon_entropy(text: str) -> float:
    """Calculate Shannon entropy - high for obfuscated/SQLi strings"""
    if not text:
        return 0.0
    import math
    from collections import Counter
    freq = Counter(text)
    length = len(text)
    return -sum((count / length) * math.log2(count / length) for count in freq.values())


# ==============================================================================
# BATCH FEATURE ENHANCEMENT (Call this in data_processor.py)
# ==============================================================================
def enhance_features_batch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply extract_query_features to entire DataFrame + add per-user behavioral baselines
    """
    if df.empty:
        return df

    # 1. Extract raw features
    logging.info("Extracting query syntax features...")
    feature_dfs = df.apply(extract_query_features, axis=1)
    features_df = pd.json_normalize(feature_dfs)
    df = pd.concat([df.reset_index(drop=True), features_df], axis=1)

    # 2. Per-user behavioral baselines (z-score deviation)
    logging.info("Computing per-user behavioral deviations...")
    for metric in ["execution_time_ms", "rows_returned"]:
        df[f"{metric}_zscore"] = 0.0
        df[f"{metric}_pct"] = 0.0

        def zscore_group(g):
            if len(g) < 3:
                return pd.Series([0.0] * len(g), index=g.index)
            mean, std = g[metric].mean(), g[metric].std()
            if std == 0:
                return pd.Series([0.0] * len(g), index=g.index)
            return (g[metric] - mean) / std

        df[f"{metric}_zscore"] = df.groupby("user")[metric].transform(zscore_group)

    # 3. Final categorical encoding prep
    categorical_cols = ["user", "client_ip", "database", "command_type"]
    for col in categorical_cols:
        if col not in df.columns:
            df[col] = "unknown"
        df[col] = df[col].astype("category")

    # 4. Final feature list for ML
    final_features = [
        # Temporal
        "hour_sin", "hour_cos", "is_weekend", "is_late_night", "is_work_hours",
        # Syntax
        "query_length", "query_entropy", "num_tables", "num_joins", "num_where_conditions",
        "num_subqueries", "has_limit", "has_order_by", "has_group_by", "has_union",
        "is_select_star", "has_into_outfile", "has_load_data", "has_sleep_benchmark",
        # Risk
        "is_risky_command", "is_admin_command", "accessed_sensitive_tables",
        "has_error", "is_access_denied",
        # Performance
        "execution_time_ms", "rows_returned", "rows_affected",
        "execution_time_ms_zscore", "rows_returned_zscore",
    ]

    # Add categorical columns for LightGBM
    df = df.copy()
    for col in categorical_cols:
        if col in df.columns and col not in final_features:
            final_features.append(col)

    logging.info(f"Feature engineering complete. Final {len(final_features)} features.")
    return df, final_features