"""
================================================================================
MODULE XỬ LÝ DỮ LIỆU CHÍNH (PHIÊN BẢN ĐỘC LẬP) - FINAL VERSION 2025
================================================================================
Tích hợp:
- Feature engineering cấp nghiên cứu (sqlglot + behavioral z-score + entropy)
- Semi-supervised ML: Isolation Forest → pseudo-label → LightGBM (self-training)
- Tất cả luật Rule-based cũ vẫn hoạt động tốt hơn nhờ feature mới
"""

import os
import sys
import logging
import joblib
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.ensemble import IsolationForest
from datetime import datetime
import hashlib
from pathlib import Path

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [DataProcessor-PROD] - %(message)s')
logger = logging.getLogger(__name__)

# --- Paths ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MODELS_DIR, USER_MODELS_DIR
from engine.features import enhance_features_batch
from utils import (
    is_late_night_query, is_potential_large_dump,
    analyze_sensitive_access, check_unusual_user_activity_time,
    is_suspicious_function_used, is_privilege_change, get_normalized_query
)

# Production model paths
PROD_MODEL_PATH = os.path.join(MODELS_DIR, "lgb_uba_production.joblib")
FALLBACK_MODEL_PATH = os.path.join(MODELS_DIR, "lgb_uba_fallback.joblib")
MODEL_METADATA_PATH = os.path.join(MODELS_DIR, "model_metadata.json")

os.makedirs(MODELS_DIR, exist_ok=True)


class ProductionUBAEngine:
    def __init__(self):
        self.model = None
        self.fallback_model = None
        self.features = None
        self.model_version = "unknown"
        self.last_trained = None
        self.load_models()

    def load_models(self):
        """Load production → fallback → train new"""
        if os.path.exists(PROD_MODEL_PATH):
            try:
                data = joblib.load(PROD_MODEL_PATH)
                self.model = data['model']
                self.features = data['features']
                self.model_version = data.get('version', 'v0')
                self.last_trained = data.get('trained_at', 'unknown')
                logger.info(f"Loaded PRODUCTION model v{self.model_version} (trained: {self.last_trained})")
                return
            except Exception as e:
                logger.error(f"Failed to load production model: {e}")

        if os.path.exists(FALLBACK_MODEL_PATH):
            try:
                data = joblib.load(FALLBACK_MODEL_PATH)
                self.model = data['model']
                self.features = data['features']
                logger.warning("Loaded FALLBACK model")
                return
            except Exception as e:
                logger.error(f"Failed to load fallback: {e}")

        logger.warning("No model found. Will train on first batch.")

    def save_production_model(self, model, features):
        """Atomic save with versioning"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        version_hash = hashlib.md5(str(features).encode()).hexdigest()[:8]
        version = f"v{len([f for f in os.listdir(MODELS_DIR) if f.startswith('lgb_uba_')])-1}_{version_hash}"

        data = {
            'model': model,
            'features': features,
            'version': version,
            'trained_at': datetime.now().isoformat(),
            'feature_count': len(features)
        }

        # Backup current production
        if os.path.exists(PROD_MODEL_PATH):
            backup_path = PROD_MODEL_PATH.replace(".joblib", f"_backup_{timestamp}.joblib")
            os.replace(PROD_MODEL_PATH, backup_path)

        # Save new production
        joblib.dump(data, PROD_MODEL_PATH)
        joblib.dump(data, FALLBACK_MODEL_PATH)  # always update fallback
        logger.info(f"New PRODUCTION model saved: {version} → {PROD_MODEL_PATH}")

    def train_and_update(self, df_enhanced):
        """Auto-retrain when enough new data"""
        if len(df_enhanced) < 5000:
            return False

        X = df_enhanced[self.features].fillna(0) if self.features else df_enhanced.filter(like='').fillna(0)

        # Self-training pipeline
        iso = IsolationForest(contamination=0.015, random_state=42, n_jobs=-1)
        iso.fit(X)
        pseudo_pred = iso.predict(X)
        high_conf_anoms = pseudo_pred == -1

        X_train = pd.concat([X, X[high_conf_anoms]])
        y_train = np.concatenate([np.zeros(len(X)), np.ones(sum(high_conf_anoms))])

        new_model = lgb.LGBMClassifier(
            n_estimators=600,
            learning_rate=0.05,
            max_depth=-1,
            num_leaves=256,
            scale_pos_weight=20,
            random_state=42,
            n_jobs=-1,
            verbose=-1
        )

        cat_cols = [c for c in ['user', 'client_ip', 'database', 'command_type'] if c in X.columns]
        new_model.fit(X_train, y_train, categorical_feature=cat_cols)

        self.save_production_model(new_model, list(X.columns))
        self.model = new_model
        self.features = list(X.columns)
        return True


# Global engine instance
uba_engine = ProductionUBAEngine()


def load_and_process_data(input_df: pd.DataFrame, config_params: dict) -> dict:
    global uba_engine

    if input_df is None or input_df.empty:
        return {"all_logs": pd.DataFrame(), "anomalies_ml": pd.DataFrame()}

    df_logs = input_df.copy()

    # Basic cleanup
    for col in ['rows_returned', 'rows_affected', 'execution_time_ms']:
        if col not in df_logs.columns:
            df_logs[col] = 0
        df_logs[col] = df_logs[col].fillna(0)

    df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp'], errors='coerce', utc=True)
    df_logs = df_logs.dropna(subset=['timestamp']).reset_index(drop=True)
    df_logs['query'] = df_logs['query'].astype(str)
    
    df_logs['normalized_query'] = df_logs['query'].apply(get_normalized_query)
    df_logs['query_length'] = df_logs['normalized_query'].str.len()

    if df_logs.empty:
        return {"all_logs": df_logs}

    # ADVANCED FEATURES
    df_enhanced, ML_FEATURES = enhance_features_batch(df_logs.copy())
    df_logs = df_enhanced

    # Auto-retrain check
    if uba_engine.model is None or len(df_logs) > 10000:
        logger.info("Triggering auto-retrain...")
        uba_engine.train_and_update(df_logs)

    # ML Scoring
    if uba_engine.model and uba_engine.features:
        try:
            X = df_logs[uba_engine.features].fillna(0)
            scores = uba_engine.model.predict_proba(X)[:, 1]
            df_logs['ml_anomaly_score'] = scores
            threshold = max(np.quantile(scores, 0.99), 0.75)
            anomalies_ml = df_logs[scores >= threshold].copy()
        except Exception as e:
            logger.error(f"ML inference failed: {e}. Using rule-only mode.")
            df_logs['ml_anomaly_score'] = 0.0
            anomalies_ml = pd.DataFrame()
    else:
        df_logs['ml_anomaly_score'] = 0.0
        anomalies_ml = pd.DataFrame()

    # === 4. RULE-BASED DETECTION (giữ nguyên + mạnh hơn) ===
    logging.info("Running rule-based detection...")
    rules_config = config_params or {}

    # Lấy cấu hình rule
    from datetime import time as dt_time
    p_late_night_start_time = dt_time.fromisoformat(rules_config.get('p_late_night_start_time', '00:00:00'))
    p_late_night_end_time = dt_time.fromisoformat(rules_config.get('p_late_night_end_time', '05:00:00'))
    p_known_large_tables = rules_config.get('p_known_large_tables', [])
    p_time_window_minutes = int(rules_config.get('p_time_window_minutes', 5))
    p_min_distinct_tables = int(rules_config.get('p_min_distinct_tables', 3))
    p_sensitive_tables = rules_config.get('p_sensitive_tables', [])
    p_allowed_users_sensitive = rules_config.get('p_allowed_users_sensitive', [])
    p_safe_hours_start = int(rules_config.get('p_safe_hours_start', 8))
    p_safe_hours_end = int(rules_config.get('p_safe_hours_end', 18))
    p_safe_weekdays = rules_config.get('p_safe_weekdays', [0, 1, 2, 3, 4])
    p_min_queries_for_profile = int(rules_config.get('p_min_queries_for_profile', 10))
    p_quantile_start = float(rules_config.get('p_quantile_start', 0.15))
    p_quantile_end = float(rules_config.get('p_quantile_end', 0.85))

    # Rule 1: Late night
    df_logs['is_late_night'] = df_logs['timestamp'].apply(
        lambda ts: is_late_night_query(ts, p_late_night_start_time, p_late_night_end_time)
    )
    anomalies_late_night = df_logs[df_logs['is_late_night']].copy()

    # Rule 2: Large dump
    df_logs['is_potential_dump'] = df_logs.apply(
        lambda row: is_potential_large_dump(row, p_known_large_tables), axis=1
    )
    anomalies_large_dump = df_logs[df_logs['is_potential_dump']].copy()

    # Rule 3: Multi-table in short window (dùng accessed_tables từ feature mới)
    anomalies_multiple_tables_list = []
    window = pd.Timedelta(minutes=p_time_window_minutes)
    for user, group in df_logs.groupby('user'):
        if len(group) < 2: continue
        group = group.sort_values('timestamp').reset_index(drop=True)
        session_tables = set()
        session_queries = []
        start_time = group.iloc[0]['timestamp']

        for _, row in group.iterrows():
            tables = set(row.get('accessed_tables', []))
            if (row['timestamp'] - start_time) > window:
                if len(session_tables) >= p_min_distinct_tables:
                    anomalies_multiple_tables_list.append({
                        'user': user,
                        'start_time': start_time,
                        'end_time': session_queries[-1]['timestamp'],
                        'distinct_tables_count': len(session_tables),
                        'tables_accessed_in_session': sorted(list(session_tables)),
                        'queries_details': session_queries
                    })
                session_tables = tables
                session_queries = [{'timestamp': row['timestamp'], 'query': row['query'], 'tables_touched': list(tables)}]
                start_time = row['timestamp']
            else:
                session_tables.update(tables)
                session_queries.append({'timestamp': row['timestamp'], 'query': row['query'], 'tables_touched': list(tables)})
        if len(session_tables) >= p_min_distinct_tables:
            anomalies_multiple_tables_list.append({
                'user': user,
                'start_time': start_time,
                'end_time': session_queries[-1]['timestamp'],
                'distinct_tables_count': len(session_tables),
                'tables_accessed_in_session': sorted(list(session_tables)),
                'queries_details': session_queries
            })
    anomalies_multiple_tables_df = pd.DataFrame(anomalies_multiple_tables_list)

    # Rule 4: Sensitive access
    df_logs['sensitive_access_info'] = df_logs.apply(
        lambda row: analyze_sensitive_access(row, p_sensitive_tables, p_allowed_users_sensitive,
                                             p_safe_hours_start, p_safe_hours_end, p_safe_weekdays),
        axis=1
    )
    anomalies_sensitive_access = df_logs[df_logs['sensitive_access_info'].notna()].copy()
    if not anomalies_sensitive_access.empty:
        expanded = anomalies_sensitive_access['sensitive_access_info'].apply(pd.Series)
        anomalies_sensitive_access = pd.concat([anomalies_sensitive_access.drop(columns=['sensitive_access_info'], errors='ignore'), expanded], axis=1)

    # Rule 5: Unusual user time
    user_profiles = {}
    for user, g in df_logs.groupby('user'):
        if len(g) >= p_min_queries_for_profile:
            hours = g['timestamp'].dt.hour
            user_profiles[user] = {
                'active_start': int(hours.quantile(p_quantile_start)),
                'active_end': int(hours.quantile(p_quantile_end))
            }
    df_logs['unusual_activity_reason'] = df_logs.apply(
        lambda row: check_unusual_user_activity_time(row, user_profiles), axis=1
    )
    anomalies_unusual_user_time = df_logs[df_logs['unusual_activity_reason'].notna()].copy()

    # Rule 6 & 7: SQLi + Privilege
    sqli_res = df_logs['query'].apply(is_suspicious_function_used).apply(pd.Series)
    sqli_res.columns = ['is_suspicious_func', 'suspicious_func_name']
    df_logs = pd.concat([df_logs, sqli_res], axis=1)
    anomalies_sqli = df_logs[df_logs['is_suspicious_func'] == True].copy()

    priv_res = df_logs['query'].apply(is_privilege_change).apply(pd.Series)
    priv_res.columns = ['is_privilege_change', 'privilege_cmd_name']
    df_logs = pd.concat([df_logs, priv_res], axis=1)
    anomalies_privilege = df_logs[df_logs['is_privilege_change'] == True].copy()

    # Normal activities
    anomalous_indices = set(anomalies_ml.index)
    for df_anom in [anomalies_late_night, anomalies_large_dump, anomalies_sensitive_access,
                    anomalies_unusual_user_time, anomalies_sqli, anomalies_privilege]:
        if not df_anom.empty:
            anomalous_indices.update(df_anom.index)
    normal_activities = df_logs[~df_logs.index.isin(anomalous_indices)].copy()

    # === KẾT QUẢ ===
    results = {
        "all_logs": df_logs,
        "anomalies_ml": anomalies_ml,
        "anomalies_late_night": anomalies_late_night,
        "anomalies_dump": anomalies_large_dump,
        "anomalies_multi_table": anomalies_multiple_tables_df,
        "anomalies_sensitive": anomalies_sensitive_access,
        "anomalies_user_time": anomalies_unusual_user_time,
        "anomalies_sqli": anomalies_sqli,
        "anomalies_privilege": anomalies_privilege,
        "normal_activities": normal_activities
    }

    logging.info(f"Processing complete. Total anomalies: {len(anomalous_indices)} / {len(df_logs)}")
    return results


# --------------------------- Model I/O (giữ lại nếu cần sau) ---------------------------
def save_model_and_scaler(model, scaler, path):
    joblib.dump({'model': model, 'scaler': scaler}, path)

def load_model_and_scaler(path):
    if os.path.exists(path):
        data = joblib.load(path)
        return data['model'], data['scaler']
    return None, None