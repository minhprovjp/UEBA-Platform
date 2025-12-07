# UBA-PLATFORM/engine/data_processor.py
"""
================================================================================
MODULE XỬ LÝ DỮ LIỆU CHÍNH
================================================================================
Tích hợp:
- Feature engineering
- Semi-supervised ML: AutoEncoder (PyOD) → LightGBM
- Background Training: Huấn luyện chạy ngầm không chặn luồng chính
"""

import os
import sys
import logging
import joblib
import pandas as pd
import numpy as np
import lightgbm as lgb
import time
import hashlib
import threading  # <--- [QUAN TRỌNG] Thêm thư viện threading
from datetime import datetime
from pathlib import Path

# --- Import PyOD ---
try:
    from pyod.models.auto_encoder import AutoEncoder
except ImportError as e:
    logging.warning(f"DEBUG ERROR: {e}")
    logging.warning("PyOD/AutoEncoder not found. Install via 'pip install pyod tensorflow torch'.")
    AutoEncoder = None

from sklearn.preprocessing import StandardScaler

# --- Paths ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MODELS_DIR, USER_MODELS_DIR, ACTIVE_RESPONSE_TRIGGER_THRESHOLD, SENSITIVE_TABLES_DEFAULT, ALLOWED_USERS_FOR_SENSITIVE_DEFAULT
from engine.features import enhance_features_batch
from utils import (
    is_late_night_query, is_potential_large_dump,
    analyze_sensitive_access, check_unusual_user_activity_time,
    is_suspicious_function_used, is_privilege_change, get_normalized_query
)

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [DataProcessor] - %(message)s')
logger = logging.getLogger(__name__)

# --- Constants & Configs ---
PROD_MODEL_PATH = os.path.join(MODELS_DIR, "lgb_uba_production.joblib")
FALLBACK_MODEL_PATH = os.path.join(MODELS_DIR, "lgb_uba_fallback.joblib")
BUFFER_FILE_PATH = os.path.join(MODELS_DIR, "training_buffer_cache.parquet")
CAT_MAP_PATH = os.path.join(MODELS_DIR, "cat_features_map.joblib") 

os.makedirs(MODELS_DIR, exist_ok=True)

# Cấu hình tham số
DEFAULT_ML_CONFIG = {
    "min_train_size": 50,          
    "max_buffer_size": 5000,
    "save_interval_sec": 60,
    
    # AutoEncoder
    "ae_contamination": 0.05,
    "ae_epochs": 20,
    "ae_batch_size": 32,
    "ae_hidden_neurons": [64, 32, 32, 64],
    "ae_verbose": 0,

    # LightGBM
    "lgb_n_estimators": 150,
    "lgb_learning_rate": 0.05,
    "lgb_num_leaves": 31,
    "lgb_max_depth": -1,
    "lgb_scale_pos_weight": 10,
    
    "inference_quantile_threshold": 0.99,
    "inference_min_threshold": 0.75 
}

class ProductionUBAEngine:
    def __init__(self, config=None):
        self.config = config if config else DEFAULT_ML_CONFIG
        
        self.model = None
        self.fallback_model = None
        self.features = None
        self.model_version = "unknown"
        self.last_trained = None
        self.cat_mapping = {}
        
        self.MIN_TRAIN_SIZE = self.config["min_train_size"]
        self.MAX_BUFFER_SIZE = self.config["max_buffer_size"]
        self.SAVE_INTERVAL_SEC = self.config["save_interval_sec"]
        
        self.last_save_time = time.time()
        
        # [NEW] Khóa luồng (Thread Lock) để tránh train chồng chéo
        self.train_lock = threading.Lock()
        self.is_training = False
        
        # Khởi tạo Buffer
        self.training_buffer = self._load_buffer_from_disk()
        self.load_models()

    def _load_buffer_from_disk(self) -> pd.DataFrame:
        if os.path.exists(BUFFER_FILE_PATH):
            try:
                # Cần cài pyarrow: pip install pyarrow
                df = pd.read_parquet(BUFFER_FILE_PATH)
                logger.info(f"🔄 Restored training buffer: {len(df)} rows.")
                return df
            except Exception as e:
                logger.error(f"Failed to load buffer: {e}")
        return pd.DataFrame()

    def _save_buffer_to_disk(self, force=False):
        now = time.time()
        if not force and (now - self.last_save_time < self.SAVE_INTERVAL_SEC):
            return

        try:
            str_cols = ['error_message', 'query', 'normalized_query', 'query_digest', 
                        'user', 'database', 'client_ip', 'connection_type', 'command_type', 
                        'event_name', 'suspicious_func_name', 'privilege_cmd_name', 
                        'unusual_activity_reason']
            
            df_to_save = self.training_buffer.copy()
            for col in str_cols:
                if col in df_to_save.columns:
                    df_to_save[col] = df_to_save[col].astype(str)
            
            df_to_save.to_parquet(BUFFER_FILE_PATH, index=False)
            self.last_save_time = now
        except Exception as e:
            logger.error(f"Failed to persist buffer: {e}")
            
    def load_models(self):
        if os.path.exists(PROD_MODEL_PATH):
            try:
                data = joblib.load(PROD_MODEL_PATH)
                self.model = data['model']
                self.features = data['features']
                if os.path.exists(CAT_MAP_PATH):
                    self.cat_mapping = joblib.load(CAT_MAP_PATH)
                self.model_version = data.get('version', 'v0')
                logger.info(f"Loaded PRODUCTION model v{self.model_version}")
                return
            except Exception as e:
                logger.error(f"Failed load prod model: {e}")

        # Fallback logic nếu cần...
        if not self.training_buffer.empty and len(self.training_buffer) >= self.MIN_TRAIN_SIZE:
            logger.info("No model found. Triggering initial background training...")
            # Kích hoạt thread train ngay khi khởi động nếu đủ data
            self.train_and_update(pd.DataFrame()) 

    def save_production_model(self, model, features):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        version_hash = hashlib.md5(str(features).encode()).hexdigest()[:8]
        version = f"v{timestamp}_{version_hash}"

        data = {
            'model': model,
            'features': features,
            'version': version,
            'trained_at': datetime.now().isoformat(),
            'feature_count': len(features)
        }
        joblib.dump(data, PROD_MODEL_PATH)
        joblib.dump(data, FALLBACK_MODEL_PATH)
        logger.info(f"✅ New PRODUCTION model saved: {version}")

    def _train_thread_target(self):
        """Hàm worker chạy trong thread riêng để huấn luyện model"""
        with self.train_lock:
            self.is_training = True
            try:
                logger.info(f"🏋️ Background Training Started... Buffer size: {len(self.training_buffer)}")
                self._train_core()
                logger.info("🎉 Background Training Finished.")
            except Exception as e:
                logger.error(f"⚠️ Background Training Failed: {e}", exc_info=True)
            finally:
                self.is_training = False

    def _train_core(self):
        """Logic huấn luyện chính (Nặng CPU)"""
        if len(self.training_buffer) < self.MIN_TRAIN_SIZE:
            return False
        
        if AutoEncoder is None:
            return False

        # 1. Feature Selection
        exclude_cols = [
            'timestamp', 'event_id', 'thread_os_id', 'source_dbms', 'client_port', 
            'query', 'normalized_query', 'error_message', 'query_digest', 
            'is_anomaly', 'ml_anomaly_score', 'unusual_activity_reason', 'analysis_type',
            'accessed_tables', 'sensitive_access_info', 'tables_touched',
            'suspicious_func_name', 'privilege_cmd_name', 'error_code' 
        ]

        # Refresh features list based on current buffer
        potential_feats = self.training_buffer.select_dtypes(include=[np.number, 'category', 'object']).columns.tolist()
        self.features = [f for f in potential_feats if f not in exclude_cols]

        X = self.training_buffer[self.features].copy()
        
        # 2. Categorical Handling
        cat_cols = ['user', 'client_ip', 'database', 'command_type']
        current_mapping = {}
        
        for col in X.columns:
            if col in cat_cols or X[col].dtype == 'object':
                X[col] = X[col].astype(str).astype('category')
                current_mapping[col] = X[col].cat.categories.tolist()
            else:
                X[col] = pd.to_numeric(X[col], errors='coerce').fillna(0)
        
        self.cat_mapping = current_mapping
        joblib.dump(self.cat_mapping, CAT_MAP_PATH)

        try:
            # 3. AutoEncoder (Teacher)
            X_ae = X.copy()
            for col in X_ae.columns:
                if X_ae[col].dtype.name == 'category':
                    X_ae[col] = X_ae[col].cat.codes
            
            scaler = StandardScaler()
            X_ae_scaled = scaler.fit_transform(X_ae)

            # 1. Khởi tạo Model (Chỉ cấu trúc mạng)
            ae = AutoEncoder(
                hidden_neuron_list=self.config["ae_hidden_neurons"],
                epoch_num=self.config["ae_epochs"],
                batch_size=self.config["ae_batch_size"],
                contamination=self.config["ae_contamination"],
                verbose=0,
                random_state=42
            )
            
            # 2. Truyền tham số train vào hàm fit()
            ae.fit(X_ae_scaled)
            pseudo_labels = ae.labels_ 
            
            # 4. LightGBM (Student)
            lgb_model = lgb.LGBMClassifier(
                n_estimators=self.config["lgb_n_estimators"],
                learning_rate=self.config["lgb_learning_rate"],
                num_leaves=self.config["lgb_num_leaves"],
                scale_pos_weight=self.config["lgb_scale_pos_weight"],
                random_state=42,
                n_jobs=-1,
                verbose=-1
            )

            lgb_cat_cols = [c for c in cat_cols if c in X.columns]
            lgb_model.fit(X, pseudo_labels, categorical_feature=lgb_cat_cols)

            self.save_production_model(lgb_model, self.features)
            self.model = lgb_model
            return True
            
        except Exception as e:
            logger.error(f"Training core failed: {e}")
            return False

    def train_and_update(self, df_enhanced):
        """
        Cập nhật buffer và kích hoạt thread train nếu cần.
        Hàm này trả về NGAY LẬP TỨC, không chờ train xong.
        """
        if not df_enhanced.empty:
            self.training_buffer = pd.concat([self.training_buffer, df_enhanced], ignore_index=True)
            
            if len(self.training_buffer) > self.MAX_BUFFER_SIZE:
                self.training_buffer = self.training_buffer.iloc[-self.MAX_BUFFER_SIZE:]

            self._save_buffer_to_disk(force=False)

        # Kích hoạt train nếu đủ dữ liệu và chưa có thread nào đang chạy
        if len(self.training_buffer) >= self.MIN_TRAIN_SIZE:
            if not self.is_training:
                # Tạo thread chạy ngầm
                t = threading.Thread(target=self._train_thread_target, daemon=True)
                t.start()
                return True
            else:
                # logger.debug("Skipping trigger: Training already in progress.")
                pass
        return False

# Global instance
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
    if df_logs.empty: return {"all_logs": df_logs}

    # Feature Extraction
    df_enhanced, ML_FEATURES = enhance_features_batch(df_logs.copy())
    df_logs = df_enhanced

    # [NON-BLOCKING] Trigger background training
    uba_engine.train_and_update(df_logs)

    # ML Inference (Scoring) using CURRENT model
    anomalies_ml = pd.DataFrame()
    if uba_engine.model and uba_engine.features:
        try:
            X = df_logs.copy()
            # Ensure columns exist
            for f in uba_engine.features:
                if f not in X.columns: X[f] = 0
            X = X[uba_engine.features]

            # Categorical casting
            for col in X.columns:
                if col in uba_engine.cat_mapping:
                    # Ép kiểu về Category với đúng danh sách đã học
                    known_cats = uba_engine.cat_mapping[col]
                    X[col] = X[col].astype(str).astype(pd.CategoricalDtype(categories=known_cats))
                    # Các giá trị lạ sẽ tự động thành NaN -> Fill 'unknown' nếu 'unknown' có trong list, ko thì fill mode
                    if 'unknown' in known_cats:
                        X[col] = X[col].fillna('unknown')
                    else:
                        # Fallback về category đầu tiên nếu không có unknown
                        X[col] = X[col].fillna(known_cats[0])
                else:
                    X[col] = pd.to_numeric(X[col], errors='coerce').fillna(0)

            scores = uba_engine.model.predict_proba(X)[:, 1]
            df_logs['ml_anomaly_score'] = scores
            
            q_thresh = DEFAULT_ML_CONFIG["inference_quantile_threshold"]
            min_thresh = DEFAULT_ML_CONFIG["inference_min_threshold"]
            threshold = max(np.quantile(scores, q_thresh), min_thresh) if len(scores) > 0 else min_thresh
            
            anomalies_ml = df_logs[scores >= threshold].copy()
        except Exception as e:
            logger.error(f"Inference failed: {e}")
            df_logs['ml_anomaly_score'] = 0.0
    else:
        df_logs['ml_anomaly_score'] = 0.0

    # === RULE-BASED DETECTION ===
    logging.info("Running rule-based detection...")
    rules_config = config_params or {}
    from datetime import time as dt_time
    p_late_night_start_time = dt_time.fromisoformat(rules_config.get('p_late_night_start_time', '00:00:00'))
    p_late_night_end_time = dt_time.fromisoformat(rules_config.get('p_late_night_end_time', '05:00:00'))
    p_known_large_tables = rules_config.get('p_known_large_tables', [])
    p_time_window_minutes = int(rules_config.get('p_time_window_minutes', 5))
    p_min_distinct_tables = int(rules_config.get('p_min_distinct_tables', 3))
    p_sensitive_tables = rules_config.get('p_sensitive_tables', []) or SENSITIVE_TABLES_DEFAULT
    p_allowed_users_sensitive = rules_config.get('p_allowed_users_sensitive', []) or ALLOWED_USERS_FOR_SENSITIVE_DEFAULT
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

    # Rule 3: Multi-table in short window
    import ast

    # Hàm biến chuỗi "['a', 'b']" thành list ['a', 'b']
    def parse_list_safe(x):
        if isinstance(x, list): return x
        if isinstance(x, str):
            try: return ast.literal_eval(x)
            except: return []
        return []

    # Parse cột accessed_tables TRƯỚC khi xử lý
    # Lưu ý: Đảm bảo cột accessed_tables tồn tại trong df_logs
    df_logs['accessed_tables_parsed'] = df_logs['accessed_tables'].apply(parse_list_safe)

    anomalies_multiple_tables_list = []
    window = pd.Timedelta(minutes=p_time_window_minutes)

    for user, group in df_logs.groupby('user', observed=False):
        if len(group) < 2: continue
        group = group.sort_values('timestamp').reset_index(drop=True)
        
        session_tables = set()
        session_queries = []
        start_time = group.iloc[0]['timestamp']

        for _, row in group.iterrows():
            # QUAN TRỌNG: Dùng cột đã parse
            current_tables = row['accessed_tables_parsed']
            if not current_tables: continue
            
            tables = set(current_tables)

            if (row['timestamp'] - start_time) > window:
                # Kiểm tra session cũ
                if len(session_tables) >= p_min_distinct_tables:
                    anomalies_multiple_tables_list.append({
                        'user': user,
                        'start_time': start_time,
                        'end_time': session_queries[-1]['timestamp'],
                        'distinct_tables_count': len(session_tables),
                        'tables_accessed_in_session': sorted(list(session_tables)),
                        'queries_details': session_queries,
                        # Các trường bắt buộc cho AggregateAnomaly
                        'anomaly_type': 'multi_table_access',
                        'severity': 0.8,
                        'reason': f"Accessed {len(session_tables)} distinct tables in short window",
                        'scope': 'session',
                        'database': row.get('database', 'unknown'),
                        'details': {"tables": sorted(list(session_tables))}
                    })
                # Reset
                session_tables = tables
                session_queries = [{'timestamp': row['timestamp'], 'query': row['query']}]
                start_time = row['timestamp']
            else:
                session_tables.update(tables)
                session_queries.append({'timestamp': row['timestamp'], 'query': row['query']})
        
        # Kiểm tra session cuối cùng
        if len(session_tables) >= p_min_distinct_tables:
            anomalies_multiple_tables_list.append({
                'user': user,
                'start_time': start_time,
                'end_time': session_queries[-1]['timestamp'],
                'distinct_tables_count': len(session_tables),
                'tables_accessed_in_session': sorted(list(session_tables)),
                'queries_details': session_queries,
                'anomaly_type': 'multi_table_access',
                'severity': 0.8,
                'reason': f"Accessed {len(session_tables)} distinct tables in short window",
                'scope': 'session',
                'database': session_queries[0].get('database', 'unknown') if session_queries else 'unknown',
                'details': {"tables": sorted(list(session_tables))}
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
    for user, g in df_logs.groupby('user', observed=False):
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


    # # ========================================================
    # # CHUẨN BỊ DỮ LIỆU ACTIVE RESPONSE
    # # ========================================================
    # users_to_lock_list = []
    # # 1. Thu thập tất cả các vi phạm
    # list_of_violation_dfs = []

    # # Các vi phạm dạng "point-in-time"
    # point_in_time_anomalies = [
    #     anomalies_late_night, anomalies_large_dump, anomalies_sensitive_access,
    #     anomalies_unusual_user_time
    # ]
    # for df in point_in_time_anomalies:
    #     if not df.empty and 'user' in df.columns and 'client_ip' in df.columns:
    #         list_of_violation_dfs.append(df[['user', 'client_ip']])

    # # Các vi phạm dạng "session" (multi_table)
    # if not anomalies_multiple_tables_df.empty:
    #     session_violations = []
    #     for _, row in anomalies_multiple_tables_df.iterrows():
    #         user = row['user']
    #         if row['queries_details']:
    #             client_ip = row['queries_details'][0].get('client_ip')
    #             if user and client_ip:
    #                 session_violations.append({'user': user, 'client_ip': client_ip})

    #     if session_violations:
    #         list_of_violation_dfs.append(pd.DataFrame(session_violations))

    # # 2. Tổng hợp, đếm và lọc các user vượt ngưỡng
    # if list_of_violation_dfs:
    #     all_violations_df = pd.concat(list_of_violation_dfs, ignore_index=True)

    #     # Tổng hợp vi phạm THEO USER
    #     user_violation_counts = all_violations_df.groupby('user').size().reset_index(name='total_violation_count')

    #     # Lọc ra các user vượt ngưỡng TỔNG
    #     offenders = user_violation_counts[
    #         user_violation_counts['total_violation_count'] >= ACTIVE_RESPONSE_TRIGGER_THRESHOLD
    #         ]
    #     # Chuyển thành list dictionary để truyền đi
    #     if not offenders.empty:
    #         users_to_lock_list = offenders.to_dict('records')
    # # =============================================================

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
        "normal_activities": normal_activities,
        # "users_to_lock": users_to_lock_list  # List [{'user': 'abc', 'total_violation_count': 5}]
    }

    logging.info(f"Processing complete. Total anomalies: {len(anomalous_indices)} / {len(df_logs)}")
    return results


# --------------------------- Model I/O ---------------------------
def save_model_and_scaler(model, scaler, path):
    joblib.dump({'model': model, 'scaler': scaler}, path)

def load_model_and_scaler(path):
    if os.path.exists(path):
        data = joblib.load(path)
        return data['model'], data['scaler']
    return None, None