"""
================================================================================
MODULE XỬ LÝ DỮ LIỆU CHÍNH
================================================================================
Tích hợp:
- Feature engineering cấp nghiên cứu (sqlglot + behavioral z-score + entropy)
- Semi-supervised ML: Isolation Forest → pseudo-label → LightGBM (self-training)
- Persistent Buffering: Lưu cache training xuống đĩa để không mất dữ liệu khi restart
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
import time

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
BUFFER_FILE_PATH = os.path.join(MODELS_DIR, "training_buffer_cache.parquet")

# File để lưu danh sách category lúc train
CAT_MAP_PATH = os.path.join(MODELS_DIR, "cat_features_map.joblib") 

os.makedirs(MODELS_DIR, exist_ok=True)


class ProductionUBAEngine:
    def __init__(self):
        self.model = None
        self.fallback_model = None
        self.features = None
        self.model_version = "unknown"
        self.last_trained = None
        self.cat_mapping = {}
        
        # Cấu hình Training
        self.MIN_TRAIN_SIZE = 1      # Train ngay khi có 1 dòng
        self.MAX_BUFFER_SIZE = 5000   # Giữ tối đa 5000 dòng để train (Sliding Window)
        
        # Biến để kiểm soát tần suất ghi đĩa
        self.last_save_time = time.time()
        self.SAVE_INTERVAL_SEC = 60 # Chỉ ghi xuống đĩa mỗi 60s hoặc khi buffer đầy
        
        # Khởi tạo Buffer từ đĩa (Persistence)
        self.training_buffer = self._load_buffer_from_disk()
        self.load_models()

    def _load_buffer_from_disk(self) -> pd.DataFrame:
        """Load dữ liệu cũ từ đĩa lên RAM khi khởi động lại"""
        if os.path.exists(BUFFER_FILE_PATH):
            try:
                df = pd.read_parquet(BUFFER_FILE_PATH)
                logger.info(f"🔄 Restored training buffer from disk: {len(df)} rows.")
                return df
            except Exception as e:
                logger.error(f"Failed to load buffer file: {e}")
        return pd.DataFrame()

    def _save_buffer_to_disk(self, force=False):
        """Lưu Buffer xuống đĩa với cơ chế Rate Limit"""
        now = time.time()
        # Chỉ ghi nếu force=True HOẶC đã quá 60s HOẶC buffer > 1000 dòng mới
        if not force and (now - self.last_save_time < self.SAVE_INTERVAL_SEC):
            return

        try:
            # Ép kiểu các cột string dễ gây lỗi trước khi lưu
            str_cols = ['error_message', 'query', 'normalized_query', 'query_digest', 
                        'user', 'database', 'client_ip', 'connection_type', 'command_type', 
                        'event_name', 'suspicious_func_name', 'privilege_cmd_name', 
                        'unusual_activity_reason']
            
            df_to_save = self.training_buffer.copy()
            for col in str_cols:
                if col in df_to_save.columns:
                    df_to_save[col] = df_to_save[col].astype(str)
            
            # Parquet ghi rất nhanh
            df_to_save.to_parquet(BUFFER_FILE_PATH, index=False)
            self.last_save_time = now
            # logging.info("Persistent buffer saved to disk.")
        except Exception as e:
            logger.error(f"Failed to persist buffer to disk: {e}")
            
    def load_models(self):
        """Load production → fallback → train new"""
        if os.path.exists(PROD_MODEL_PATH):
            try:
                data = joblib.load(PROD_MODEL_PATH)
                self.model = data['model']
                self.features = data['features']
                if os.path.exists(CAT_MAP_PATH):
                	self.cat_mapping = joblib.load(CAT_MAP_PATH)
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

        logger.warning("No model found.")
        # Nếu không có model nhưng có buffer cũ -> Train ngay lập tức
        if not self.training_buffer.empty and len(self.training_buffer) >= self.MIN_TRAIN_SIZE:
            logger.info("No model found but buffer exists. Training immediately...")
            self._train_core()

    def save_production_model(self, model, features):
        """Atomic save with versioning"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        version_hash = hashlib.md5(str(features).encode()).hexdigest()[:8]
        
        # Tự động tăng version dựa trên số file hiện có
        try:
            existing = [f for f in os.listdir(MODELS_DIR) if f.startswith('lgb_uba_')]
            ver_num = len(existing)
        except: ver_num = 0
            
        version = f"v{ver_num}_{version_hash}"

        data = {
            'model': model,
            'features': features,
            'version': version,
            'trained_at': datetime.now().isoformat(),
            'feature_count': len(features)
        }

        # Save new production
        joblib.dump(data, PROD_MODEL_PATH)
        # Luôn cập nhật fallback để an toàn
        joblib.dump(data, FALLBACK_MODEL_PATH)
        logger.info(f"✅ New PRODUCTION model saved: {version}")

    def _train_core(self):
        """Hàm train nội bộ - Tách ra để tái sử dụng"""
        if len(self.training_buffer) < self.MIN_TRAIN_SIZE:
            return False
        
        # Danh sách các cột KHÔNG dùng cho Machine Learning
        exclude_cols = [
            # 1. Định danh & Thời gian (Metadata)
            'timestamp', 
            'event_id', 
            'thread_os_id',
            'source_dbms',      # Hằng số (luôn là MySQL)
            'client_port',      # Port client thay đổi ngẫu nhiên (Ephemeral port)
            
            # 2. Văn bản thô (Raw Text) - Model không hiểu được
            'query', 
            'normalized_query', 
            'error_message',    # Nội dung lỗi biến thiên quá nhiều
            'query_digest',     # Hash chuỗi (Cardinallity quá cao, dễ gây overfit nếu data ít)
            
            # 3. Kết quả đầu ra (Label Leakage) - Cấm kỵ đưa vào input
            'is_anomaly', 
            'ml_anomaly_score', 
            'unusual_activity_reason',
            'analysis_type',
            
            # 4. Các cột phụ trợ / JSON
            'accessed_tables', 
            'sensitive_access_info', 
            'tables_touched',
            'suspicious_func_name', 
            'privilege_cmd_name',
            
            # 5. Mã lỗi cụ thể (Optional)
            # Nên bỏ error_code vì nó là dạng Category có quá nhiều giá trị (null, 1064, 1146...)
            # Ta đã có 'has_error' và 'error_count' đại diện tốt hơn.
            'error_code' 
        ]

        # Nếu chưa có feature list, tự động chọn
        if not self.features:
            # Lấy tất cả cột số và category
            potential_feats = self.training_buffer.select_dtypes(include=[np.number, 'category', 'object']).columns.tolist()
            
            # Lọc bỏ các cột trong blacklist
            self.features = [f for f in potential_feats if f not in exclude_cols]
            
            # Log ra để kiểm tra xem Model đang dùng feature gì
            logger.info(f"🚀 Model Features ({len(self.features)}): {self.features}")

        # Tạo X cho LightGBM (giữ nguyên category)
        X = self.training_buffer[self.features].copy()
        
        # Chuẩn hóa Category khi Train
        cat_cols = ['user', 'client_ip', 'database', 'command_type']
        current_mapping = {}
        
        # Xử lý NaN và kiểu dữ liệu cho X
        for col in X.columns:
            if col in cat_cols or X[col].dtype == 'object':
                X[col] = X[col].astype(str).astype('category')
                # Lưu lại danh sách category đã biết
                current_mapping[col] = X[col].cat.categories.tolist()
            else:
                X[col] = X[col].fillna(0)
        
        # Lưu mapping
        self.cat_mapping = current_mapping
        joblib.dump(self.cat_mapping, CAT_MAP_PATH)

        try:
            # Tạo bản sao X_iso được mã hóa số học cho Isolation Forest
            X_iso = X.copy()
            for col in X_iso.columns:
                if X_iso[col].dtype.name == 'category':
                    # Chuyển chuỗi thành số (0, 1, 2...)
                    X_iso[col] = X_iso[col].cat.codes

            # 1. Pipeline: Isolation Forest (Dùng X_iso toàn số)
            iso = IsolationForest(contamination=0.05, random_state=42, n_jobs=-1)
            iso.fit(X_iso)
            high_conf_anoms_mask = (iso.predict(X_iso) == -1)
            
            # 2. Tạo tập train cho Supervised Model (LightGBM)
            # Dùng X gốc (có category) vì LightGBM xử lý tốt hơn
            X_train = pd.concat([X, X[high_conf_anoms_mask]])
            y_train = np.concatenate([np.zeros(len(X)), np.ones(sum(high_conf_anoms_mask))])

            # 3. Train LightGBM
            new_model = lgb.LGBMClassifier(
                n_estimators=100,
                learning_rate=0.05,
                max_depth=5,
                num_leaves=31,
                scale_pos_weight=10,
                random_state=42,
                n_jobs=-1,
                verbose=-1
            )

            cat_cols = [c for c in ['user', 'client_ip', 'database', 'command_type'] if c in X.columns]
            new_model.fit(X_train, y_train, categorical_feature=cat_cols)

            self.save_production_model(new_model, self.features)
            self.model = new_model
            return True
            
        except Exception as e:
            logger.error(f"Training core failed: {e}", exc_info=True)
            return False

    def train_and_update(self, df_enhanced):
        """
        Public method gọi khi có dữ liệu mới: Tích lũy -> Lưu -> Train
        """
        if df_enhanced.empty:
            return False

        # 1. Cộng dồn vào RAM
        self.training_buffer = pd.concat([self.training_buffer, df_enhanced], ignore_index=True)
        
        # 2. Cắt bớt nếu quá lớn (Sliding Window)
        if len(self.training_buffer) > self.MAX_BUFFER_SIZE:
            self.training_buffer = self.training_buffer.iloc[-self.MAX_BUFFER_SIZE:]

        # 3. Lưu ngay xuống đĩa (Persistence)
        self._save_buffer_to_disk(force=False)

        # 4. Train ngay lập tức nếu đủ dữ liệu
        logger.info(f"Training triggered. Buffer size: {len(self.training_buffer)}")
        return self._train_core()


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

    # ADVANCED FEATURES (Features.py enhancement)
    df_enhanced, ML_FEATURES = enhance_features_batch(df_logs.copy())
    df_logs = df_enhanced

    # [UPDATE] Always update buffer and attempt training
    # Không cần chờ 10000 dòng nữa, buffer sẽ tự lo
    try:
        uba_engine.train_and_update(df_logs)
    except Exception as e:
        logger.error(f"Auto-retrain trigger failed: {e}")

    # ML Scoring
    if uba_engine.model and uba_engine.features:
        try:
            # 1. Chuẩn bị X với đúng các cột features mô hình cần
            X = df_logs.copy()
            for f in uba_engine.features:
                if f not in X.columns:
                    X[f] = 0
            X = X[uba_engine.features]

            # 2. [FIX] Ép kiểu Category tường minh cho giống lúc Train
            cat_cols = ['user', 'client_ip', 'database', 'command_type']
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
                    # Các cột số thì ép về float/int và điền 0
                    X[col] = pd.to_numeric(X[col], errors='coerce').fillna(0)

            # 3. Dự đoán
            scores = uba_engine.model.predict_proba(X)[:, 1]
            df_logs['ml_anomaly_score'] = scores
            
            # Ngưỡng động: Lấy top 1% hoặc > 0.75
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
    for user, group in df_logs.groupby('user', observed=False):
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


# --------------------------- Model I/O ---------------------------
def save_model_and_scaler(model, scaler, path):
    joblib.dump({'model': model, 'scaler': scaler}, path)

def load_model_and_scaler(path):
    if os.path.exists(path):
        data = joblib.load(path)
        return data['model'], data['scaler']
    return None, None