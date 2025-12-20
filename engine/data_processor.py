# UBA-PLATFORM/engine/data_processor.py
"""
================================================================================
MODULE XỬ LÝ DỮ LIỆU CHÍNH (REFACTORED FOR CASCADING LOGIC)
================================================================================
Luồng xử lý:
1. Feature Engineering
2. Rule-Based Detection (Lọc)
3. ML Detection (Chạy trên log sạch)
4. Active Response (Tổng hợp vi phạm & Đề xuất khóa user)
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
import json
import threading
from datetime import datetime
from pathlib import Path
from redis import Redis

# --- Tắt log rác của thư viện parser SQL ---
logging.getLogger('sqlglot').setLevel(logging.ERROR)

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
from config import MODELS_DIR, USER_MODELS_DIR, REDIS_URL
from engine.features import enhance_features_batch
from engine.config_manager import load_config
from utils import (
    get_normalized_query,
    check_access_anomalies, check_insider_threats, check_technical_attacks,
    check_data_destruction, check_multi_table_anomalies,
    update_behavior_redis, check_behavior_redis
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

# Cấu hình tham số ML mặc định
DEFAULT_ML_CONFIG = {
    "min_train_size": 50,
    "max_buffer_size": 5000,
    "save_interval_sec": 60,

    # AutoEncoder (Tuning based on 10179 rows)
    "ae_contamination": 0.039,  # Tỷ lệ bất thường dự kiến
    "ae_epochs": 20,
    "ae_batch_size": 64,
    "ae_hidden_neurons": [64, 32, 32, 64],
    "ae_verbose": 0,

    # LightGBM (Tuning for Imbalance ratio 1:30.0)
    "lgb_n_estimators": 200,
    "lgb_learning_rate": 0.05,
    "lgb_num_leaves": 31,
    "lgb_max_depth": -1,
    "lgb_scale_pos_weight": 30.03,  # Trọng số cho lớp bất thường

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

        # Khóa luồng (Thread Lock)
        self.train_lock = threading.Lock()
        self.is_training = False

        # Khởi tạo Buffer
        self.training_buffer = self._load_buffer_from_disk()
        self.load_models()

    def _load_buffer_from_disk(self) -> pd.DataFrame:
        if os.path.exists(BUFFER_FILE_PATH):
            try:
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
            str_cols = ['error_message', 'query', 'normalized_query',
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

        if not self.training_buffer.empty and len(self.training_buffer) >= self.MIN_TRAIN_SIZE:
            logger.info("No model found. Triggering initial background training...")
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
        if len(self.training_buffer) < self.MIN_TRAIN_SIZE:
            return False

        if AutoEncoder is None:
            return False

        exclude_cols = [
            'timestamp', 'event_id',
            'query', 'normalized_query', 'error_message',
            'is_anomaly', 'ml_anomaly_score', 'unusual_activity_reason', 'analysis_type',
            'accessed_tables', 'sensitive_access_info', 'tables_touched',
            'suspicious_func_name', 'privilege_cmd_name', 'error_code', 'behavior_group'
        ]

        potential_feats = self.training_buffer.select_dtypes(include=[np.number, 'category', 'object']).columns.tolist()
        self.features = [f for f in potential_feats if f not in exclude_cols]

        X = self.training_buffer[self.features].copy()

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
            X_ae = X.copy()
            for col in X_ae.columns:
                if X_ae[col].dtype.name == 'category':
                    X_ae[col] = X_ae[col].cat.codes

            scaler = StandardScaler()
            X_ae_scaled = scaler.fit_transform(X_ae)

            ae = AutoEncoder(
                hidden_neuron_list=self.config["ae_hidden_neurons"],
                epoch_num=self.config["ae_epochs"],
                batch_size=self.config["ae_batch_size"],
                contamination=self.config["ae_contamination"],
                verbose=0,
                random_state=42
            )

            ae.fit(X_ae_scaled)
            pseudo_labels = ae.labels_

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
        if not df_enhanced.empty:
            self.training_buffer = pd.concat([self.training_buffer, df_enhanced], ignore_index=True)

            if len(self.training_buffer) > self.MAX_BUFFER_SIZE:
                self.training_buffer = self.training_buffer.iloc[-self.MAX_BUFFER_SIZE:]

            self._save_buffer_to_disk(force=False)

        if len(self.training_buffer) >= self.MIN_TRAIN_SIZE:
            if not self.is_training:
                t = threading.Thread(target=self._train_thread_target, daemon=True)
                t.start()
                return True
        return False


# Global instance
uba_engine = ProductionUBAEngine()


def process_rule_results(df_logs, anomalies_dict, group_name):
    """
    Chuyển đổi Dict {RuleName: [indexes]} thành DataFrame.
    Gán cột 'specific_rule' để biết chính xác lỗi gì.
    """
    if not anomalies_dict:
        return pd.DataFrame()

    all_indices = set()
    for indices in anomalies_dict.values():
        all_indices.update(indices)

    if not all_indices:
        return pd.DataFrame()

    # Lấy các dòng vi phạm
    df_result = df_logs.loc[list(all_indices)].copy()

    # Gán nhãn nhóm
    df_result['behavior_group'] = group_name
    df_result['specific_rule'] = ''

    # Gán nhãn chi tiết (nối chuỗi nếu dính nhiều rule)
    for rule_name, indices in anomalies_dict.items():
        mask = df_result.index.isin(indices)
        # Nếu đã có text rồi thì thêm dấu chấm phẩy
        df_result.loc[mask & (df_result['specific_rule'] != ''), 'specific_rule'] += f"; {rule_name}"
        # Nếu chưa có thì gán mới
        df_result.loc[mask & (df_result['specific_rule'] == ''), 'specific_rule'] = rule_name

    return df_result


# Hàm đọc config động
def load_engine_config_dynamic():
    try:
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'engine_config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}


def _aggregate_multi_table_alerts(df_rule_multi):
    """
    Hàm hỗ trợ: Gom nhóm các log vi phạm Multi-table thành các Session cảnh báo.
    Input: DataFrame chứa các log vi phạm (df_rule_multi)
    Output: DataFrame chứa thông tin tổng hợp (Session level)
    """
    if df_rule_multi.empty:
        return pd.DataFrame()

    aggregated_data = []

    # Gom nhóm theo User và cửa sổ thời gian (ví dụ: mỗi 5 phút là 1 session tấn công)
    # Lưu ý: Cần sort trước khi group
    df_sorted = df_rule_multi.sort_values('timestamp')

    # Sử dụng Grouper 5 phút để tách các đợt tấn công khác nhau của cùng 1 user
    grouped = df_sorted.groupby(['user', pd.Grouper(key='timestamp', freq='5Min')], observed=False)

    for (user, time_window), group in grouped:
        # Nếu nhóm này ít hơn 2 bảng thì có thể không đáng gọi là session tấn công lớn (tùy logic)
        # Nhưng vì rule gốc đã lọc rồi, nên ta cứ aggregate hết.
        
        client_ip = 'unknown'
        if 'client_ip' in group.columns and not group['client_ip'].empty:
            client_ip = group['client_ip'].iloc[0]

        # Lấy danh sách bảng bị truy cập trong session này
        # (Cần trích xuất lại tên bảng từ query vì trong df_rule_multi có thể chưa có cột clean list)
        from engine.utils import get_tables_with_sqlglot  # Import hàm này

        all_tables = set()
        queries_details = []

        for _, row in group.iterrows():
            # Lấy tên bảng (Dùng lại hàm utils hoặc regex đơn giản để tối ưu tốc độ)
            tbls = get_tables_with_sqlglot(row['query'])
            all_tables.update(tbls)

            queries_details.append({
                "timestamp": row['timestamp'].isoformat() if pd.notna(row['timestamp']) else "",
                "query": row['query']
            })

        if len(all_tables) < 2:
            continue

        aggregated_data.append({
            "user": user,
            "client_ip": client_ip,
            "start_time": group['timestamp'].min(),
            "end_time": group['timestamp'].max(),
            "tables_accessed_in_session": list(all_tables),
            "distinct_tables_count": len(all_tables),
            "queries_details": queries_details,
            "anomaly_type": "multi_table_access",
            "behavior_group": "MULTI_TABLE_ACCESS"
        })

    return pd.DataFrame(aggregated_data)


def load_and_process_data(input_df: pd.DataFrame, config_params: dict) -> dict:
    """
    Hàm xử lý chính: RULES -> FILTER -> ML
    """
    global uba_engine

    if input_df is None or input_df.empty:
        return {"all_logs": pd.DataFrame(), "anomalies_ml": pd.DataFrame()}

    # 1. LOAD CONFIG (NGAY ĐẦU HÀM ĐỂ TRÁNH LỖI VARIABLE SCOPE)
    full_config = load_engine_config_dynamic()
    rules_json_config = full_config.get('security_rules', {})

    # Merge config params (Ưu tiên config từ file JSON)
    combined_rules_config = {**(config_params or {}), **rules_json_config}

    df_logs = input_df.copy()

    # 2. PREPROCESSING & FEATURES (Làm trên toàn bộ batch để đảm bảo tính nhất quán)
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

    # ==========================================================================
    # 3. RULE-BASED DETECTION (CHẠY TRƯỚC ĐỂ LỌC)
    # ==========================================================================
    logging.info("--- Phase 1: Running Rules ---")

    # --- A. Các Rule Mới (Advanced) ---
    try:
        dict_access = check_access_anomalies(df_logs, combined_rules_config)
        dict_insider = check_insider_threats(df_logs, combined_rules_config)
        dict_technical = check_technical_attacks(df_logs, combined_rules_config)
        dict_destruction = check_data_destruction(df_logs, combined_rules_config)
        dict_multi_table = check_multi_table_anomalies(df_logs, combined_rules_config)

        # Chuyển Dict thành DataFrame (Gán nhãn specific_rule)
        df_rule_access = process_rule_results(df_logs, dict_access, 'ACCESS_ANOMALY')
        df_rule_insider = process_rule_results(df_logs, dict_insider, 'INSIDER_THREAT')
        df_rule_technical = process_rule_results(df_logs, dict_technical, 'TECHNICAL_ATTACK')
        df_rule_destruction = process_rule_results(df_logs, dict_destruction, 'DATA_DESTRUCTION')
        df_rule_multi = process_rule_results(df_logs, dict_multi_table, 'MULTI_TABLE_ACCESS')
    except Exception as e:
        logging.error(f"Rule Engine Error: {e}", exc_info=True)
        df_rule_access = df_rule_insider = df_rule_technical = df_rule_destruction = df_rule_multi = pd.DataFrame()

    anomalies_user_time = pd.DataFrame()
    # Tính profile đơn giản
    try:
        redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
        
        # 1. Lấy ngưỡng từ Config (Mặc định là 5 nếu không tìm thấy)
        profile_threshold = combined_rules_config.get('thresholds', {}).get('min_occurrences_threshold', 5)

        # 2. KIỂM TRA (DETECTION)
        current_indices_to_check = df_logs.index.difference(
            set(df_rule_access.index).union(df_rule_insider.index).union(df_rule_technical.index)
        )
        df_to_check = df_logs.loc[current_indices_to_check]
        
        if not df_to_check.empty:
            # Truyền profile_threshold vào hàm check
            bad_indices = check_behavior_redis(redis_client, df_to_check, min_threshold=profile_threshold)
            
            if bad_indices:
                anomalies_user_time = df_logs.loc[bad_indices].copy()
                anomalies_user_time['behavior_group'] = 'UNUSUAL_BEHAVIOR'
                anomalies_user_time['specific_rule'] = 'Rare Access Time (Profile Mismatch)'
                
                anomalies_user_time['unusual_activity_reason'] = anomalies_user_time['timestamp'].apply(
                    lambda x: f"Access at {x.hour}h is rare (< {profile_threshold} times in history)"
                )

        # 3. HỌC (LEARNING)
        df_to_learn = df_logs[~df_logs.index.isin(df_rule_technical.index)]
        update_behavior_redis(redis_client, df_to_learn)
        
        redis_client.close()
        
    except Exception as e:
        logging.error(f"Redis Behavioral Profiling Failed: {e}")
        # Fallback: Nếu lỗi Redis, bỏ qua rule này, không làm crash hệ thống
        anomalies_user_time = pd.DataFrame()

    # GOM TẤT CẢ LOG VI PHẠM
    rule_caught_indices = set(
        df_rule_access.index.tolist() +
        df_rule_insider.index.tolist() +
        df_rule_technical.index.tolist() +
        df_rule_destruction.index.tolist() +
        df_rule_multi.index.tolist() +
        anomalies_user_time.index.tolist()
    )

    logging.info(f"Phase 1 Complete. Rules caught {len(rule_caught_indices)} logs.")

    # ==========================================================================
    # 4. FILTERING & ML DETECTION (CHẠY TRÊN LOG SẠCH)
    # ==========================================================================

    # Lọc bỏ log đã bị Rule bắt
    df_for_ml = df_logs[~df_logs.index.isin(rule_caught_indices)].copy()
    anomalies_ml = pd.DataFrame()

    if not df_for_ml.empty:
        logging.info(f"--- Phase 2: Running ML on {len(df_for_ml)} clean logs ---")

        # 1. Train Background (Chỉ học trên log sạch -> Model chuẩn hơn)
        uba_engine.train_and_update(df_for_ml)

        # 2. Predict (Tìm unknown anomalies)
        if uba_engine.model and uba_engine.features:
            try:
                X = df_for_ml.copy()
                for f in uba_engine.features:
                    if f not in X.columns: X[f] = 0
                X = X[uba_engine.features]

                # Categorical handling
                for col in X.columns:
                    if col in uba_engine.cat_mapping:
                        known_cats = uba_engine.cat_mapping[col]
                        X[col] = X[col].astype(str).astype(pd.CategoricalDtype(categories=known_cats))
                        if 'unknown' in known_cats:
                            X[col] = X[col].fillna('unknown')
                        else:
                            X[col] = X[col].fillna(known_cats[0])
                    else:
                        X[col] = pd.to_numeric(X[col], errors='coerce').fillna(0)

                scores = uba_engine.model.predict_proba(X)[:, 1]
                df_for_ml['ml_anomaly_score'] = scores

                # Ngưỡng động
                q_thresh = DEFAULT_ML_CONFIG["inference_quantile_threshold"]
                min_thresh = DEFAULT_ML_CONFIG["inference_min_threshold"]
                threshold = max(np.quantile(scores, q_thresh), min_thresh) if len(scores) > 0 else min_thresh

                # Chỉ lấy những cái vượt ngưỡng
                anomalies_ml = df_for_ml[scores >= threshold].copy()
                if not anomalies_ml.empty:
                    anomalies_ml['behavior_group'] = 'ML_DETECTED'
            except Exception as e:
                logger.error(f"Inference failed: {e}")
                df_logs['ml_anomaly_score'] = 0.0
    else:
        logging.info("All logs caught by Rules. Skipping ML.")

    # ========================================================
    # TÍNH TOÁN AGGREGATE
    # ========================================================
    anomalies_multi_table_agg = _aggregate_multi_table_alerts(df_rule_multi)

    # ========================================================
    # CHUẨN BỊ DỮ LIỆU ACTIVE RESPONSE
    # ========================================================
    users_to_lock_list = []

    # 1. Thu thập tất cả các vi phạm từ CÁC NHÓM
    list_of_violation_dfs = []
    
    ar_config = full_config.get("active_response_config", {})
    threshold = ar_config.get("max_violation_threshold", 3)

    # Danh sách các DataFrame chứa vi phạm (Point-in-time)
    violation_sources = [
        df_rule_access,
        df_rule_insider,
        df_rule_technical,
        df_rule_destruction
    ]

    for df in violation_sources:
        if not df.empty and 'user' in df.columns:
            # Lấy thêm client_ip để xử lý
            cols = ['user']
            if 'client_ip' in df.columns: cols.append('client_ip')
            list_of_violation_dfs.append(df[cols].copy())

    # Xử lý riêng cho Multi-table (Session based)
    if not anomalies_multi_table_agg.empty and 'user' in anomalies_multi_table_agg.columns:
        temp_df = anomalies_multi_table_agg[['user']].copy()
        if 'client_ip' not in temp_df.columns: temp_df['client_ip'] = 'unknown'
        list_of_violation_dfs.append(temp_df)

    # 2. USER VI PHẠM NGHIÊM TRỌNG (ZERO TOLERANCE)
    critical_users = set()

    # Nhóm Technical Attacks
    if not df_rule_technical.empty and 'user' in df_rule_technical.columns:
        critical_users.update(df_rule_technical['user'].unique())

    # Nhóm Data Destruction
    if not df_rule_destruction.empty and 'user' in df_rule_destruction.columns:
        critical_users.update(df_rule_destruction['user'].unique())


    # 3. TỔNG HỢP VÀ RA QUYẾT ĐỊNH
    if list_of_violation_dfs:
        all_violations_df = pd.concat(list_of_violation_dfs, ignore_index=True)

        # Đếm tổng số vi phạm của mỗi user
        user_violation_counts = all_violations_df.groupby('user', observed=False).size().reset_index(
            name='total_violation_count')

        # --- LOGIC QUYẾT ĐỊNH KHÓA ---
        offenders = user_violation_counts[
            (user_violation_counts['total_violation_count'] >= threshold) |
            (user_violation_counts['user'].isin(critical_users))
            ].copy()
        # Đánh dấu lý do
        def get_lock_reason(row):
            if row['user'] in critical_users:
                return f"CRITICAL VIOLATION (Zero Tolerance) - Count: {row['total_violation_count']}"
            return f"Threshold Exceeded - Count: {row['total_violation_count']}"

        if not offenders.empty:
            # Thêm cột lý do cụ thể
            offenders['lock_reason'] = offenders.apply(get_lock_reason, axis=1)

            # Chuyển thành list dictionary
            users_to_lock_list = offenders.to_dict('records')


    # ========================================================
    # CẬP NHẬT LẠI THÔNG TIN RULE VÀO LOG TỔNG (df_logs)
    # Để Log Explorer hiển thị được tên Rule
    # ========================================================
    
    # 1. Danh sách các DataFrame chứa kết quả Rule
    detection_sources = [
        df_rule_access,
        df_rule_insider,
        df_rule_technical,
        df_rule_destruction,
        df_rule_multi,
        anomalies_user_time, 
        anomalies_ml
    ]

    # 2. Đảm bảo cột ghi lý do tồn tại trong df_logs
    if 'unusual_activity_reason' not in df_logs.columns:
        df_logs['unusual_activity_reason'] = None
    
    # 3. Duyệt qua từng nguồn phát hiện và cập nhật vào df_logs
    for source_df in detection_sources:
        if source_df is not None and not source_df.empty:
            # Chỉ lấy các index có trong df_logs (để an toàn)
            common_indices = source_df.index.intersection(df_logs.index)
            
            if not common_indices.empty:
                # Lấy tên rule từ cột 'specific_rule' hoặc 'anomaly_type'
                # Ưu tiên 'specific_rule' (do hàm process_rule_results tạo ra)
                if 'specific_rule' in source_df.columns:
                    rule_series = source_df.loc[common_indices, 'specific_rule']
                elif 'behavior_group' in source_df.columns:
                    rule_series = source_df.loc[common_indices, 'behavior_group']
                else:
                    rule_series = pd.Series("Detected by System", index=common_indices)

                # Cập nhật vào df_logs
                # Lưu ý: Nếu 1 log dính nhiều rule, code này sẽ ghi đè rule cuối cùng (hoặc bạn có thể nối chuỗi)
                df_logs.loc[common_indices, 'unusual_activity_reason'] = rule_series
                df_logs.loc[common_indices, 'is_anomaly'] = 1

    # ========================================================
    # TỔNG HỢP KẾT QUẢ CUỐI CÙNG
    # ========================================================
    # Tổng hợp toàn bộ index bất thường (Rule + ML)
    all_anomalous_indices = rule_caught_indices.union(set(anomalies_ml.index.tolist()))

    # Log bình thường = Không dính Rule VÀ Không dính ML
    normal_activities = df_logs[~df_logs.index.isin(all_anomalous_indices)].copy()

    # Mapping kết quả trả về
    results = {
        "all_logs": df_logs,

        # ML
        "anomalies_ml": anomalies_ml,

        # Rule
        "rule_access": df_rule_access,
        "rule_insider": df_rule_insider,
        "rule_technical": df_rule_technical,
        "rule_destruction": df_rule_destruction,
        "rule_behavior_profile": anomalies_user_time,

        # Rule (Session Level)
        "rule_multi_table": anomalies_multi_table_agg,

        # Active Response Data
        "users_to_lock": users_to_lock_list,  # List [{'user': 'abc', 'total_violation_count': 5}]

        "normal_activities": normal_activities,
    }

    logging.info(f"Processing complete. Rules: {len(rule_caught_indices)}, ML: {len(anomalies_ml)}")
    return results


# --------------------------- Model I/O ---------------------------
def save_model_and_scaler(model, scaler, path):
    joblib.dump({'model': model, 'scaler': scaler}, path)


def load_model_and_scaler(path):
    if os.path.exists(path):
        data = joblib.load(path)
        return data['model'], data['scaler']
    return None, None