# UBA-PLATFORM/engine/data_processor.py
"""
================================================================================
MODULE XỬ LÝ DỮ LIỆU CHÍNH (REFACTORED WITH PYTORCH LSTM)
================================================================================
Luồng xử lý:
1. Feature Engineering
2. Rule-Based Detection (Lọc)
3. ML Detection (Chạy trên log sạch - Hybrid LSTM + LightGBM)
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

# --- Import PyTorch ---
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

# --- Tắt log rác ---
logging.getLogger('sqlglot').setLevel(logging.ERROR)

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

# ==============================================================================
# 1. CLASSES LSTM AUTOENCODER (PYTORCH)
# ==============================================================================

class LSTMAE_Module(nn.Module):
    def __init__(self, input_dim, hidden_dim):
        super(LSTMAE_Module, self).__init__()
        # Encoder: Nén chuỗi đầu vào (batch, seq, feat) -> (batch, 1, hidden)
        self.encoder = nn.LSTM(input_dim, hidden_dim, batch_first=True)
        # Decoder: Tái tạo lại chuỗi
        self.decoder = nn.LSTM(hidden_dim, input_dim, batch_first=True)

    def forward(self, x):
        # x shape: (batch, seq_len, input_dim)
        _, (hidden, _) = self.encoder(x)
        # Hidden state shape: (1, batch, hidden_dim)
        # Ta cần lặp lại hidden state này cho mỗi bước thời gian để decoder bung ra
        # Permute -> (batch, 1, hidden) -> Repeat -> (batch, seq_len, hidden)
        hidden_expanded = hidden.permute(1, 0, 2).repeat(1, x.size(1), 1)
        
        # Output shape: (batch, seq_len, input_dim)
        output, _ = self.decoder(hidden_expanded)
        return output

class DeepLSTMAutoEncoder:
    """
    Wrapper class để giả lập hành vi giống PyOD (fit, predict, labels_)
    nhưng chạy bằng PyTorch LSTM bên dưới.
    """
    def __init__(self, sequence_length=5, hidden_dim=64, epochs=10, contamination=0.05, batch_size=64):
        self.seq_len = sequence_length
        self.hidden_dim = hidden_dim
        self.epochs = epochs
        self.contamination = contamination
        self.batch_size = batch_size
        self.model = None
        self.labels_ = None # Sẽ chứa 0 (bình thường) hoặc 1 (bất thường)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.threshold_ = 0

    def _create_sequences(self, X):
        """
        Chuyển đổi dữ liệu 2D (Sample, Feat) thành 3D (Sample, Seq, Feat)
        Sử dụng Sliding Window tối ưu tốc độ bằng numpy strides.
        """
        if len(X) <= self.seq_len:
            return np.array([])
        
        # Đảm bảo dữ liệu liên tục trong bộ nhớ để tránh lỗi as_strided
        X = np.ascontiguousarray(X)

        num_samples = len(X) - self.seq_len + 1
        sub_shape = (num_samples, self.seq_len, X.shape[1])
        sub_strides = (X.strides[0], X.strides[0], X.strides[1])
        
        Xs = np.lib.stride_tricks.as_strided(
            X,
            shape=sub_shape,
            strides=sub_strides,
            writeable=False
        )
        
        # Copy ra mảng mới để tách biệt bộ nhớ, tránh side-effect khi convert sang Tensor
        return Xs.copy()

    def fit(self, X):
        # 1. Chuẩn bị dữ liệu chuỗi
        # X ở đây kỳ vọng đã được sort theo User & Time bên ngoài
        X_seq = self._create_sequences(X)
        
        if len(X_seq) == 0:
            # Fallback nếu dữ liệu quá ít: Gán nhãn 0 hết
            self.labels_ = np.zeros(len(X))
            return self

        self.input_dim = X_seq.shape[2]
        
        # Chuyển sang Tensor
        dataset = TensorDataset(torch.tensor(X_seq, dtype=torch.float32).to(self.device))
        dataloader = DataLoader(dataset, batch_size=self.batch_size, shuffle=True)

        # 2. Khởi tạo Model
        self.model = LSTMAE_Module(self.input_dim, self.hidden_dim).to(self.device)
        optimizer = optim.Adam(self.model.parameters(), lr=0.001)
        criterion = nn.MSELoss()

        # 3. Training Loop
        self.model.train()
        for epoch in range(self.epochs):
            for batch in dataloader:
                batch_x = batch[0]
                optimizer.zero_grad()
                output = self.model(batch_x)
                loss = criterion(output, batch_x)
                loss.backward()
                optimizer.step()

        # 4. Tính toán Threshold (Ngưỡng) sau khi train
        self.model.eval()
        with torch.no_grad():
            X_tensor = torch.tensor(X_seq, dtype=torch.float32).to(self.device)
            reconstructions = self.model(X_tensor)
            
            # Tính MSE cho từng mẫu (giảm chiều seq và feat)
            # loss shape: (batch_size, )
            loss = torch.mean((X_tensor - reconstructions) ** 2, dim=[1, 2]).cpu().numpy()
        
        # Xác định ngưỡng dựa trên contamination (ví dụ top 5% lỗi cao nhất là bất thường)
        if len(loss) > 0:
            self.threshold_ = np.percentile(loss, 100 * (1 - self.contamination))
            seq_labels = (loss > self.threshold_).astype(int)
        else:
            self.threshold_ = 0
            seq_labels = np.array([])
        
        # 5. Gán nhãn (Labels)
        # Vì tạo sequence làm mất (seq_len - 1) dòng đầu, ta cần pad thêm 0 vào đầu
        # để độ dài labels_ khớp với độ dài X ban đầu.
        padding = np.zeros(self.seq_len - 1, dtype=int)
        self.labels_ = np.concatenate([padding, seq_labels])
        
        return self

# ==============================================================================
# 2. CONFIGURATION
# ==============================================================================

# Cấu hình tham số ML mặc định
DEFAULT_ML_CONFIG = {
    "min_train_size": 100,      # Tăng lên 100 vì LSTM cần chuỗi dữ liệu
    "max_buffer_size": 5000,
    "save_interval_sec": 60,

    # --- Cấu hình LSTM Autoencoder (MỚI) ---
    "ae_contamination": 0.039,
    "ae_epochs": 20,
    "ae_batch_size": 64,
    
    # [FIX] Số nguyên (int) thay vì list, khớp với LSTMAE_Module
    "ae_hidden_dim": 64,       
    
    # [THÊM] Độ dài chuỗi quan sát
    "ae_sequence_length": 5,   

    "ae_verbose": 0,

    # --- Cấu hình LightGBM ---
    "lgb_n_estimators": 200,
    "lgb_learning_rate": 0.05,
    "lgb_num_leaves": 31,
    "lgb_max_depth": -1,
    "lgb_scale_pos_weight": 30.03,

    "inference_quantile_threshold": 0.99,
    "inference_min_threshold": 0.75
}

# ==============================================================================
# 3. PRODUCTION ENGINE CLASS
# ==============================================================================

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

        exclude_cols = [
            'timestamp', 'event_id',
            'query', 'normalized_query', 'error_message',
            'is_anomaly', 'ml_anomaly_score', 'unusual_activity_reason', 'analysis_type',
            'accessed_tables', 'sensitive_access_info', 'tables_touched',
            'suspicious_func_name', 'privilege_cmd_name', 'error_code', 'behavior_group'
        ]

        potential_feats = self.training_buffer.select_dtypes(include=[np.number, 'category', 'object']).columns.tolist()
        self.features = [f for f in potential_feats if f not in exclude_cols]

        # [FIX] QUAN TRỌNG: Sắp xếp dữ liệu theo User và Thời gian trước khi train
        # Để LSTM học được chuỗi hành vi liền mạch của từng người
        df_sorted = self.training_buffer.sort_values(by=['user', 'timestamp']).copy()

        X = df_sorted[self.features].copy()

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
            # Convert sang float32 để nhẹ gánh cho PyTorch
            X_ae_scaled = scaler.fit_transform(X_ae).astype(np.float32)

            # --- START NEW CODE: LSTM Autoencoder ---
            ae = DeepLSTMAutoEncoder(
                sequence_length=self.config.get("ae_sequence_length", 5),
                hidden_dim=self.config.get("ae_hidden_dim", 64),
                epochs=self.config.get("ae_epochs", 20),
                contamination=self.config["ae_contamination"],
                batch_size=self.config.get("ae_batch_size", 64)
            )

            # Fit mô hình (Hàm này đã tự xử lý chuyển đổi chuỗi 3D bên trong)
            ae.fit(X_ae_scaled)
            pseudo_labels = ae.labels_
            # --- END NEW CODE ---

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
            
            # Lưu ý: pseudo_labels được sinh ra từ X đã sort, nên X đưa vào LGBM cũng phải là X (đã sort)
            lgb_model.fit(X, pseudo_labels, categorical_feature=lgb_cat_cols)

            self.save_production_model(lgb_model, self.features)
            self.model = lgb_model
            return True

        except Exception as e:
            logger.error(f"Training core failed: {e}", exc_info=True)
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
        
        client_ip = 'unknown'
        if 'client_ip' in group.columns and not group['client_ip'].empty:
            client_ip = group['client_ip'].iloc[0]

        # Lấy danh sách bảng bị truy cập trong session này
        from engine.utils import get_tables_with_sqlglot  # Import hàm này

        all_tables = set()
        queries_details = []

        for _, row in group.iterrows():
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

    # 1. LOAD CONFIG
    full_config = load_engine_config_dynamic()
    rules_json_config = full_config.get('security_rules', {})
    combined_rules_config = {**(config_params or {}), **rules_json_config}

    df_logs = input_df.copy()

    # 2. PREPROCESSING & FEATURES
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
    # 3. RULE-BASED DETECTION
    # ==========================================================================
    logging.info("--- Phase 1: Running Rules ---")

    try:
        dict_access = check_access_anomalies(df_logs, combined_rules_config)
        dict_insider = check_insider_threats(df_logs, combined_rules_config)
        dict_technical = check_technical_attacks(df_logs, combined_rules_config)
        dict_destruction = check_data_destruction(df_logs, combined_rules_config)
        dict_multi_table = check_multi_table_anomalies(df_logs, combined_rules_config)

        df_rule_access = process_rule_results(df_logs, dict_access, 'ACCESS_ANOMALY')
        df_rule_insider = process_rule_results(df_logs, dict_insider, 'INSIDER_THREAT')
        df_rule_technical = process_rule_results(df_logs, dict_technical, 'TECHNICAL_ATTACK')
        df_rule_destruction = process_rule_results(df_logs, dict_destruction, 'DATA_DESTRUCTION')
        df_rule_multi = process_rule_results(df_logs, dict_multi_table, 'MULTI_TABLE_ACCESS')
    except Exception as e:
        logging.error(f"Rule Engine Error: {e}", exc_info=True)
        df_rule_access = df_rule_insider = df_rule_technical = df_rule_destruction = df_rule_multi = pd.DataFrame()

    anomalies_user_time = pd.DataFrame()
    # Redis Profiling
    try:
        redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
        profile_threshold = combined_rules_config.get('thresholds', {}).get('min_occurrences_threshold', 5)

        current_indices_to_check = df_logs.index.difference(
            set(df_rule_access.index).union(df_rule_insider.index).union(df_rule_technical.index)
        )
        df_to_check = df_logs.loc[current_indices_to_check]
        
        if not df_to_check.empty:
            bad_indices = check_behavior_redis(redis_client, df_to_check, min_threshold=profile_threshold)
            
            if bad_indices:
                anomalies_user_time = df_logs.loc[bad_indices].copy()
                anomalies_user_time['behavior_group'] = 'UNUSUAL_BEHAVIOR'
                anomalies_user_time['specific_rule'] = 'Rare Access Time (Profile Mismatch)'
                
                anomalies_user_time['unusual_activity_reason'] = anomalies_user_time['timestamp'].apply(
                    lambda x: f"Access at {x.hour}h is rare (< {profile_threshold} times in history)"
                )

        # Learning
        df_to_learn = df_logs[~df_logs.index.isin(df_rule_technical.index)]
        update_behavior_redis(redis_client, df_to_learn)
        redis_client.close()
        
    except Exception as e:
        logging.error(f"Redis Behavioral Profiling Failed: {e}")
        anomalies_user_time = pd.DataFrame()

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
    # 4. FILTERING & ML DETECTION
    # ==========================================================================

    df_for_ml = df_logs[~df_logs.index.isin(rule_caught_indices)].copy()
    anomalies_ml = pd.DataFrame()

    if not df_for_ml.empty:
        logging.info(f"--- Phase 2: Running ML on {len(df_for_ml)} clean logs ---")

        # 1. Train Background
        uba_engine.train_and_update(df_for_ml)

        # 2. Predict
        if uba_engine.model and uba_engine.features:
            try:
                X = df_for_ml.copy()
                for f in uba_engine.features:
                    if f not in X.columns: X[f] = 0
                X = X[uba_engine.features]

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

                q_thresh = DEFAULT_ML_CONFIG["inference_quantile_threshold"]
                min_thresh = DEFAULT_ML_CONFIG["inference_min_threshold"]
                threshold = max(np.quantile(scores, q_thresh), min_thresh) if len(scores) > 0 else min_thresh

                anomalies_ml = df_for_ml[scores >= threshold].copy()
                if not anomalies_ml.empty:
                    anomalies_ml['behavior_group'] = 'ML_DETECTED'
            except Exception as e:
                logger.error(f"Inference failed: {e}")
                df_logs['ml_anomaly_score'] = 0.0
    else:
        logging.info("All logs caught by Rules. Skipping ML.")

    # ========================================================
    # AGGREGATE & ACTIVE RESPONSE
    # ========================================================
    anomalies_multi_table_agg = _aggregate_multi_table_alerts(df_rule_multi)

    users_to_lock_list = []
    list_of_violation_dfs = []
    
    ar_config = full_config.get("active_response_config", {})
    threshold = ar_config.get("max_violation_threshold", 3)

    violation_sources = [
        df_rule_access, df_rule_insider, df_rule_technical, df_rule_destruction
    ]

    for df in violation_sources:
        if not df.empty and 'user' in df.columns:
            cols = ['user']
            if 'client_ip' in df.columns: cols.append('client_ip')
            list_of_violation_dfs.append(df[cols].copy())

    if not anomalies_multi_table_agg.empty and 'user' in anomalies_multi_table_agg.columns:
        temp_df = anomalies_multi_table_agg[['user']].copy()
        if 'client_ip' not in temp_df.columns: temp_df['client_ip'] = 'unknown'
        list_of_violation_dfs.append(temp_df)

    critical_users = set()
    if not df_rule_technical.empty and 'user' in df_rule_technical.columns:
        critical_users.update(df_rule_technical['user'].unique())
    if not df_rule_destruction.empty and 'user' in df_rule_destruction.columns:
        critical_users.update(df_rule_destruction['user'].unique())

    if list_of_violation_dfs:
        all_violations_df = pd.concat(list_of_violation_dfs, ignore_index=True)
        user_violation_counts = all_violations_df.groupby('user', observed=False).size().reset_index(
            name='total_violation_count')

        offenders = user_violation_counts[
            (user_violation_counts['total_violation_count'] >= threshold) |
            (user_violation_counts['user'].isin(critical_users))
            ].copy()
        
        def get_lock_reason(row):
            if row['user'] in critical_users:
                return f"CRITICAL VIOLATION (Zero Tolerance) - Count: {row['total_violation_count']}"
            return f"Threshold Exceeded - Count: {row['total_violation_count']}"

        if not offenders.empty:
            offenders['lock_reason'] = offenders.apply(get_lock_reason, axis=1)
            users_to_lock_list = offenders.to_dict('records')

    # ========================================================
    # UPDATE LOGS & RETURN
    # ========================================================
    detection_sources = [
        df_rule_access, df_rule_insider, df_rule_technical,
        df_rule_destruction, df_rule_multi, anomalies_user_time, anomalies_ml
    ]

    if 'unusual_activity_reason' not in df_logs.columns:
        df_logs['unusual_activity_reason'] = None
    
    for source_df in detection_sources:
        if source_df is not None and not source_df.empty:
            common_indices = source_df.index.intersection(df_logs.index)
            
            if not common_indices.empty:
                if 'specific_rule' in source_df.columns:
                    rule_series = source_df.loc[common_indices, 'specific_rule']
                elif 'behavior_group' in source_df.columns:
                    rule_series = source_df.loc[common_indices, 'behavior_group']
                else:
                    rule_series = pd.Series("Detected by System", index=common_indices)

                df_logs.loc[common_indices, 'unusual_activity_reason'] = rule_series
                df_logs.loc[common_indices, 'is_anomaly'] = 1

    all_anomalous_indices = rule_caught_indices.union(set(anomalies_ml.index.tolist()))
    normal_activities = df_logs[~df_logs.index.isin(all_anomalous_indices)].copy()

    results = {
        "all_logs": df_logs,
        "anomalies_ml": anomalies_ml,
        "rule_access": df_rule_access,
        "rule_insider": df_rule_insider,
        "rule_technical": df_rule_technical,
        "rule_destruction": df_rule_destruction,
        "rule_behavior_profile": anomalies_user_time,
        "rule_multi_table": anomalies_multi_table_agg,
        "users_to_lock": users_to_lock_list,
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