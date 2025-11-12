"""
================================================================================
MODULE XỬ LÝ DỮ LIỆU CHÍNH (PHIÊN BẢN ĐỘC LẬP)
================================================================================
Đây là "trái tim" của engine phân tích, đã được tái cấu trúc để chạy độc lập
mà không cần đến Streamlit. Nó thực hiện các công việc sau:
1.  Tải và kiểm tra tính hợp lệ của DataFrame log.
2.  Tiền xử lý dữ liệu.
3.  Feature Engineering.
4.  Áp dụng mô hình Machine Learning (AI) nếu có feedback.
5.  Áp dụng các luật (Rules) đã được định nghĩa.
6.  Trả về một dictionary chứa tất cả các kết quả đã xử lý.
"""

import os
import sys
import logging
import joblib
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from datetime import time as dt_time, datetime
from tzlocal import get_localzone
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.preprocessing import StandardScaler

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [DataProcessor] - %(message)s')

# --- Import nội bộ ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MODELS_DIR, USER_MODELS_DIR

from utils import (
    is_late_night_query, is_potential_large_dump, get_tables_with_sqlglot,
    analyze_sensitive_access, check_unusual_user_activity_time, extract_query_features,
    is_suspicious_function_used, is_privilege_change
)

# Đảm bảo thư mục model tồn tại
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(USER_MODELS_DIR, exist_ok=True)

# ==============================================================================
# PHẦN 1 (MỚI): CÁC HÀM CHO PIPELINE SEMI-SUPERVISED
# ==============================================================================

def create_initial_features(df: pd.DataFrame, rules_config: dict | None = None) -> pd.DataFrame:
    """Thêm các cột đặc trưng cần thiết cho ML pipeline."""
    if df.empty: return df
    df_copy = df.copy()
    df_copy['hour'] = df_copy['timestamp'].dt.hour
    df_copy['day_of_week'] = df_copy['timestamp'].dt.dayofweek
    df_copy['hour_sin'] = np.sin(2 * np.pi * df_copy['hour'] / 24)
    df_copy['hour_cos'] = np.cos(2 * np.pi * df_copy['hour'] / 24)
    return df_copy

def build_preprocessing_pipeline() -> Pipeline:
    """Xây dựng pipeline của Scikit-learn để xử lý tất cả các loại đặc trưng."""
    categorical_features = ['user', 'client_ip', 'database', 'source_dbms']
    numerical_features = [
        'query_length', 'num_joins', 'num_where_conditions', 'hour_sin', 'hour_cos',
        'rows_returned', 'rows_affected', 'execution_time_ms'
    ]
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numerical_features),
            ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features)
        ],
        remainder='drop'
    )
    pipeline = Pipeline(steps=[('preprocessor', preprocessor)])
    pipeline.set_output(transform="pandas")
    return pipeline

def get_pseudo_labels(X_transformed: pd.DataFrame, contamination: float) -> np.ndarray:
    """Sử dụng Isolation Forest để tạo ra các nhãn giả."""
    if X_transformed is None or X_transformed.empty: return np.array([])
    logging.info(f"Bắt đầu giai đoạn Unsupervised với Isolation Forest (contamination={contamination})...")
    iso_forest = IsolationForest(contamination=contamination, random_state=42, n_jobs=-1)
    predictions = iso_forest.fit_predict(X_transformed)
    return np.where(predictions == -1, 1, 0)

def train_and_score_with_classifier(X_transformed: pd.DataFrame, pseudo_labels: np.ndarray):
    """Huấn luyện mô hình LightGBM và trả về điểm số cuối cùng."""
    num_anomalies = np.sum(pseudo_labels)
    if num_anomalies == 0: return None
    
    num_normals = len(pseudo_labels) - num_anomalies
    scale_pos_weight = num_normals / num_anomalies
    
    logging.info(f"Bắt đầu giai đoạn Supervised với LightGBM (scale_pos_weight={scale_pos_weight:.2f})...")
    lgbm_classifier = lgb.LGBMClassifier(random_state=42, n_jobs=-1, scale_pos_weight=scale_pos_weight, verbose=-1)
    lgbm_classifier.fit(X_transformed, pseudo_labels)
    logging.info("Huấn luyện mô hình Supervised hoàn tất.")
    
    return lgbm_classifier.predict_proba(X_transformed)[:, 1]

# --------------------------- Core Processor ---------------------------

def load_and_process_data(input_df: pd.DataFrame, config_params: dict) -> dict:
    """
    Hàm điều phối chính:
    1. Chạy pipeline Semi-Supervised để lấy điểm số ML.
    2. Chạy các luật cũ để làm giàu dữ liệu và phát hiện các vi phạm cụ thể.
    """
    # 1) Validate input và Tiền xử lý
    if input_df is None or input_df.empty:
        logging.warning("DataFrame đầu vào rỗng, không có dữ liệu để xử lý.")
        empty = pd.DataFrame()
        return { "all_logs": empty, "anomalies_ml": empty, "anomalies_rules": empty }
    
    df_logs = input_df.copy()
    
    stat_columns = {'rows_returned': 0, 'rows_affected': 0, 'execution_time_ms': 0.0}
    for col, default_val in stat_columns.items():
        if col not in df_logs.columns: df_logs[col] = default_val
        else: df_logs[col] = df_logs[col].fillna(default_val)
    
    df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp'], errors='coerce', utc=True)
    df_logs.dropna(subset=['timestamp'], inplace=True)
    if df_logs.empty: return load_and_process_data(pd.DataFrame(), {})

    try:
        df_logs['timestamp'] = df_logs['timestamp'].dt.tz_convert(get_localzone())
    except Exception as e:
        logging.error(f"Lỗi khi chuẩn hóa múi giờ: {e}.")

    df_logs['query'] = df_logs['query'].astype(str)
    df_logs['query_length'] = df_logs['query'].str.len().fillna(0).astype(int)

    # 2) Feature Engineering (cả cho ML và cho luật)
    logging.info("Bắt đầu Feature Engineering...")
    rules_config = config_params or {}
    df_logs = create_initial_features(df_logs, rules_config) # Gọi hàm mới
    query_features_df = df_logs['query'].apply(extract_query_features).apply(pd.Series)
    df_logs = pd.concat([df_logs, query_features_df], axis=1).fillna(0)

    # 3) CHẠY PIPELINE SEMI-SUPERVISED
    logging.info("Bắt đầu pipeline Semi-Supervised ML...")
    pipeline_path = os.path.join(MODELS_DIR, "preprocessing_pipeline.joblib")
    
    pipeline = build_preprocessing_pipeline()
    missing_cols = [c for c in ['query_length','num_joins','num_where_conditions','hour_sin','hour_cos',
                            'rows_returned','rows_affected','execution_time_ms'] if c not in df_logs.columns]
    if missing_cols:
        logging.warning(f"Thiếu cột trước khi pipeline: {missing_cols}")

    # đảm bảo các cột numeric còn lại đúng kiểu số
    for col in ['num_joins','num_where_conditions','hour_sin','hour_cos',
                'rows_returned','rows_affected','execution_time_ms']:
        if col in df_logs.columns:
            df_logs[col] = pd.to_numeric(df_logs[col], errors='coerce').fillna(0)


    X_transformed = pipeline.fit_transform(df_logs) # Chạy trên df_logs đã được enrich
    
    semi_supervised_config = config_params.get("semi_supervised_params", {})
    contamination_rate = semi_supervised_config.get("contamination", 0.02)
    
    pseudo_labels = get_pseudo_labels(X_transformed, contamination=contamination_rate)
    final_anomaly_scores = train_and_score_with_classifier(X_transformed, pseudo_labels)
    
    anomalies_ml = pd.DataFrame()
    if final_anomaly_scores is not None:
        df_logs['ml_anomaly_score'] = final_anomaly_scores
        anomaly_threshold = semi_supervised_config.get("anomaly_threshold", 0.5)
        anomalies_ml = df_logs[df_logs['ml_anomaly_score'] >= anomaly_threshold].copy()
    else:
        df_logs['ml_anomaly_score'] = 0.0

    logging.info(f"Phát hiện {len(anomalies_ml)} bất thường bằng ML.")

    # 6) Áp dụng Rules

    logging.info("Bắt đầu áp dụng các luật (Rules)...")
    rules_config = config_params or {}

    # <<< THÊM LẠI TOÀN BỘ KHỐI LẤY THAM SỐ NÀY >>>
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
    anomalies_late_night = df_logs[df_logs['is_late_night']].copy().reset_index(drop=True)
    
    if 'rows_returned' not in df_logs.columns:
        # Gán giá trị 0 cho các bản ghi không có cột này (chủ yếu là logs từ General Log)
        df_logs['rows_returned'] = 0
        logging.getLogger('DataProcessor').warning(
            "Column 'rows_returned' missing in a batch. Set to 0 to avoid KeyError."
    )

    # Rule 2: Potential large dump
    df_logs['is_potential_dump'] = df_logs.apply(
        lambda row: is_potential_large_dump(row, p_known_large_tables),
        axis=1
    )
    anomalies_large_dump = df_logs[df_logs['is_potential_dump']].copy().reset_index(drop=True)

    # Rule 3: Multi-table access window
    df_logs['accessed_tables'] = df_logs['query'].apply(get_tables_with_sqlglot)
    current_time_window = pd.Timedelta(minutes=p_time_window_minutes)
    anomalies_multiple_tables_list = []

    for user, user_df_orig in df_logs.groupby('user'):
        if pd.isna(user) or user_df_orig.empty:
            continue

        user_df = user_df_orig.sort_values('timestamp').reset_index(drop=True)
        if user_df.empty:
            continue

        current_session_tables = set()
        current_session_queries_details = []
        session_start_time = user_df.iloc[0]['timestamp']

        for _, row in user_df.iterrows():
            query_time = row['timestamp']
            tables_in_this_query_list = row['accessed_tables'] if isinstance(row['accessed_tables'], list) else []
            tables_in_this_query_set = set(tables_in_this_query_list)

            if (query_time - session_start_time) > current_time_window:
                if len(current_session_tables) >= p_min_distinct_tables and current_session_queries_details:
                    anomalies_multiple_tables_list.append({
                        'user': user,
                        'start_time': session_start_time,
                        'end_time': current_session_queries_details[-1]['timestamp'],
                        'distinct_tables_count': len(current_session_tables),
                        'tables_accessed_in_session': sorted(list(current_session_tables)),
                        'queries_details': current_session_queries_details
                    })
                current_session_tables = tables_in_this_query_set
                current_session_queries_details = [{
                    'timestamp': query_time,
                    'query': row['query'],
                    'tables_touched': tables_in_this_query_list
                }]
                session_start_time = query_time
            else:
                current_session_tables.update(tables_in_this_query_set)
                current_session_queries_details.append({
                    'timestamp': query_time,
                    'query': row['query'],
                    'tables_touched': tables_in_this_query_list
                })

        if len(current_session_tables) >= p_min_distinct_tables and current_session_queries_details:
            anomalies_multiple_tables_list.append({
                'user': user,
                'start_time': session_start_time,
                'end_time': current_session_queries_details[-1]['timestamp'],
                'distinct_tables_count': len(current_session_tables),
                'tables_accessed_in_session': sorted(list(current_session_tables)),
                'queries_details': current_session_queries_details
            })

    anomalies_multiple_tables_df = pd.DataFrame(anomalies_multiple_tables_list) if anomalies_multiple_tables_list else pd.DataFrame()
    if not anomalies_multiple_tables_df.empty:
        anomalies_multiple_tables_df = anomalies_multiple_tables_df.reset_index().rename(columns={'index': 'anomaly_id'})

    # Rule 4: Sensitive access
    df_logs['sensitive_access_info'] = df_logs.apply(
        lambda row: analyze_sensitive_access(
            row,
            p_sensitive_tables,
            p_allowed_users_sensitive,
            p_safe_hours_start,
            p_safe_hours_end,
            p_safe_weekdays
        ),
        axis=1
    )
    anomalies_sensitive_access = df_logs[df_logs['sensitive_access_info'].notna()].copy()
    if not anomalies_sensitive_access.empty:
        expanded = anomalies_sensitive_access['sensitive_access_info'].apply(pd.Series)
        anomalies_sensitive_access = pd.concat(
            [anomalies_sensitive_access.drop(columns=['sensitive_access_info'], errors='ignore'), expanded],
            axis=1
        ).reset_index(drop=True)

    # Rule 5: Unusual user activity window
    user_activity_profiles = {}
    if 'user' in df_logs.columns and 'timestamp' in df_logs.columns:
        for user, group in df_logs.groupby('user'):
            if pd.isna(user) or len(group) < p_min_queries_for_profile:
                continue
            hours = group['timestamp'].dt.hour
            if len(hours) >= 5:
                active_start_hour = int(hours.quantile(p_quantile_start))
                active_end_hour = int(hours.quantile(p_quantile_end))
                if active_end_hour <= active_start_hour:
                    active_end_hour = min(active_start_hour + 4, 23)
                user_activity_profiles[user] = {
                    'active_start': active_start_hour,
                    'active_end': active_end_hour
                }

    df_logs['unusual_activity_reason'] = df_logs.apply(
        lambda row: check_unusual_user_activity_time(row, user_activity_profiles),
        axis=1
    )
    anomalies_unusual_user_time = df_logs[df_logs['unusual_activity_reason'].notna()].copy().reset_index(drop=True)
    
    # Rule 6 (MỚI): SQLi / Hàm đáng ngờ
    suspicious_results = df_logs['query'].apply(is_suspicious_function_used).apply(pd.Series)
    suspicious_results.columns = ['is_suspicious_func', 'suspicious_func_name']
    df_logs = pd.concat([df_logs, suspicious_results], axis=1)
    anomalies_sqli = df_logs[df_logs['is_suspicious_func'] == True].copy().reset_index(drop=True)

    # Rule 7 (MỚI): Thay đổi quyền
    privilege_results = df_logs['query'].apply(is_privilege_change).apply(pd.Series)
    privilege_results.columns = ['is_privilege_change', 'privilege_cmd_name']
    df_logs = pd.concat([df_logs, privilege_results], axis=1)
    anomalies_privilege = df_logs[df_logs['is_privilege_change'] == True].copy().reset_index(drop=True)

    # 7) Normal activities
    anomalous_indices = set()
    for part in [
        anomalies_late_night.index,
        anomalies_large_dump.index,
        anomalies_sensitive_access.index if not anomalies_sensitive_access.empty else [],
        anomalies_unusual_user_time.index,
        anomalies_sqli.index,        
        anomalies_privilege.index
    ]:
        anomalous_indices.update(part)

    if not anomalies_multiple_tables_df.empty:
        for queries_list in anomalies_multiple_tables_df['queries_details']:
            queries_df = pd.DataFrame(queries_list)
            merged = pd.merge(df_logs.reset_index(), queries_df, on=['timestamp', 'query'])
            if not merged.empty:
                anomalous_indices.update(merged['index'])

    normal_activities = df_logs[~df_logs.index.isin(anomalous_indices)].copy()

    # 8) Kết quả
    results = {
        "all_logs": df_logs,
        "anomalies_late_night": anomalies_late_night,
        "anomalies_dump": anomalies_large_dump,
        "anomalies_multi_table": anomalies_multiple_tables_df,
        "anomalies_sensitive": anomalies_sensitive_access,
        "anomalies_user_time": anomalies_unusual_user_time,
        "anomalies_sqli": anomalies_sqli,          
        "anomalies_privilege": anomalies_privilege,
        "normal_activities": normal_activities
    }

    logging.info(f"Hoàn thành xử lý. Tìm thấy {len(anomalous_indices)} bất thường trên tổng số {len(df_logs)} bản ghi.")
    return results

# --------------------------- Model I/O ---------------------------

def save_model_and_scaler(model, scaler, path):
    try:
        joblib.dump({'model': model, 'scaler': scaler}, path)
        logging.info(f"Đã lưu mô hình và scaler vào: {path}")
    except Exception as e:
        logging.error(f"Lỗi khi lưu mô hình tại {path}: {e}")

def load_model_and_scaler(path):
    try:
        if os.path.exists(path):
            data = joblib.load(path)
            logging.info(f"Đã tải mô hình và scaler từ: {path}")
            return data['model'], data['scaler']
        return None, None
    except Exception as e:
        logging.error(f"Lỗi khi tải mô hình tại {path}: {e}")
        return None, None