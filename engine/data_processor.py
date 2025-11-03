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
from datetime import time as dt_time, datetime
from tzlocal import get_localzone
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.preprocessing import StandardScaler

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [DataProcessor] - %(message)s')

# --- Import nội bộ ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import *
from .utils import (
    is_late_night_query, is_potential_large_dump, get_tables_with_sqlglot,
    analyze_sensitive_access, check_unusual_user_activity_time, extract_query_features
)

# Đảm bảo thư mục model tồn tại
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(USER_MODELS_DIR, exist_ok=True)

# --------------------------- Helpers cấu hình ---------------------------

def get_cfg(cfg: dict, *keys, default=None):
    """
    Trả về giá trị đầu tiên tìm thấy theo danh sách khóa (cho phép tương thích tên cũ/mới).
    """
    for k in keys:
        if k in cfg and cfg[k] is not None:
            return cfg[k]
    return default

def coerce_time(val, fallback: dt_time) -> dt_time:
    """
    Ưu tiên nhận dt_time trực tiếp.
    Cũng hỗ trợ:
      - int/float = giờ (0..23)  -> dt_time(h, 0)
      - "HH:MM[:SS]"            -> dt_time(...)
    Nếu không phân giải được -> trả fallback.
    """
    if isinstance(val, dt_time):
        return val
    if isinstance(val, (int, float)):
        try:
            h = int(val)
            return dt_time(max(0, min(23, h)), 0)
        except Exception:
            return fallback
    if isinstance(val, str):
        parts = val.strip().split(":")
        try:
            if len(parts) == 2:
                return dt_time(int(parts[0]), int(parts[1]))
            if len(parts) == 3:
                return dt_time(int(parts[0]), int(parts[1]), int(parts[2]))
        except Exception:
            return fallback
    return fallback

# --------------------------- Core Processor ---------------------------

def load_and_process_data(input_df: pd.DataFrame, config_params: dict) -> dict:
    """
    Xử lý dữ liệu log từ DataFrame và tham số cấu hình (dict).
    Trả về dict kết quả.
    """
    # 1) Validate input
    if input_df is None or input_df.empty:
        logging.warning("DataFrame đầu vào rỗng hoặc None, không có dữ liệu để xử lý.")
        empty = pd.DataFrame()
        return {
            "all_logs": empty,
            "anomalies_late_night": empty,
            "anomalies_dump": empty,
            "anomalies_multi_table": empty,
            "anomalies_sensitive": empty,
            "anomalies_user_time": empty,
            "anomalies_complexity": empty,
            "normal_activities": empty
        }

    df_logs = input_df.copy()

    # 2) Chuẩn hóa timestamp & query
    # Cho phép mixed: ISO8601, pandas datetime, v.v.
    df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp'], errors='coerce', utc=None)
    df_logs.dropna(subset=['timestamp'], inplace=True)
    if df_logs.empty:
        logging.warning("Không còn dữ liệu sau khi loại bỏ timestamp không hợp lệ.")
        return {"empty": True}

    try:
        local_tz = get_localzone()
        if df_logs['timestamp'].dt.tz is not None:
            df_logs['timestamp'] = df_logs['timestamp'].dt.tz_convert(local_tz)
        else:
            logging.warning("Timestamp không có múi giờ. Giả định UTC -> chuyển sang local.")
            df_logs['timestamp'] = df_logs['timestamp'].dt.tz_localize('UTC').dt.tz_convert(local_tz)
    except Exception as e:
        logging.error(f"Lỗi khi chuẩn hóa múi giờ: {e}.")

    df_logs['query'] = df_logs['query'].astype(str)

    # 3) Lấy & chuẩn hóa tham số cấu hình (hỗ trợ cả tên khóa cũ & mới)
    cfg = config_params or {}

    # Late Night: nhận dt_time trực tiếp (hoặc ép kiểu an toàn)
    late_start_raw = get_cfg(cfg, 'p_late_night_start_time', 'late_night_start', default=LATE_NIGHT_START_TIME_DEFAULT)
    late_end_raw   = get_cfg(cfg, 'p_late_night_end_time',   'late_night_end',   default=LATE_NIGHT_END_TIME_DEFAULT)
    p_late_night_start_time = coerce_time(late_start_raw, LATE_NIGHT_START_TIME_DEFAULT)
    p_late_night_end_time   = coerce_time(late_end_raw,   LATE_NIGHT_END_TIME_DEFAULT)

    # Known large tables
    p_known_large_tables = get_cfg(cfg, 'p_known_large_tables', 'known_large_tables', default=KNOWN_LARGE_TABLES_DEFAULT)

    # Multi-table window & threshold
    p_time_window_minutes = int(get_cfg(cfg, 'p_time_window_minutes', 'multi_table_window_minutes', default=TIME_WINDOW_DEFAULT_MINUTES))
    p_min_distinct_tables = int(get_cfg(cfg, 'p_min_distinct_tables', 'multi_table_min_distinct', default=MIN_DISTINCT_TABLES_THRESHOLD_DEFAULT))

    # Sensitive tables & allowlist
    p_sensitive_tables = get_cfg(cfg, 'p_sensitive_tables', 'sensitive_tables', default=SENSITIVE_TABLES_DEFAULT)
    p_allowed_users_sensitive = get_cfg(cfg, 'p_allowed_users_sensitive', 'allowed_users_for_sensitive', default=ALLOWED_USERS_FOR_SENSITIVE_DEFAULT)

    # Safe hours (giờ nguyên) & weekdays
    p_safe_hours_start = int(get_cfg(cfg, 'p_safe_hours_start', 'safe_hours_start', default=SAFE_HOURS_START_DEFAULT))
    p_safe_hours_end   = int(get_cfg(cfg, 'p_safe_hours_end',   'safe_hours_end',   default=SAFE_HOURS_END_DEFAULT))
    p_safe_weekdays    = get_cfg(cfg, 'p_safe_weekdays', 'safe_weekdays', default=SAFE_WEEKDAYS_DEFAULT)

    # User profile quantiles & minimum samples
    p_quantile_start = float(get_cfg(cfg, 'p_quantile_start', 'quantile_start', default=QUANTILE_START_DEFAULT))
    p_quantile_end   = float(get_cfg(cfg, 'p_quantile_end',   'quantile_end',   default=QUANTILE_END_DEFAULT))
    p_min_queries_for_profile = int(get_cfg(cfg, 'p_min_queries_for_profile', 'min_queries_for_profile', default=MIN_QUERIES_FOR_PROFILE_DEFAULT))

    # (Tùy chọn) log nhẹ để xác nhận kiểu tham số
    logging.info(
        f"LateNight={p_late_night_start_time}-{p_late_night_end_time} "
        f"| SafeHours={p_safe_hours_start}-{p_safe_hours_end} "
        f"| Window={p_time_window_minutes}min Distinct≥{p_min_distinct_tables}"
    )

    # 4) Feature engineering
    df_logs['query_lower'] = df_logs['query'].str.lower()
    df_logs['query_length'] = df_logs['query_lower'].str.len()

    write_cmds = ('insert', 'update', 'delete', 'replace')
    ddl_cmds = ('create', 'alter', 'drop', 'truncate', 'rename')

    df_logs['is_write_query'] = df_logs['query_lower'].str.startswith(write_cmds, na=False)
    df_logs['is_ddl_query']   = df_logs['query_lower'].str.startswith(ddl_cmds, na=False)
    df_logs.drop(columns=['query_lower'], inplace=True)

    query_features_df = df_logs['query'].apply(extract_query_features).apply(pd.Series)
    df_logs = pd.concat([df_logs, query_features_df], axis=1)

    # 5) Phân tích AI (nếu có feedback đủ)
    feature_cols = [
        'num_joins', 'num_where_conditions', 'num_group_by_cols',
        'num_order_by_cols', 'has_limit', 'has_subquery',
        'has_union', 'has_where', 'query_length',
        'is_write_query', 'is_ddl_query'
    ]
    df_logs[feature_cols] = df_logs[feature_cols].fillna(0)

    supervised_model = train_supervised_model_from_feedback(feature_cols)
    if supervised_model:
        predictions_proba = supervised_model.predict_proba(df_logs[feature_cols])
        df_logs['anomaly_score'] = predictions_proba[:, 1]
        df_logs['is_complexity_anomaly'] = supervised_model.predict(df_logs[feature_cols])
        df_logs['analysis_type'] = "Supervised Feedback"
    else:
        scores, is_anomaly, types = analyze_contextual_complexity_anomalies(
            df_logs, p_min_queries_for_profile
        )
        df_logs['anomaly_score'] = scores
        df_logs['is_complexity_anomaly'] = is_anomaly
        df_logs['analysis_type'] = types

    df_logs['is_complexity_anomaly'] = df_logs['is_complexity_anomaly'].astype(bool).fillna(False)
    df_logs['analysis_type'] = df_logs['analysis_type'].fillna("Not Analyzed")
    anomalies_complexity = df_logs[df_logs['is_complexity_anomaly']].copy().reset_index(drop=True)

    # 6) Áp dụng Rules

    # Rule 1: Late night
    df_logs['is_late_night'] = df_logs['timestamp'].apply(
        lambda ts: is_late_night_query(ts, p_late_night_start_time, p_late_night_end_time)
    )
    anomalies_late_night = df_logs[df_logs['is_late_night']].copy().reset_index(drop=True)

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

    # 7) Normal activities
    anomalous_indices = set()
    for part in [
        anomalies_late_night.index,
        anomalies_large_dump.index,
        anomalies_sensitive_access.index if not anomalies_sensitive_access.empty else [],
        anomalies_unusual_user_time.index,
        anomalies_complexity.index
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
        "anomalies_complexity": anomalies_complexity,
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

# --------------------------- AI Phân tích ---------------------------

def train_supervised_model_from_feedback(feature_cols):
    feedback_file = 'feedback.csv'
    if not os.path.exists(feedback_file) or os.path.getsize(feedback_file) == 0:
        return None
    try:
        df_feedback = pd.read_csv(feedback_file)
    except Exception as e:
        logging.error(f"Lỗi khi đọc file feedback.csv: {e}")
        return None

    if len(df_feedback) < 20 or df_feedback['is_anomaly_label'].nunique() < 2:
        return None

    logging.info(f"Đang sử dụng mô hình nâng cao được huấn luyện từ {len(df_feedback)} mẫu phản hồi.")
    available_features = [col for col in feature_cols if col in df_feedback.columns]
    X_train = df_feedback[available_features].fillna(0)
    y_train = df_feedback['is_anomaly_label']
    model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    model.fit(X_train, y_train)
    return model

def analyze_contextual_complexity_anomalies(df_with_features, min_queries_for_profile):
    feature_cols = [
        'num_joins', 'num_where_conditions', 'num_group_by_cols',
        'num_order_by_cols', 'has_limit', 'has_subquery',
        'has_union', 'has_where', 'query_length',
        'is_write_query', 'is_ddl_query'
    ]
    df_analysis = df_with_features[feature_cols + ['user']].dropna()
    if df_analysis.empty:
        return pd.Series(name='anomaly_score'), pd.Series(name='is_complexity_anomaly'), pd.Series(name='analysis_type')

    global_model_path = os.path.join(MODELS_DIR, "global_isolation_forest.joblib")
    global_model, scaler_global = load_model_and_scaler(global_model_path)
    if global_model is None or scaler_global is None:
        logging.info("Không tìm thấy mô hình toàn cục, đang tiến hành huấn luyện lần đầu...")
        scaler_global = StandardScaler()
        X_global_scaled = scaler_global.fit_transform(df_analysis[feature_cols])
        global_model = IsolationForest(contamination=0.05, random_state=42)
        global_model.fit(X_global_scaled)
        save_model_and_scaler(global_model, scaler_global, global_model_path)

    all_results = []
    for user, user_df in df_analysis.groupby('user'):
        current_user_df = user_df.copy()
        user_model_path = os.path.join(USER_MODELS_DIR, f"{user}.joblib")

        if len(user_df) >= min_queries_for_profile:
            user_model, scaler_user = load_model_and_scaler(user_model_path)
            if user_model is None or scaler_user is None:
                logging.info(f"Tạo hồ sơ AI mới cho user: {user}")
                scaler_user = StandardScaler()
                X_user_scaled_train = scaler_user.fit_transform(user_df[feature_cols])
                user_model = IsolationForest(contamination=0.05, random_state=42)
                user_model.fit(X_user_scaled_train)
                save_model_and_scaler(user_model, scaler_user, user_model_path)

            X_user_scaled_predict = scaler_user.transform(user_df[feature_cols])
            scores = user_model.decision_function(X_user_scaled_predict)
            predictions = user_model.predict(X_user_scaled_predict)
            current_user_df['analysis_type'] = "Per-User Profile"
        else:
            X_fallback_scaled = scaler_global.transform(user_df[feature_cols])
            scores = global_model.decision_function(X_fallback_scaled)
            predictions = global_model.predict(X_fallback_scaled)
            current_user_df['analysis_type'] = "Global Fallback"

        current_user_df['anomaly_score'] = scores
        current_user_df['is_complexity_anomaly'] = (predictions == -1)
        all_results.append(current_user_df)

    if not all_results:
        return pd.Series(name='anomaly_score'), pd.Series(name='is_complexity_anomaly'), pd.Series(name='analysis_type')

    final_df = pd.concat(all_results)
    return final_df['anomaly_score'], final_df['is_complexity_anomaly'], final_df['analysis_type']
