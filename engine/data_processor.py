# engine/data_processor.py
"""
================================================================================
MODULE XỬ LÝ DỮ LIỆU CHÍNH (PHIÊN BẢN ĐỘC LẬP)
================================================================================
Đây là "trái tim" của engine phân tích, đã được tái cấu trúc để chạy độc lập
mà không cần đến Streamlit. Nó thực hiện các công việc sau:
1.  Tải và kiểm tra tính hợp lệ của file log CSV.
2.  Tiền xử lý dữ liệu.
3.  Thực hiện Feature Engineering.
4.  Áp dụng các mô hình Machine Learning (AI).
5.  Áp dụng các luật (Rules) đã được định nghĩa.
6.  Trả về một dictionary chứa tất cả các kết quả đã xử lý.
"""

import pandas as pd
from datetime import time as dt_time, datetime
from tzlocal import get_localzone
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
import os
import joblib
import logging
import sys
# Thư viện để vẽ biểu đồ
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import classification_report, confusion_matrix

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [MySQLParser] - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)]) # Chỉ in ra console cho đơn giản

# --- Thêm thư mục gốc vào sys.path để import config và các module khác ---
# Điều này làm cho module này có thể chạy được từ bất kỳ đâu.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *
# Sử dụng relative import cho các module trong cùng package 'engine'
from .utils import (
    is_late_night_query, is_potential_large_dump, get_tables_with_sqlglot,
    analyze_sensitive_access, check_unusual_user_activity_time, extract_query_features
)
# from email_alert import send_email_alert # Sẽ tích hợp lại sau nếu cần

# Đảm bảo các thư mục tồn tại
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(USER_MODELS_DIR, exist_ok=True)

def load_and_process_data(input_df: pd.DataFrame, config_params: dict) -> dict:
    """
    Xử lý dữ liệu log từ một DataFrame đã được cung cấp.
    """
    if input_df is None or input_df.empty:
        logging.warning("DataFrame đầu vào rỗng hoặc None, không có dữ liệu để xử lý.")
        return {"empty": True}
    
    # Lấy cấu hình whitelist
    whitelists = config_params.get("whitelists", {})
    maintenance_users = whitelists.get("maintenance_users", [])

    def is_whitelisted(row):
        if row['user'] in maintenance_users:
            return True
        # Thêm các điều kiện khác (IP, thời gian)
        return False
    
    df_logs = input_df.copy()

    # --- Tiền xử lý Dữ liệu ---
    
    df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp'], format='mixed', errors='coerce')
    df_logs.dropna(subset=['timestamp'], inplace=True)
    if df_logs.empty:
        logging.warning("Không còn dữ liệu sau khi loại bỏ timestamp không hợp lệ.")
        return {"empty": True}
    try:
        local_tz = get_localzone()
        if df_logs['timestamp'].dt.tz is None:
            df_logs['timestamp'] = df_logs['timestamp'].dt.tz_localize('UTC').dt.tz_convert(local_tz)
        else:
            df_logs['timestamp'] = df_logs['timestamp'].dt.tz_convert(local_tz)
    except Exception as e:
        logging.error(f"Lỗi khi chuẩn hóa múi giờ: {e}.")
    df_logs['query'] = df_logs['query'].astype(str)
    
    def tag_maintenance_activity(row, whitelists):
        # Nếu user là maintenance user VÀ hoạt động vào ban đêm
        if row['user'] in whitelists.get("maintenance_users", []) and (row['timestamp'].hour < 5 or row['timestamp'].hour > 22):
            return True
        # Nếu query chứa các từ khóa bảo trì
        if any(keyword in row['query'].lower() for keyword in ['backup', 'optimize table', 'analyze table']):
            return True
        return False

    # --- Lấy và Chuẩn hóa Tham số Cấu hình ---
    try:
        p_late_night_start_time = dt_time.fromisoformat(config_params.get('p_late_night_start_time'))
        p_late_night_end_time = dt_time.fromisoformat(config_params.get('p_late_night_end_time'))
    except (ValueError, TypeError):
        logging.error("Định dạng thời gian trong config không hợp lệ. Sử dụng giá trị mặc định.")
        p_late_night_start_time = LATE_NIGHT_START_TIME_DEFAULT
        p_late_night_end_time = LATE_NIGHT_END_TIME_DEFAULT
    
    # --- Lấy tham số từ dictionary cấu hình ---
    p_known_large_tables = config_params.get('p_known_large_tables', KNOWN_LARGE_TABLES_DEFAULT)
    p_time_window_minutes = config_params.get('p_time_window_minutes', TIME_WINDOW_DEFAULT_MINUTES)
    p_min_distinct_tables = config_params.get('p_min_distinct_tables', MIN_DISTINCT_TABLES_THRESHOLD_DEFAULT)
    p_sensitive_tables = config_params.get('p_sensitive_tables', SENSITIVE_TABLES_DEFAULT)
    p_allowed_users_sensitive = config_params.get('p_allowed_users_sensitive', ALLOWED_USERS_FOR_SENSITIVE_DEFAULT)
    p_safe_hours_start = config_params.get('p_safe_hours_start', SAFE_HOURS_START_DEFAULT)
    p_safe_hours_end = config_params.get('p_safe_hours_end', SAFE_HOURS_END_DEFAULT)
    p_safe_weekdays = config_params.get('p_safe_weekdays', SAFE_WEEKDAYS_DEFAULT)
    p_quantile_start = config_params.get('p_quantile_start', QUANTILE_START_DEFAULT)
    p_quantile_end = config_params.get('p_quantile_end', QUANTILE_END_DEFAULT)
    p_min_queries_for_profile = config_params.get('p_min_queries_for_profile', MIN_QUERIES_FOR_PROFILE_DEFAULT)
    # --- Feature Engineering ---
    df_logs['query_lower'] = df_logs['query'].str.lower()
    df_logs['query_length'] = df_logs['query_lower'].str.len()
    write_commands = ['insert', 'update', 'delete', 'replace']
    ddl_commands = ['create', 'alter', 'drop', 'truncate', 'rename']
    df_logs['is_write_query'] = df_logs['query_lower'].str.startswith(tuple(write_commands), na=False)
    df_logs['is_ddl_query'] = df_logs['query_lower'].str.startswith(tuple(ddl_commands), na=False)
    df_logs = df_logs.drop(columns=['query_lower'])
    query_features_df = df_logs['query'].apply(extract_query_features).apply(pd.Series)
    df_logs = pd.concat([df_logs, query_features_df], axis=1)
    
    # --- Phân tích AI (Supervised và Contextual) ---
    feature_cols = [
        'num_joins', 'num_where_conditions', 'num_group_by_cols',
        'num_order_by_cols', 'has_limit', 'has_subquery', 'has_union', 
        'has_where', 'query_length', 'is_write_query', 'is_ddl_query'
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
    anomalies_complexity = df_logs[df_logs['is_complexity_anomaly'] & ~df_logs['is_whitelisted']].copy().reset_index(drop=True)

    # --- Áp dụng các Rules ---
    
    # Tạo một cột boolean để đánh dấu các hoạt động được miễn trừ
    df_logs['is_whitelisted'] = df_logs.apply(is_whitelisted, axis=1)
    
    # Tạo đặc trưng mới
    df_logs['is_maintenance'] = df_logs.apply(lambda row: tag_maintenance_activity(row, whitelists), axis=1)
    
    # Rule 1
    df_logs['is_late_night'] = df_logs['timestamp'].apply(lambda ts: is_late_night_query(ts, p_late_night_start_time, p_late_night_end_time))
    anomalies_late_night = df_logs[df_logs['is_late_night'] & ~df_logs['is_whitelisted']].copy().reset_index(drop=True)
    # Rule 2
    df_logs['is_potential_dump'] = df_logs.apply(lambda row: is_potential_large_dump(row, p_known_large_tables), axis=1)
    anomalies_large_dump = df_logs[df_logs['is_potential_dump'] & ~df_logs['is_whitelisted']].copy().reset_index(drop=True)
    # Rule 3
    df_logs['accessed_tables'] = df_logs['query'].apply(get_tables_with_sqlglot)
    current_time_window = pd.Timedelta(minutes=p_time_window_minutes)
    anomalies_multiple_tables_list = []
    for user, user_df_orig in df_logs.groupby('user'):
        if pd.isna(user) or user_df_orig.empty:
            continue
        
        user_df = user_df_orig.sort_values('timestamp').reset_index(drop=True)
        
        current_session_tables = set()
        current_session_queries_details = []
        
        if user_df.empty:
            continue
        session_start_time = user_df.iloc[0]['timestamp']

        for idx, row in user_df.iterrows():
            query_time = row['timestamp']
            tables_in_this_query_list = row['accessed_tables'] if isinstance(row['accessed_tables'], list) else []
            tables_in_this_query_set = set(tables_in_this_query_list)

            if (query_time - session_start_time) > current_time_window: 
                if len(current_session_tables) >= p_min_distinct_tables:
                    if current_session_queries_details:
                        anomalies_multiple_tables_list.append({
                            'user': user,
                            'start_time': session_start_time,
                            'end_time': current_session_queries_details[-1]['timestamp'], 
                            'distinct_tables_count': len(current_session_tables),
                            'tables_accessed_in_session': sorted(list(current_session_tables)),
                            'queries_details': current_session_queries_details
                        })
                current_session_tables = tables_in_this_query_set
                current_session_queries_details = [{'timestamp': query_time, 'query': row['query'], 'tables_touched': tables_in_this_query_list}]
                session_start_time = query_time
            else:
                current_session_tables.update(tables_in_this_query_set)
                current_session_queries_details.append({'timestamp': query_time, 'query': row['query'], 'tables_touched': tables_in_this_query_list})
        
        if len(current_session_tables) >= p_min_distinct_tables:
             if current_session_queries_details:
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
    # Rule 4
    df_logs['sensitive_access_info'] = df_logs.apply(lambda row: analyze_sensitive_access(row, p_sensitive_tables, p_allowed_users_sensitive, p_safe_hours_start, p_safe_hours_end, p_safe_weekdays), axis=1)
    anomalies_sensitive_access = df_logs[df_logs['sensitive_access_info'].notna()].copy()
    if not anomalies_sensitive_access.empty:
        sensitive_info_expanded = anomalies_sensitive_access['sensitive_access_info'].apply(pd.Series)
        anomalies_sensitive_access = pd.concat([anomalies_sensitive_access.drop(columns=['sensitive_access_info'], errors='ignore'), sensitive_info_expanded], axis=1).reset_index(drop=True)
    # Rule 5
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
                user_activity_profiles[user] = {'active_start': active_start_hour, 'active_end': active_end_hour}
    df_logs['unusual_activity_reason'] = df_logs.apply(lambda row: check_unusual_user_activity_time(row, user_activity_profiles), axis=1)
    anomalies_unusual_user_time = df_logs[df_logs['unusual_activity_reason'].notna() & ~df_logs['is_whitelisted']].copy().reset_index(drop=True)

    # --- Xác định Hoạt động Bình thường ---
    anomalous_indices = set()
    if not anomalies_late_night.empty: anomalous_indices.update(anomalies_late_night.index)
    if not anomalies_large_dump.empty: anomalous_indices.update(anomalies_large_dump.index)
    if not anomalies_sensitive_access.empty: anomalous_indices.update(anomalies_sensitive_access.index)
    if not anomalies_unusual_user_time.empty: anomalous_indices.update(anomalies_unusual_user_time.index)
    if not anomalies_complexity.empty: anomalous_indices.update(anomalies_complexity.index)
    if not anomalies_multiple_tables_df.empty:
        for queries_list in anomalies_multiple_tables_df['queries_details']:
            queries_df = pd.DataFrame(queries_list)
            merged = pd.merge(df_logs.reset_index(), queries_df, on=['timestamp', 'query'])
            if not merged.empty: anomalous_indices.update(merged['index'])
    normal_activities = df_logs[~df_logs.index.isin(anomalous_indices) & ~df_logs['is_whitelisted']].copy()
    
    # --- Xây dựng Dictionary Kết quả ---
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

# === CÁC HÀM HELPER ĐÃ ĐƯỢC TÁI CẤU TRÚC ===

def save_model_and_scaler(model, scaler, path):
    """Lưu mô hình và scaler, sử dụng logging."""
    try:
        joblib.dump({'model': model, 'scaler': scaler}, path)
        logging.info(f"Đã lưu mô hình và scaler vào: {path}")
    except Exception as e:
        logging.error(f"Lỗi khi lưu mô hình tại {path}: {e}")

def load_model_and_scaler(path):
    """Tải mô hình và scaler, sử dụng logging."""
    try:
        if os.path.exists(path):
            data = joblib.load(path)
            logging.info(f"Đã tải mô hình và scaler từ: {path}")
            return data['model'], data['scaler']
        return None, None
    except Exception as e:
        logging.error(f"Lỗi khi tải mô hình tại {path}: {e}")
        return None, None

def train_supervised_model_from_feedback(feature_cols):
    """Đọc dữ liệu phản hồi và huấn luyện mô hình, sử dụng logging."""
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
    """Phân tích độ phức tạp, sử dụng logging."""
    feature_cols = [
        'num_joins', 'num_where_conditions', 'num_group_by_cols',
        'num_order_by_cols', 'has_limit', 'has_subquery',
        'has_union', 'has_where', 'query_length',
        'is_write_query', 'is_ddl_query'
    ]
    df_analysis = df_with_features[feature_cols + ['user']].dropna()
    if df_analysis.empty:
        return pd.Series(name='anomaly_score'), pd.Series(name='is_complexity_anomaly'), pd.Series(name='analysis_type')
    
    # Xử lý mô hình toàn cục
    global_model_path = os.path.join(MODELS_DIR, "global_isolation_forest.joblib")
    global_model, scaler_global = load_model_and_scaler(global_model_path)
    if global_model is None or scaler_global is None:
        logging.info("Không tìm thấy mô hình toàn cục, đang tiến hành huấn luyện lần đầu...")
        scaler_global = StandardScaler()
        X_global_scaled = scaler_global.fit_transform(df_analysis[feature_cols])
        global_model = IsolationForest(contamination=0.05, random_state=42)
        global_model.fit(X_global_scaled)
        save_model_and_scaler(global_model, scaler_global, global_model_path)

    # Xử lý mô hình cho từng user
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