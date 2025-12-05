# engine/db_writer.py
import logging
import os
import sys
from typing import Dict, List, Any
from datetime import datetime
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import json
import numpy as np
import uuid

# Đảm bảo import backend_api khi chạy từ project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from backend_api.models import SessionLocal, AllLogs, Anomaly, AggregateAnomaly  # type: ignore

log = logging.getLogger("DBWriter")
if not log.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [DBWriter] - %(message)s"
    )
    handler.setFormatter(formatter)
    log.addHandler(handler)
log.setLevel(logging.INFO)

# Parquet output directory
PARQUET_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
os.makedirs(PARQUET_OUTPUT_DIR, exist_ok=True)

# ========= PARQUET EXPORT HELPER =========

def save_to_parquet(df: pd.DataFrame, prefix: str):
    """Save DataFrame to Parquet file with timestamp and unique ID"""
    if df is None or df.empty:
        return
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"{prefix}_{timestamp}_{unique_id}.parquet"
        filepath = os.path.join(PARQUET_OUTPUT_DIR, filename)
        
        # Convert timestamp columns to string for Parquet compatibility
        df_export = df.copy()
        for col in df_export.columns:
            if pd.api.types.is_datetime64_any_dtype(df_export[col]):
                df_export[col] = df_export[col].astype(str)
        
        df_export.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
        log.info(f"💾 Saved {len(df)} records to {filename}")
    except Exception as e:
        log.error(f"Failed to save Parquet file {prefix}: {e}")

# ========= SAFE CONVERSION HELPERS =========

def _safe_int(val):
    """Chuyển đổi an toàn sang int (xử lý '0.0', NaN, None)"""
    if val is None: return 0
    try:
        # Nếu là float NaN
        if isinstance(val, (float, np.floating)) and np.isnan(val):
            return 0
        # Ép sang float trước để xử lý chuỗi '0.0', sau đó int
        return int(float(val))
    except (ValueError, TypeError):
        return 0

def _safe_float(val):
    """Chuyển đổi an toàn sang float (xử lý None, string rác)"""
    if val is None: return 0.0
    try:
        if isinstance(val, (float, np.floating)) and np.isnan(val):
            return 0.0
        return float(val)
    except (ValueError, TypeError):
        return 0.0

def _safe_bool(val):
    """Chuyển đổi an toàn sang bool (xử lý '0', 'false', 0.0)"""
    if pd.isna(val) or val is None: return False
    s_val = str(val).lower().strip()
    return s_val in ['1', 'true', 't', 'yes', 'y', '1.0', 'on']

def _to_serializable(obj):
    """Chuyển đổi object sang JSON-safe"""
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    if isinstance(obj, (list, dict)):
        return json.dumps(obj, ensure_ascii=False, default=str)
    return str(obj)

# ========= MAIN SAVING FUNCTION =========

def save_results_to_db(results: Dict[str, Any]):
    """Save all logs + all anomaly types to PostgreSQL"""
    if not results or "all_logs" not in results:
        return

    df_all = results.get("all_logs")
    if df_all is None or df_all.empty:
        return

    # Ensure timestamp is naive datetime
    if 'timestamp' in df_all.columns:
        df_all['timestamp'] = pd.to_datetime(df_all['timestamp'], utc=True)
        df_all['timestamp'] = df_all['timestamp'].dt.tz_localize(None)

    # === 1. Save to AllLogs (rich schema) ===
    all_logs_to_save = df_all.copy()

    # --- DEFINITIONS ---
    # Danh sách cột Boolean (Postgres strict)
    BOOL_COLS = {
        'is_late_night', 'is_work_hours', 'is_select_star', 'has_limit', 
        'has_order_by', 'has_into_outfile', 'has_load_data', 'has_sleep_benchmark',
        'is_risky_command', 'is_admin_command', 'is_potential_dump', 
        'is_suspicious_func', 'is_privilege_change', 'is_system_table',
        'is_sensitive_access', 'is_system_access', 'has_comment', 'has_hex', 
        'is_anomaly', 'has_error'
    }

    # Danh sách cột Integer (Postgres strict, ko nhận '0.0')
    INT_COLS = {
        'created_tmp_disk_tables', 'created_tmp_tables', 'select_full_join', 
        'select_scan', 'sort_merge_passes', 'no_index_used', 'no_good_index_used',
        'error_count', 'warning_count', 'client_port', 'thread_os_id', 
        'rows_returned', 'rows_examined', 'rows_affected', 'num_tables',
        'num_joins', 'num_where_conditions', 'subquery_depth', 'query_length',
        'accessed_sensitive_tables', 'event_id', 'error_code'
    }

    # Danh sách cột Float
    FLOAT_COLS = {
        'execution_time_ms', 'lock_time_ms', 'query_entropy', 'scan_efficiency',
        'ml_anomaly_score', 'query_count_5m', 'error_count_5m', 'total_rows_5m',
        'data_retrieval_speed', 'execution_time_ms_zscore', 'rows_returned_zscore'
    }

    # Map DataFrame columns -> DB columns
    log_mapping = {
        'timestamp': 'timestamp',
        'user': 'user',
        'client_ip': 'client_ip',
        'client_port': 'client_port',
        'connection_type': 'connection_type',
        'thread_os_id': 'thread_os_id',
        'database': 'database',
        'source_dbms': 'source_dbms',
        'query': 'query',
        'normalized_query': 'normalized_query',
        'query_digest': 'query_digest',
        'event_id': 'event_id',
        'event_name': 'event_name',
        'command_type': 'command_type',
        'execution_time_ms': 'execution_time_ms',
        'lock_time_ms': 'lock_time_ms',
        'rows_returned': 'rows_returned',
        'rows_examined': 'rows_examined',
        'rows_affected': 'rows_affected',
        'scan_efficiency': 'scan_efficiency',
        'query_length': 'query_length',
        'query_entropy': 'query_entropy',
        'ml_anomaly_score': 'ml_anomaly_score',
        'analysis_type': 'analysis_type',
        'is_system_table': 'is_system_table',
        'is_admin_command': 'is_admin_command',
        'is_risky_command': 'is_risky_command',
        'has_comment': 'has_comment',
        'has_hex': 'has_hex',
        'is_select_star': 'is_select_star',
        'has_into_outfile': 'has_into_outfile',
        'is_anomaly': 'is_anomaly',
        'created_tmp_disk_tables': 'created_tmp_disk_tables',
        'created_tmp_tables': 'created_tmp_tables',
        'select_full_join': 'select_full_join',
        'select_scan': 'select_scan',
        'sort_merge_passes': 'sort_merge_passes',
        'no_index_used': 'no_index_used',
        'no_good_index_used': 'no_good_index_used',
        'query_count_5m': 'query_count_5m',
        'error_count_5m': 'error_count_5m',
        'total_rows_5m': 'total_rows_5m',
        'data_retrieval_speed': 'data_retrieval_speed',
        'execution_time_ms_zscore': 'execution_time_ms_zscore',
        'rows_returned_zscore': 'rows_returned_zscore',                    
        'num_tables': 'num_tables',
        'num_joins': 'num_joins',
        'num_where_conditions': 'num_where_conditions',
        'subquery_depth': 'subquery_depth',
        'is_sensitive_access': 'is_sensitive_access',
        'is_system_access': 'is_system_access',
        'has_load_data': 'has_load_data',
        'has_sleep_benchmark': 'has_sleep_benchmark',
        'accessed_sensitive_tables': 'accessed_sensitive_tables',
        'accessed_tables': 'accessed_tables',
        'unusual_activity_reason': 'unusual_activity_reason',
        'suspicious_func_name': 'suspicious_func_name',
        'is_suspicious_func': 'is_suspicious_func',
        'is_privilege_change': 'is_privilege_change',
        'privilege_cmd_name': 'privilege_cmd_name',
        'is_late_night': 'is_late_night',
        'is_work_hours': 'is_work_hours',
        'is_potential_dump': 'is_potential_dump',
        'has_limit': 'has_limit',
        'has_order_by': 'has_order_by',
        'error_code': 'error_code',
        'error_message': 'error_message',
        'error_count': 'error_count',
        'warning_count': 'warning_count',
        'has_error': 'has_error'
    }

    # Prepare records with STRICT TYPE CASTING
    records = []
    for idx in all_logs_to_save.index:
        row = all_logs_to_save.loc[idx]
        rec = {}
        
        for src_col, dest_col in log_mapping.items():
            # Lấy giá trị thô
            val = row.get(src_col) # Dùng .get an toàn hơn

            # --- ÉP KIỂU MẠNH (STRICT CASTING) ---
            
            if dest_col in BOOL_COLS:
                rec[dest_col] = _safe_bool(val)
            
            elif dest_col in INT_COLS:
                rec[dest_col] = _safe_int(val)
                
            elif dest_col in FLOAT_COLS:
                rec[dest_col] = _safe_float(val)
            
            elif dest_col == 'timestamp':
                if isinstance(val, (pd.Timestamp, datetime)):
                    rec[dest_col] = val.isoformat()
                else:
                    rec[dest_col] = val
            
            elif isinstance(val, (list, dict, set, np.ndarray)):
                # JSON columns (accessed_tables...)
                rec[dest_col] = json.dumps(val, ensure_ascii=False, default=str) if val is not None else None
            
            else:
                # String columns
                rec[dest_col] = str(val) if pd.notna(val) else None

        # Fallback logic cho is_anomaly (nếu ML chưa set hoặc set sai)
        if not rec.get('is_anomaly'):
            score = float(rec.get('ml_anomaly_score') or 0.0)
            is_bad = (
                score > 0.7 or
                rec.get('is_late_night') or
                rec.get('is_potential_dump') or
                rec.get('is_risky_command') or
                rec.get('is_suspicious_func') or
                rec.get('is_privilege_change') or 
                rec.get('has_error')
            )
            rec['is_anomaly'] = bool(is_bad)
            
        records.append(rec)

    if records:
        # Save to Parquet before PostgreSQL
        save_to_parquet(pd.DataFrame(records), "AllLogs")
        
        try:
            with SessionLocal() as db:
                db.bulk_insert_mappings(AllLogs, records)
                db.commit()
            log.info(f"Saved {len(records)} logs to AllLogs")
        except Exception as e:
            log.error(f"Failed to save AllLogs: {e}", exc_info=True)

    # === 2. Save Event-Level Anomalies ===
    anomaly_frames = []
    for key, df in results.items():
        if not key.startswith("anomalies_") or df is None or df.empty:
            continue
        df_anom = df.copy()
        df_anom['anomaly_type'] = key.replace("anomalies_", "")
        df_anom['score'] = df_anom.get('ml_anomaly_score', 1.0)
        df_anom['reason'] = df_anom.get('unusual_activity_reason', f"Rule: {key}")
        anomaly_frames.append(df_anom)

    if anomaly_frames:
        df_anomalies = pd.concat(anomaly_frames, ignore_index=True)
        
        # Fix timestamp
        if 'timestamp' in df_anomalies.columns:
            df_anomalies['timestamp'] = pd.to_datetime(df_anomalies['timestamp'], utc=True).dt.tz_localize(None)

        anomaly_records = []
        for _, row in df_anomalies.iterrows():
            rec = {
                'timestamp': row.get('timestamp'),
                'user': row.get('user'),
                'client_ip': row.get('client_ip'),
                'database': row.get('database'),
                'query': str(row.get('query', '')),
                'anomaly_type': row.get('anomaly_type', 'unknown'),
                'score': _safe_float(row.get('score')),
                'reason': str(row.get('reason', ''))[:500],
                'status': 'new',
                'execution_time_ms': _safe_float(row.get('execution_time_ms')),
                'rows_returned': _safe_int(row.get('rows_returned')),
                'rows_affected': _safe_int(row.get('rows_affected')),
            }
            # Loại bỏ các bản ghi không có timestamp
            if pd.notna(rec['timestamp']):
                anomaly_records.append(rec)

        if anomaly_records:
            # Save to Parquet before PostgreSQL
            save_to_parquet(pd.DataFrame(anomaly_records), "Anomalies")
            
            try:
                with SessionLocal() as db:
                    db.bulk_insert_mappings(Anomaly, anomaly_records)
                    db.commit()
                log.info(f"Saved {len(anomaly_records)} event-level anomalies")
            except Exception as e:
                log.error(f"Failed to save Anomalies: {e}", exc_info=True)

    # === 3. Save Session-Level (Aggregate) Anomalies ===
    if "anomalies_multi_table" in results:
        df_agg = results["anomalies_multi_table"]
        if not df_agg.empty:
            agg_records = []
            for _, row in df_agg.iterrows():
                details = {
                    "tables": row.get("tables_accessed_in_session", []),
                    "query_count": len(row.get("queries_details", [])),
                    "duration_sec": (row['end_time'] - row['start_time']).total_seconds() if pd.notna(row['start_time']) and pd.notna(row['end_time']) else 0
                }
                agg_records.append({
                    'scope': 'session',
                    'user': row.get('user'),
                    'database': None,
                    'start_time': pd.to_datetime(row['start_time']).tz_localize(None) if pd.notna(row['start_time']) else None,
                    'end_time': pd.to_datetime(row['end_time']).tz_localize(None) if pd.notna(row['end_time']) else None,
                    'anomaly_type': 'multi_table_access',
                    'severity': float(row.get('distinct_tables_count', 0)),
                    'reason': f"Accessed {row.get('distinct_tables_count', 0)} tables in short window",
                    'details': details
                })

            if agg_records:
                # Save to Parquet before PostgreSQL
                save_to_parquet(pd.DataFrame(agg_records), "AggregateAnomalies")
                
                try:
                    with SessionLocal() as db:
                        db.bulk_insert_mappings(AggregateAnomaly, agg_records)
                        db.commit()
                    log.info(f"Saved {len(agg_records)} session-level anomalies")
                except Exception as e:
                    log.error(f"Failed to save AggregateAnomaly: {e}", exc_info=True)