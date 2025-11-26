import logging
import os
import sys
from typing import Dict, List, Any
from datetime import datetime
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import json
import numpy as np

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


# ========= HELPERS =========

def _coerce_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=False)


def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def _normalize_all_logs(df_logs: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn hóa cho bảng AllLogs:
    - Mỗi record = 1 log event thật.
    - Không tự tạo / nhân bản.
    """
    df = df_logs.copy()

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Nếu là tz-aware -> convert về naive (giữ local time)
    if hasattr(df['timestamp'].dtype, 'tz') and df['timestamp'].dt.tz is not None:
        df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    
    df = df[df['timestamp'].notna()].copy()  # vì AllLogs.timestamp NOT NULL

    for col in ['user', 'client_ip', 'database', 'query']:
        if col not in df.columns:
            df[col] = None

    df['query'] = df['query'].astype(str)

    for col, default in [
        ('execution_time_ms', 0.0),
        ('rows_returned', 0),
        ('rows_affected', 0),
    ]:
        if col not in df.columns:
            df[col] = default
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(default)

    if 'is_anomaly' not in df.columns:
        df['is_anomaly'] = False
    if 'analysis_type' not in df.columns:
        df['analysis_type'] = None

    valid_cols = set(AllLogs.__table__.columns.keys())
    df = df[[c for c in df.columns if c in valid_cols]]
    return df


# def _collect_anomaly_frames(results: Dict[str, Any]) -> List[pd.DataFrame]:
#     frames: List[pd.DataFrame] = []
#     for key, value in results.items():
#         if key.startswith("anomalies_") and isinstance(value, pd.DataFrame) and not value.empty:
#             anomaly_type = key.replace("anomalies_", "") or "unknown"
#             df = value.copy()
#             df["anomaly_type"] = anomaly_type
#             frames.append(df)
#     return frames


# def _normalize_anomalies_only_from_all_logs(
#     frames: List[pd.DataFrame],
#     df_all_logs_norm: pd.DataFrame,
# ) -> pd.DataFrame:
#     """
#     Chuẩn hóa anomalies_* thành bảng Anomaly theo ĐÚNG yêu cầu:

#     - Chỉ giữ anomaly là LOG có thật trong all_logs.
#     - Tức là: anomaly phải map được tới ít nhất 1 dòng trong df_all_logs_norm
#       theo các cột join chung (timestamp/user/database/query).
#     - Những dòng aggregate (multi_table, session profile...) không map được
#       => không insert vào Anomaly (nếu muốn giữ, sau này làm bảng khác).
#     """
#     if not frames:
#         return pd.DataFrame()

#     raw = pd.concat(frames, ignore_index=True).copy()

#     # Chuẩn core columns
#     for col in ("timestamp", "user", "client_ip", "database", "query", "anomaly_type"):
#         if col not in raw.columns:
#             if col == "anomaly_type":
#                 continue
#             raw[col] = None

#     # Chuẩn timestamp từ nhiều nguồn
#     raw["timestamp"] = _coerce_datetime(raw["timestamp"])
#     for alt in ("session_start", "start_time", "first_seen", "first_timestamp"):
#         if alt in raw.columns:
#             alt_ts = _coerce_datetime(raw[alt])
#             mask = raw["timestamp"].isna() & alt_ts.notna()
#             if mask.any():
#                 raw.loc[mask, "timestamp"] = alt_ts[mask]

#     # Chuẩn text NaN -> None
#     for col in ("user", "client_ip", "database", "query", "anomaly_type"):
#         if col in raw.columns:
#             raw[col] = raw[col].where(raw[col].notna(), None)

#     # ----- Reason -----
#     reason_candidates = [
#         "reason",
#         "violation_reason",
#         "unusual_activity_reason",
#         "deviation_reasons",
#         "rule_reason",
#         "explanation",
#         "detail",
#     ]
#     if "reason" not in raw.columns:
#         raw["reason"] = pd.NA

#     if raw["reason"].isna().all():
#         for col in reason_candidates:
#             if col in raw.columns:
#                 mask = raw[col].notna()
#                 if mask.any():
#                     raw.loc[mask, "reason"] = raw.loc[mask, col].astype(str)
#                     break
#     raw["reason"] = raw["reason"].where(raw["reason"].notna(), None)

#     # ----- Score -----
#     score_candidates = ["score", "anomaly_score", "deviation_score", "risk_score", "confidence"]
#     if "score" not in raw.columns:
#         raw["score"] = pd.NA

#     if raw["score"].isna().all():
#         for col in score_candidates:
#             if col in raw.columns and raw[col].notna().any():
#                 raw["score"] = _coerce_numeric(raw[col])
#                 break
#     raw["score"] = raw["score"].where(pd.notna(raw["score"]), None)

#     # ----- Status -----
#     if "status" not in raw.columns:
#         raw["status"] = "new"
#     raw["status"] = raw["status"].where(raw["status"].notna(), "new")

#     # Perf fields
#     for col in ("execution_time_ms", "rows_returned", "rows_affected"):
#         if col in raw.columns:
#             raw[col] = _coerce_numeric(raw[col])
#         else:
#             raw[col] = 0

#     # Map chỉ những record map được vào all_logs

#     # Các cột dùng làm key join (tùy theo cái nào tồn tại)
#     join_cols = [c for c in ["timestamp", "user", "database", "query"]
#                  if c in df_all_logs_norm.columns and c in raw.columns]

#     if not join_cols:
#         # Không join được, fallback: bỏ những record không có timestamp hoặc query
#         mask_valid = raw["timestamp"].notna() & raw["query"].notna()
#         dropped = len(raw) - mask_valid.sum()
#         if dropped > 0:
#             log.info(f"[DBWriter] Bỏ {dropped} anomaly vì không map được với all_logs.")
#         filtered = raw[mask_valid].copy()
#     else:
#         # Tạo key set cho all_logs
#         keys_all = set(
#             tuple(row[c] for c in join_cols)
#             for _, row in df_all_logs_norm.iterrows()
#         )

#         def is_from_all_logs(row) -> bool:
#             key = tuple(row[c] for c in join_cols)
#             return key in keys_all

#         mask = raw.apply(is_from_all_logs, axis=1)
#         dropped = len(raw) - mask.sum()
#         if dropped > 0:
#             log.info(
#                 f"[DBWriter] Bỏ {dropped} anomaly (aggregate/không khớp all_logs). "
#                 f"Giữ {mask.sum()} anomaly là log thật."
#             )
#         filtered = raw[mask].copy()

#     if filtered.empty:
#         return filtered

#     # Đảm bảo không còn NaT bị stringify
#     filtered["timestamp"] = filtered["timestamp"].astype("object")
#     filtered.loc[pd.isna(filtered["timestamp"]), "timestamp"] = None

#     # Giữ đúng cột model
#     valid_cols = set(Anomaly.__table__.columns.keys())
#     keep = [c for c in filtered.columns if c in valid_cols]
#     filtered = filtered[keep]

#     # Dedup tránh spam do retry
#     dedup_keys = [c for c in [
#         "timestamp", "user", "client_ip", "database",
#         "query", "anomaly_type", "reason", "score"
#     ] if c in filtered.columns]
#     if dedup_keys:
#         before = len(filtered)
#         filtered = filtered.drop_duplicates(subset=dedup_keys)
#         if len(filtered) < before:
#             log.info(f"[DBWriter] Bỏ {before - len(filtered)} anomaly trùng lặp hoàn toàn.")

#     return filtered


# def _mark_anomalies_in_all_logs(df_all: pd.DataFrame, df_anom: pd.DataFrame) -> pd.DataFrame:
#     """
#     Đánh dấu is_anomaly=True cho những log xuất hiện trong bảng anomalies.
#     """
#     if df_all.empty or df_anom.empty:
#         return df_all

#     df = df_all.copy()
#     join_cols = [c for c in ["timestamp", "user", "database", "query"]
#                  if c in df.columns and c in df_anom.columns]
#     if not join_cols:
#         return df

#     anom_keys = set(
#         tuple(row[c] for c in join_cols)
#         for _, row in df_anom.iterrows()
#     )

#     def is_anom(row) -> bool:
#         key = tuple(row[c] for c in join_cols)
#         return key in anom_keys

#     df["is_anomaly"] = df.apply(is_anom, axis=1) | df.get("is_anomaly", False)
#     return df


# ========= MAIN =========

# def save_results_to_db(results: Dict[str, Any]) -> Tuple[int, int, int]:
#     df_all = results.get("all_logs")
#     if not isinstance(df_all, pd.DataFrame) or df_all.empty:
#         log.error("Không có all_logs hợp lệ.")
#         return 0, 0, 0

#     df_all_norm = _normalize_all_logs(df_all)

#     # Event anomalies từ các rule log-level
#     df_event_anoms = _build_event_anomalies(results, df_all_norm)

#     # Multi-table: tách event + aggregate
#     df_mt_events, mt_agg_rows = _build_multi_table_anomalies(results, df_all_norm)

#     # Gộp tất cả event-level anomalies
#     df_all_anoms = pd.concat(
#         [df_event_anoms, df_mt_events],
#         ignore_index=True
#     ) if (not df_event_anoms.empty or not df_mt_events.empty) else pd.DataFrame()

#     # Đánh dấu is_anomaly trong all_logs
#     if not df_all_anoms.empty:
#         join_cols = ["timestamp", "user", "database", "query"]
#         join_cols = [c for c in join_cols if c in df_all_norm.columns and c in df_all_anoms.columns]

#         anom_keys = set(
#             tuple(row[c] for c in join_cols)
#             for _, row in df_all_anoms.iterrows()
#         )

#         def is_anom(row):
#             return tuple(row[c] for c in join_cols) in anom_keys

#         df_all_norm["is_anomaly"] = df_all_norm.apply(is_anom, axis=1)

#     db = SessionLocal()
#     logs_count = anoms_count = agg_count = 0

#     try:
#         # insert all_logs
#         all_log_records = df_all_norm.where(pd.notna(df_all_norm), None).to_dict("records")
#         db.bulk_insert_mappings(AllLogs, all_log_records)
#         logs_count = len(all_log_records)

#         # insert anomalies (event-level)
#         if not df_all_anoms.empty:
#             anoms_records = df_all_anoms.where(pd.notna(df_all_anoms), None).to_dict("records")
#             db.bulk_insert_mappings(Anomaly, anoms_records)
#             anoms_count = len(anoms_records)

#         # insert aggregate anomalies
#         if mt_agg_rows:
#             db.bulk_insert_mappings(AggregateAnomaly, mt_agg_rows)
#             agg_count = len(mt_agg_rows)

#         db.commit()
#         log.info(
#             f"Lưu OK: {logs_count} all_logs, {anoms_count} anomalies, {agg_count} aggregate_anomalies."
#         )
#     except Exception as e:
#         db.rollback()
#         log.error(f"Lỗi khi lưu: {e}", exc_info=True)
#         raise
#     finally:
#         db.close()

#     return logs_count, anoms_count, agg_count


def _build_event_anomalies(results: Dict[str, Any],
                           df_all_logs: pd.DataFrame) -> pd.DataFrame:
    """Gom các anomalies_* log-level thành DataFrame cho bảng Anomaly."""
    frames = []
    for key, value in results.items():
        if not key.startswith("anomalies_"):
            continue
        if key == "anomalies_multi_table":
            continue  # xử lý riêng
        if isinstance(value, pd.DataFrame) and not value.empty:
            df = value.copy()
            df["anomaly_type"] = key.replace("anomalies_", "")
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Chuẩn hóa core fields
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df[df["timestamp"].notna()]  # vì Anomaly.timestamp NOT NULL
    df["timestamp"] = df["timestamp"].apply(_strip_tz)

    df["query"] = df["query"].astype(str)
    # df = df[df["query"].notna() & (df["query"] != "")]
    df["query"] = df["query"].str.strip().str.replace(r"\s+", " ", regex=True)

    for col in ["user", "client_ip", "database"]:
        if col not in df.columns:
            df[col] = None

    # Reason / score nếu có
    if "reason" not in df.columns:
        df["reason"] = None
    if "score" not in df.columns:
        df["score"] = None

    if "status" not in df.columns:
        df["status"] = "new"

    for col in ("execution_time_ms", "rows_returned", "rows_affected"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            df[col] = 0

    # Chỉ giữ anomaly là subset của all_logs
    join_cols = [c for c in ["timestamp", "user", "database", "query"]
                 if c in df_all_logs.columns and c in df.columns]

    if join_cols:
        keys_all = set(
            tuple(row[c] for c in join_cols)
            for _, row in df_all_logs.iterrows()
        )

        def is_in_all_logs(row):
            return tuple(row[c] for c in join_cols) in keys_all

        mask = df.apply(is_in_all_logs, axis=1)
        dropped = len(df) - mask.sum()
        if dropped > 0:
            log.info(f"[DBWriter] Bỏ {dropped} anomaly không khớp all_logs.")
        df = df[mask].copy()

    valid_cols = set(Anomaly.__table__.columns.keys())
    df = df[[c for c in df.columns if c in valid_cols]]

    df = df.drop_duplicates(
        subset=["timestamp", "user", "database", "query", "anomaly_type", "reason", "score"]
    )
    return df


def _strip_tz(ts):
    """Đưa pandas.Timestamp hoặc datetime về naive datetime (không timezone)."""
    if isinstance(ts, pd.Timestamp):
        # Nếu có tz thì bỏ, sau đó convert sang datetime thường
        if ts.tzinfo is not None:
            ts = ts.tz_localize(None)
        return ts.to_pydatetime()
    if isinstance(ts, datetime):
        if ts.tzinfo is not None:
            ts = ts.replace(tzinfo=None)
        return ts
    return ts

def _to_serializable(obj):
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    if pd.isna(obj):
        return None
    if isinstance(obj, (list, dict)):
        import json
        return json.dumps(obj, ensure_ascii=False, default=str)
    return str(obj)


def save_results_to_db(results: Dict[str, Any]):
    """Save all logs + all anomaly types to PostgreSQL"""
    if not results or "all_logs" not in results:
        log.warning("No results to save.")
        return

    df_all = results.get("all_logs")
    if df_all is None or df_all.empty:
        log.info("No logs to save.")
        return

    # Ensure timestamp is naive datetime
    if 'timestamp' in df_all.columns:
        df_all['timestamp'] = pd.to_datetime(df_all['timestamp'], utc=True)
        df_all['timestamp'] = df_all['timestamp'].dt.tz_localize(None)

    # === 1. Save to AllLogs (rich schema) ===
    all_logs_to_save = df_all.copy()

    # Map to AllLogs model columns
    log_mapping = {
        # Identity
        'timestamp': 'timestamp',
        'user': 'user',
        'client_ip': 'client_ip',
        'client_port': 'client_port',          
        'connection_type': 'connection_type',  
        'thread_os_id': 'thread_os_id',        
        'database': 'database',
        'source_dbms': 'source_dbms',

        # Content
        'query': 'query',
        'normalized_query': 'normalized_query',
        'query_digest': 'query_digest',
        'event_id': 'event_id',       
        'event_name': 'event_name',   
        'command_type': 'command_type',

        # Metrics
        'execution_time_ms': 'execution_time_ms',
        'lock_time_ms': 'lock_time_ms',      
        'rows_returned': 'rows_returned',
        'rows_examined': 'rows_examined',     
        'rows_affected': 'rows_affected',
        'scan_efficiency': 'scan_efficiency', 

        # Analysis
        'query_length': 'query_length',
        'query_entropy': 'query_entropy',
        'ml_anomaly_score': 'ml_anomaly_score',
        
        # Flags
        'is_system_table': 'is_system_table',  
        'is_admin_command': 'is_admin_command',
        'is_risky_command': 'is_risky_command',
        'has_comment': 'has_comment',
        'has_hex': 'has_hex',
        'is_select_star': 'is_select_star',
        'has_into_outfile': 'has_into_outfile',
        'is_anomaly': 'is_anomaly',
        'analysis_type': 'analysis_type',

        # Optimizer
        'created_tmp_disk_tables': 'created_tmp_disk_tables',
        'created_tmp_tables': 'created_tmp_tables',
        'select_full_join': 'select_full_join',
        'select_scan': 'select_scan',               
        'sort_merge_passes': 'sort_merge_passes',  
        'no_index_used': 'no_index_used',
        'no_good_index_used': 'no_good_index_used',

        # Contextual
        'query_count_5m': 'query_count_5m',
        'error_count_5m': 'error_count_5m',
        'total_rows_5m': 'total_rows_5m',
        'data_retrieval_speed': 'data_retrieval_speed',
        
        # Behavioral
        'execution_time_ms_zscore': 'execution_time_ms_zscore',
        'rows_returned_zscore': 'rows_returned_zscore',

        # Rule-based extras
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

        # Error Info
        'error_code': 'error_code',
        'error_message': 'error_message',
        'error_count': 'error_count',
        'warning_count': 'warning_count',
    }

    # Prepare records
    records = []
    for idx in all_logs_to_save.index:
        row = all_logs_to_save.loc[idx]
        rec = {}
        
        for src_col, dest_col in log_mapping.items():
            if src_col not in row.index:
                val = None
            else:
                val = row[src_col]
                
                if isinstance(val, (list, dict, set, np.ndarray)) and len(val) == 0:
                    val = None
                elif isinstance(val, (pd.Series, pd.DataFrame)):
                    val = val.iloc[0] if len(val) > 0 else None
                    
                # BOOLEAN COLUMNS — ÉP VỀ True/False
                if dest_col in ['is_late_night', 'is_work_hours', 'is_select_star', 'has_limit', 
                            'has_order_by', 'has_into_outfile', 'has_load_data', 'has_sleep_benchmark',
                            'is_risky_command', 'is_admin_command', 'is_potential_dump', 
                            'is_suspicious_func', 'is_privilege_change', 'is_system_table',
                            'is_sensitive_access', 'is_system_access', 'has_comment', 'has_hex', 'is_anomaly']:
                    if val in (1, '1', 'True', True, 'true', 'T'):
                        val = True
                    elif val in (0, '0', 'False', False, 'false', 'F', None):
                        val = False
                    else:
                        val = bool(val)
                elif pd.isna(val):
                    val = None
                elif isinstance(val, (list, dict, set)):
                    val = json.dumps(val, ensure_ascii=False, default=str)
                elif isinstance(val, (pd.Timestamp, datetime)):
                    val = val.isoformat()
                elif isinstance(val, float) and dest_col in ['ml_anomaly_score', 'query_entropy', 'scan_efficiency']:
                    val = float(val) if not pd.isna(val) else None
                else:
                    val = str(val) if pd.notna(val) else None
            
            rec[dest_col] = val

        # Fallback logic for is_anomaly if not set by ML
        if rec.get('is_anomaly') is None:
            score = float(rec.get('ml_anomaly_score', 0) or 0)
            rec['is_anomaly'] = bool(
                score > 0.7 or
                rec.get('is_late_night') or
                rec.get('is_potential_dump') or
                rec.get('is_risky_command') or
                rec.get('is_suspicious_func') or
                rec.get('is_privilege_change')
            )
        records.append(rec)
        
    if records:
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
        df_anomalies['timestamp'] = pd.to_datetime(df_anomalies['timestamp'], utc=True).dt.tz_localize(None)

        anomaly_records = []
        for _, row in df_anomalies.iterrows():
            rec = {
                'timestamp': row['timestamp'],
                'user': row.get('user'),
                'client_ip': row.get('client_ip'),
                'database': row.get('database'),
                'query': str(row.get('query', '')),
                'anomaly_type': row.get('anomaly_type', 'unknown'),
                'score': float(row.get('score', 1.0)) if pd.notna(row.get('score')) else None,
                'reason': str(row.get('reason', ''))[:500],
                'status': 'new',
                'execution_time_ms': float(row.get('execution_time_ms', 0)) if pd.notna(row.get('execution_time_ms')) else 0,
                'rows_returned': int(row.get('rows_returned', 0)),
                'rows_affected': int(row.get('rows_affected', 0)),
            }
            anomaly_records.append(rec)

        if anomaly_records:
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
                    'details': _to_serializable(details)
                })

            if agg_records:
                try:
                    with SessionLocal() as db:
                        db.bulk_insert_mappings(AggregateAnomaly, agg_records)
                        db.commit()
                    log.info(f"Saved {len(agg_records)} session-level anomalies")
                except Exception as e:
                    log.error(f"Failed to save AggregateAnomaly: {e}", exc_info=True)


def _build_multi_table_anomalies(results: Dict[str, Any],
                                 df_all_logs: pd.DataFrame):
    """
    Từ anomalies_multi_table:
    - Tạo event-level anomalies (Anomaly) cho từng query trong session.
    - Tạo AggregateAnomaly cho toàn session (details là JSON-safe).
    """
    df = results.get("anomalies_multi_table")
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(), []

    event_rows = []
    agg_rows = []

    for _, row in df.iterrows():
        user = row.get("user")

        start_time = pd.to_datetime(row.get("start_time"), errors="coerce")
        end_time = pd.to_datetime(row.get("end_time"), errors="coerce")
        start_time = _strip_tz(start_time) if start_time is not pd.NaT else None
        end_time = _strip_tz(end_time) if end_time is not pd.NaT else None

        distinct_count = int(row.get("distinct_tables_count", 0))

        # Có thể là list hoặc NaN
        tables = row.get("tables_accessed_in_session") or []
        queries_details = row.get("queries_details") or []

        # Đảm bảo details không còn Timestamp
        details = {
            "tables": _to_serializable(tables),
            "queries": _to_serializable(queries_details),
        }

        # -------- Aggregate anomaly (session-level) --------
        reason = (
            f"User {user} truy cập {distinct_count} bảng khác nhau "
            f"từ {start_time} đến {end_time}."
        )

        agg_rows.append({
            "scope": "session",
            "user": user,
            "database": None,
            "start_time": start_time,
            "end_time": end_time,
            "anomaly_type": "multi_table",
            "severity": float(distinct_count),
            "reason": reason,
            "details": details,  # JSON-safe
        })

        # -------- Event-level anomalies cho từng query --------
        for q in queries_details:
            # mỗi q là dict: {timestamp, query, tables_touched, ...}
            ts = pd.to_datetime(q.get("timestamp"), errors="coerce")
            ts = _strip_tz(ts)
            qtext = q.get("query")

            if not ts or pd.isna(ts) or not qtext:
                continue

            # tìm thông tin chi tiết trong all_logs (đã normalize)
            match = df_all_logs[
                (df_all_logs["timestamp"] == ts) &
                (df_all_logs["user"] == user) &
                (df_all_logs["query"] == qtext)
            ]

            if not match.empty:
                m = match.iloc[0]
                event_rows.append({
                    "timestamp": ts,
                    "user": user,
                    "client_ip": m.get("client_ip"),
                    "database": m.get("database"),
                    "query": qtext,
                    "anomaly_type": "multi_table",
                    "score": None,
                    "reason": reason,
                    "status": "new",
                    "execution_time_ms": m.get("execution_time_ms", 0),
                    "rows_returned": m.get("rows_returned", 0),
                    "rows_affected": m.get("rows_affected", 0),
                })

    # DataFrame cho bảng Anomaly (event-level từ multi_table)
    df_events = pd.DataFrame(event_rows) if event_rows else pd.DataFrame()
    if not df_events.empty:
        valid_cols = set(Anomaly.__table__.columns.keys())
        df_events = df_events[[c for c in df_events.columns if c in valid_cols]]

    # agg_rows là list[dict] JSON-safe dùng cho AggregateAnomaly
    return df_events, agg_rows


