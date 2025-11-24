# backend_api/models.py
import os
import sys
from sqlalchemy import (create_engine, Column, Integer, String, DateTime,
                        Float, Boolean, Text, Index, BigInteger, JSON, func)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB

# Add project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

# --- Database Setup ---
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# =============================================================================
# 1. Anomaly (event-level)
# =============================================================================
class Anomaly(Base):
    __tablename__ = 'anomalies'
   
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    user = Column(String, index=True)
    client_ip = Column(String)
    database = Column(String, nullable=True)
    query = Column(Text, nullable=False)
   
    anomaly_type = Column(String, index=True, nullable=False)
    score = Column(Float, nullable=True)
    reason = Column(Text, nullable=True)
    status = Column(String, default='new', index=True)
   
    execution_time_ms = Column(Float, nullable=True, server_default='0')
    rows_returned = Column(BigInteger, nullable=True, server_default='0')
    rows_affected = Column(BigInteger, nullable=True, server_default='0')


# =============================================================================
# 2. Aggregate Anomaly (session-level, multi-table, etc.)
# =============================================================================
class AggregateAnomaly(Base):
    __tablename__ = 'aggregate_anomalies'
    id = Column(Integer, primary_key=True, index=True)
    scope = Column(String, nullable=False)
    user = Column(String, index=True, nullable=True)
    database = Column(String, nullable=True)
    start_time = Column(DateTime, index=True, nullable=True)
    end_time = Column(DateTime, index=True, nullable=True)
    anomaly_type = Column(String, index=True, nullable=True)
    severity = Column(Float, nullable=True)
    reason = Column(Text, nullable=True)
    details = Column(JSONB, nullable=True)
    created_at = Column(DateTime, server_default=func.now())


# =============================================================================
# 3. AllLogs — NOW FULLY UPGRADED FOR 2025 UBA
# =============================================================================
class AllLogs(Base):
    __tablename__ = 'all_logs'
   
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    user = Column(String, index=True)
    client_ip = Column(String)
    database = Column(String, nullable=True)
    query = Column(Text, nullable=False)

    # === ORIGINAL COLUMNS (now fully supported) ===
    source_dbms = Column(String, default="MySQL")
    error_code = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    execution_time_ms = Column(Float, nullable=True, server_default='0')  # from execution_time_sec * 1000
    rows_returned = Column(BigInteger, nullable=True, server_default='0')
    rows_affected = Column(BigInteger, nullable=True, server_default='0')

    # === ANALYSIS RESULT ===
    is_anomaly = Column(Boolean, default=False, index=True)
    analysis_type = Column(String, nullable=True)

    # === NEW 2025 RESEARCH-GRADE FEATURES (from enhance_features_batch) ===
    ml_anomaly_score = Column(Float, default=0.0, index=True)  # Main AI score

    # Temporal
    is_late_night = Column(Boolean, default=False)
    is_work_hours = Column(Boolean, default=False)

    # Query Syntax & Risk
    query_length = Column(Integer, nullable=True)
    query_entropy = Column(Float, nullable=True)
    num_tables = Column(Integer, default=0)
    num_joins = Column(Integer, default=0)
    num_where_conditions = Column(Integer, default=0)
    has_limit = Column(Boolean, default=False)
    has_order_by = Column(Boolean, default=False)
    is_select_star = Column(Boolean, default=False)
    has_into_outfile = Column(Boolean, default=False)
    has_load_data = Column(Boolean, default=False)
    has_sleep_benchmark = Column(Boolean, default=False)

    # Risk Flags
    is_risky_command = Column(Boolean, default=False)
    is_admin_command = Column(Boolean, default=False)
    accessed_sensitive_tables = Column(Integer, default=0)
    accessed_tables = Column(JSONB, nullable=True)  # list of tables

    # Behavioral
    execution_time_ms_zscore = Column(Float, nullable=True)
    rows_returned_zscore = Column(Float, nullable=True)

    # Detection Flags
    is_potential_dump = Column(Boolean, default=False)
    is_suspicious_func = Column(Boolean, default=False)
    suspicious_func_name = Column(String, nullable=True)
    is_privilege_change = Column(Boolean, default=False)
    privilege_cmd_name = Column(String, nullable=True)
    unusual_activity_reason = Column(Text, nullable=True)

    # Command type (for filtering)
    command_type = Column(String, nullable=True)  # SELECT, DELETE, GRANT, etc.
    
    normalized_query = Column(Text, nullable=True)
    query_digest = Column(String(64), nullable=True, index=True)  # For fast grouping
    error_count = Column(Integer, default=0)
    warning_count = Column(Integer, default=0)
    created_tmp_disk_tables = Column(Integer, default=0)
    no_index_used = Column(Integer, default=0)

    # Indexes for performance
    __table_args__ = (
        Index('ix_all_logs_user_timestamp', 'user', 'timestamp'),
        Index('ix_all_logs_anomaly_score', 'ml_anomaly_score'),
        Index('ix_all_logs_is_anomaly', 'is_anomaly'),
    )