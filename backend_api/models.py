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
try:
    from config import DATABASE_URL
except ImportError:
    # Fallback if config import fails (for standalone testing)
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://uba_user:password@localhost:5432/uba_db")

# --- Database Setup ---
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    role = Column(String, default="analyst") # admin, analyst
    is_active = Column(Boolean, default=True)

class Anomaly(Base):
    __tablename__ = 'anomalies'
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    user = Column(String, index=True)
    client_ip = Column(String)
    database = Column(String, nullable=True)
    query = Column(Text, nullable=False)
    anomaly_type = Column(String, index=True, nullable=False)
    behavior_group = Column(String, index=True, nullable=True)
    score = Column(Float, nullable=True)
    reason = Column(Text, nullable=True)
    status = Column(String, default='new', index=True)
    execution_time_ms = Column(Float, nullable=True, server_default='0')
    rows_returned = Column(BigInteger, nullable=True, server_default='0')
    rows_affected = Column(BigInteger, nullable=True, server_default='0')
    # ai_analysis = Column(JSON, nullable=True)

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
    # ai_analysis = Column(JSON, nullable=True)

class AllLogs(Base):
    __tablename__ = 'all_logs'
   
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    
    # --- Identity ---
    user = Column(String, index=True)
    client_ip = Column(String)
    client_port = Column(Integer, nullable=True) 
    connection_type = Column(String, nullable=True) 
    thread_os_id = Column(Integer, nullable=True)  
    database = Column(String, nullable=True)
    source_dbms = Column(String, default="MySQL")

    # --- Content ---
    query = Column(Text, nullable=False)
    query_digest = Column(String, nullable=True)
    normalized_query = Column(Text, nullable=True)
    command_type = Column(String, nullable=True)
    
    # --- Traceability ---
    event_id = Column(BigInteger, nullable=True) 
    event_name = Column(String, nullable=True)   

    # --- Metrics ---
    execution_time_ms = Column(Float, nullable=True, server_default='0')
    lock_time_ms = Column(Float, nullable=True, server_default='0') # [NEW]
    rows_returned = Column(BigInteger, nullable=True, server_default='0')
    rows_examined = Column(BigInteger, nullable=True, server_default='0') # [NEW]
    rows_affected = Column(BigInteger, nullable=True, server_default='0')
    
    # --- Analysis Features ---
    query_length = Column(Integer, nullable=True)
    query_entropy = Column(Float, nullable=True)
    scan_efficiency = Column(Float, nullable=True) # [NEW]
    
    # --- Flags & Indicators ---
    is_system_table = Column(Boolean, default=False) # [NEW]
    is_admin_command = Column(Boolean, default=False)
    is_risky_command = Column(Boolean, default=False)
    has_comment = Column(Boolean, default=False)
    has_hex = Column(Boolean, default=False)
    
    # --- Optimizer Metrics ---
    created_tmp_disk_tables = Column(Integer, default=0)
    created_tmp_tables = Column(Integer, default=0) # [NEW]
    select_full_join = Column(Integer, default=0)
    select_scan = Column(Integer, default=0)        # [NEW]
    sort_merge_passes = Column(Integer, default=0)  # [NEW]
    no_index_used = Column(Integer, default=0)
    no_good_index_used = Column(Integer, default=0)

    # --- ML Results ---
    is_anomaly = Column(Boolean, default=False) 
    ml_anomaly_score = Column(Float, default=0.0)
    analysis_type = Column(String, nullable=True)
    
    behavior_group = Column(String, nullable=True, index=True) 
    specific_rule = Column(String, nullable=True, index=True)
    
# 3. Contextual / Velocity (Rolling Windows) ---
    query_count_5m = Column(Float, nullable=True)
    error_count_5m = Column(Float, nullable=True)
    total_rows_5m = Column(Float, nullable=True)
    data_retrieval_speed = Column(Float, nullable=True) # rows / time
    
    # --- Behavioral Z-Scores ---
    execution_time_ms_zscore = Column(Float, nullable=True)
    rows_returned_zscore = Column(Float, nullable=True)

    # --- Features for Rule-based ---
    num_tables = Column(Integer, default=0)
    num_joins = Column(Integer, default=0)
    num_where_conditions = Column(Integer, default=0)
    subquery_depth = Column(Integer, default=0)
    is_sensitive_access = Column(Boolean, default=False)
    is_system_access = Column(Boolean, default=False)
    is_select_star = Column(Boolean, default=False)
    has_into_outfile = Column(Boolean, default=False)
    has_load_data = Column(Boolean, default=False)
    has_sleep_benchmark = Column(Boolean, default=False)
    accessed_sensitive_tables = Column(Integer, default=0)
    accessed_tables = Column(JSONB, nullable=True)
    unusual_activity_reason = Column(Text, nullable=True)
    suspicious_func_name = Column(String, nullable=True)
    is_suspicious_func = Column(Boolean, default=False)
    is_privilege_change = Column(Boolean, default=False)
    privilege_cmd_name = Column(String, nullable=True)
    is_late_night = Column(Boolean, default=False)
    is_work_hours = Column(Boolean, default=False)
    is_potential_dump = Column(Boolean, default=False)
    has_limit = Column(Boolean, default=False)
    has_order_by = Column(Boolean, default=False)

    # --- Errors ---
    error_code = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    error_count = Column(Integer, default=0)
    warning_count = Column(Integer, default=0)
    has_error = Column(Boolean, default=False)

    # Indexes for performance
    __table_args__ = (
        Index('ix_all_logs_user_timestamp', 'user', 'timestamp'),
        Index('ix_all_logs_is_anomaly', 'is_anomaly'),
        Index('ix_all_logs_ml_score', 'ml_anomaly_score'),
    )