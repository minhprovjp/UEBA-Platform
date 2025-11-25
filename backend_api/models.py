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

class AllLogs(Base):
    __tablename__ = 'all_logs'
   
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    user = Column(String, index=True)
    client_ip = Column(String)
    database = Column(String, nullable=True)
    query = Column(Text, nullable=False)

    # === ORIGINAL METRICS ===
    source_dbms = Column(String, default="MySQL")
    error_code = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    execution_time_ms = Column(Float, nullable=True, server_default='0')
    rows_returned = Column(BigInteger, nullable=True, server_default='0')
    rows_affected = Column(BigInteger, nullable=True, server_default='0')
    # [UPDATE] Thêm trường quan trọng để tính hiệu suất
    rows_examined = Column(BigInteger, nullable=True, server_default='0') 

    # === ANALYSIS RESULT ===
    is_anomaly = Column(Boolean, default=False) 
    analysis_type = Column(String, nullable=True)
    ml_anomaly_score = Column(Float, default=0.0)

    # === NEW SOTA FEATURES (2025) ===
    # 1. Structural / Complexity
    query_length = Column(Integer, nullable=True)
    query_entropy = Column(Float, nullable=True) # Độ hỗn loạn (phát hiện SQLi/Hex)
    num_tables = Column(Integer, default=0)
    num_joins = Column(Integer, default=0)
    num_where_conditions = Column(Integer, default=0)
    subquery_depth = Column(Integer, default=0)  # Độ sâu lồng nhau

    # 2. Risk Flags
    is_risky_command = Column(Boolean, default=False) # DROP, ALTER
    is_admin_command = Column(Boolean, default=False) # GRANT, REVOKE
    is_sensitive_access = Column(Boolean, default=False) # Access HR/Salaries
    is_system_access = Column(Boolean, default=False) # Access mysql.*
    has_comment = Column(Boolean, default=False) # Phát hiện bypass WAF
    has_hex = Column(Boolean, default=False) # Phát hiện 0x...
    
    # 3. Contextual / Velocity (Rolling Windows)
    # Lưu lại để debug mô hình
    query_count_5m = Column(Float, nullable=True)
    error_count_5m = Column(Float, nullable=True)
    data_retrieval_speed = Column(Float, nullable=True) # rows / time

    # 4. Behavioral Deviation
    execution_time_ms_zscore = Column(Float, nullable=True)
    rows_returned_zscore = Column(Float, nullable=True)

    # Misc
    accessed_tables = Column(JSONB, nullable=True)
    command_type = Column(String, nullable=True)
    normalized_query = Column(Text, nullable=True)
    error_count = Column(Integer, default=0)
    warning_count = Column(Integer, default=0)
    created_tmp_disk_tables = Column(Integer, default=0)
    no_index_used = Column(Integer, default=0)

    # Indexes
    __table_args__ = (
        Index('ix_all_logs_user_timestamp', 'user', 'timestamp'),
        Index('ix_all_logs_is_anomaly', 'is_anomaly'),
    )