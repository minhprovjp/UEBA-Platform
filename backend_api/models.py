# backend_api/models.py
import os
import sys
from sqlalchemy import (create_engine, Column, Integer, String, DateTime, 
                        Float, Boolean, Text, Index, BigInteger, JSON, func)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Thêm thư mục gốc vào sys.path để có thể import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL # <-- Chúng ta sẽ định nghĩa biến này trong config.py

# --- Thiết lập Kết nối CSDL ---
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Định nghĩa Bảng `anomalies` ---
# Bảng này sẽ lưu trữ TẤT CẢ các loại bất thường được phát hiện.
class Anomaly(Base):
    __tablename__ = 'anomalies'
    
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    user = Column(String, index=True)
    client_ip = Column(String)
    database = Column(String, nullable=True)
    
    # query có thể rất dài, dùng Text
    query = Column(Text, nullable=False) 
    
    # Loại bất thường (key hệ thống, vd: 'late_night', 'sensitive', 'complexity')
    anomaly_type = Column(String, index=True, nullable=False) 
    
    # Điểm số (từ AI hoặc Rule 6)
    score = Column(Float, nullable=True) 
    
    # Lý do phát hiện (từ Rule 4, 5, 6)
    reason = Column(String, nullable=True) 
    
    # Trạng thái để quản lý (sẽ dùng trong tương lai)
    status = Column(String, default='new', index=True) 
    
    execution_time_ms = Column(Float, nullable=True, server_default='0')
    rows_returned = Column(BigInteger, nullable=True, server_default='0')
    rows_affected = Column(BigInteger, nullable=True, server_default='0')
   
# Bảng này sẽ lưu trữ Anomaly tổng hợp (multi_table, session-level, behavior-level) 
class AggregateAnomaly(Base):
    """
    Lưu các anomaly tổng hợp (session-level / multi-table / profile),
    không gắn với 1 query duy nhất.
    """
    __tablename__ = 'aggregate_anomalies'

    id = Column(Integer, primary_key=True, index=True)

    # loại: 'session', 'user', ...
    scope = Column(String, nullable=False, default='session', index=True)

    user = Column(String, index=True, nullable=True)
    database = Column(String, nullable=True)

    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)

    anomaly_type = Column(String, index=True, nullable=False)  # ví dụ: 'multi_table'
    severity = Column(Float, nullable=True)                    # vd: distinct_tables_count
    reason = Column(Text, nullable=True)

    # JSON lưu details: tables_accessed, queries_details,...
    details = Column(JSON, nullable=True)    
    
    
    created_at = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    
# Bảng này sẽ lưu TẤT CẢ các log
class AllLogs(Base):
    __tablename__ = 'all_logs'
    
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False, index=True) # Index theo thời gian
    user = Column(String, index=True) # Index theo user
    client_ip = Column(String)
    database = Column(String, nullable=True)
    query = Column(Text, nullable=False)
    
    # Thêm 2 cột quan trọng để biết kết quả phân tích
    is_anomaly = Column(Boolean, default=False, index=True)
    analysis_type = Column(String, nullable=True) # Ví dụ: "Global Fallback", "Per-User Profile"
    
    execution_time_ms = Column(Float, nullable=True, server_default='0')
    rows_returned = Column(BigInteger, nullable=True, server_default='0')
    rows_affected = Column(BigInteger, nullable=True, server_default='0')
    