# backend_api/models.py
import os
import sys
from sqlalchemy import (create_engine, Column, Integer, String, DateTime, 
                        Float, Boolean, Text, Index)
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
    timestamp = Column(DateTime, nullable=False)
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
    is_anomaly = Column(Boolean, default=False)
    analysis_type = Column(String, nullable=True) # Ví dụ: "Global Fallback", "Per-User Profile"
    
    # Thêm các chỉ mục (index) để tăng tốc độ truy vấn khi lọc theo thời gian hoặc user
    __table_args__ = (
        Index('ix_all_logs_timestamp', 'timestamp'),
        Index('ix_all_logs_user', 'user'),
    )