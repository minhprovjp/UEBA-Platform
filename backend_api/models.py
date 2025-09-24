# backend_api/models.py
import os
import sys
from sqlalchemy import (create_engine, Column, Integer, String, DateTime, 
                        Float, Boolean, Text)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from core.config import settings

# --- Thiết lập Kết nối CSDL ---
if not settings.DATABASE_URL:
    raise ValueError("DATABASE_URL is not set. Please create a .env file and define it.")

# Xóa bỏ connect_args dành cho SQLite
engine = create_engine(settings.DATABASE_URL)
 
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Thêm thư mục gốc vào sys.path để có thể import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.config import settings # <-- Chúng ta sẽ định nghĩa biến này trong config.py

# --- Thiết lập Kết nối CSDL ---
if not settings.DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in the environment variables.")
engine = create_engine(settings.DATABASE_URL)
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

# --- Định nghĩa Bảng `parsed_logs` ---
# Bảng này sẽ lưu trữ TẤT CẢ các log đã được parse từ các nguồn khác nhau.
class ParsedLog(Base):
    __tablename__ = 'parsed_logs'
    
    id = Column(Integer, primary_key=True, index=True)
    # Thêm cột source_type để biết log này đến từ đâu (mysql, postgres...)
    source_type = Column(String, index=True, default='unknown') 
    timestamp = Column(DateTime, nullable=False, index=True)
    user = Column(String, index=True)
    client_ip = Column(String, index=True)
    database = Column(String, nullable=True, index=True)
    query = Column(Text, nullable=False)