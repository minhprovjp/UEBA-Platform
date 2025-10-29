# init_db.py

import sys
import os
from sqlalchemy import create_engine, text

# Thêm thư mục gốc vào path để import config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import DATABASE_URL

def initialize_database():
    """
    Kết nối tới CSDL và tạo các bảng cần thiết nếu chúng chưa tồn tại.
    """
    print(f"Đang kết nối tới CSDL: {DATABASE_URL}")
    engine = create_engine(DATABASE_URL)
    
    # Câu lệnh SQL để tạo bảng staging_logs
    # Các cột phải khớp với dữ liệu mà parser tạo ra
    create_staging_table_sql = """
    CREATE TABLE IF NOT EXISTS staging_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME NOT NULL,
        user TEXT,
        client_ip TEXT,
        database TEXT,
        query TEXT,
        source_dbms VARCHAR(50) NOT NULL,
        parsed_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Câu lệnh SQL để tạo bảng anomalies (ví dụ)
    create_anomalies_table_sql = """
    CREATE TABLE IF NOT EXISTS anomalies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME NOT NULL,
        user TEXT,
        client_ip TEXT,
        database TEXT,
        query TEXT,
        anomaly_type VARCHAR(100),
        score REAL,
        reason TEXT,
        source_dbms VARCHAR(50)
    );
    """

    try:
        with engine.connect() as connection:
            print("Đang tạo bảng 'staging_logs'...")
            connection.execute(text(create_staging_table_sql))
            print("Đang tạo bảng 'anomalies'...")
            connection.execute(text(create_anomalies_table_sql))
            # Commit các thay đổi
            connection.commit()
        print("Khởi tạo CSDL thành công!")
    except Exception as e:
        print(f"Lỗi khi khởi tạo CSDL: {e}")

if __name__ == "__main__":
    initialize_database()