# init_db.py
import sys
import os
import logging
from sqlalchemy.exc import OperationalError
from sqlalchemy import text

# Thêm thư mục gốc vào path để import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
CURRENT_FILE_PATH = os.path.abspath(__file__)
CURRENT_DIR = os.path.dirname(CURRENT_FILE_PATH)
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

try:
    # Import các thành phần CSDL từ 'backend_api'
    # Quan trọng: Import AllLogs để SQLAlchemy biết structure bảng này
    from backend_api.models import Base, engine, AllLogs, Anomaly, AggregateAnomaly
    from config import DATABASE_URL
except ImportError as e:
    print("Lỗi: Không thể import 'backend_api.models' hoặc 'config'.")
    print(f"Lỗi chi tiết: {e}")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("InitDB")

log.info(f"Đang kết nối đến CSDL: {DATABASE_URL}")

try:
    log.info("-------------------------------------------------")
    log.info("BƯỚC 1: Dọn dẹp Database cũ (Hard Drop)")
    
    # Sử dụng kết nối trực tiếp để DROP TABLE CASCADE
    # Cách này mạnh hơn drop_all vì nó xóa bất chấp dependencies
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS all_logs CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS anomalies CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS aggregate_anomalies CASCADE;"))
    
    log.info("Đã xóa sạch các bảng cũ.")

    log.info("-------------------------------------------------")
    log.info("BƯỚC 2: Tạo lại bảng mới từ Models")
    Base.metadata.create_all(bind=engine)
    log.info("Đã tạo xong các bảng: all_logs, anomalies, aggregate_anomalies.")

    log.info("-------------------------------------------------")
    log.info("BƯỚC 3: Kiểm tra bổ sung")
    
    # Đoạn này chỉ để double-check hoặc tạo index đặc biệt nếu model chưa cover
    # (Hiện tại Models đã cover đủ, nhưng giữ lại check cho an toàn)
    create_agg_check_sql = """
    CREATE TABLE IF NOT EXISTS aggregate_anomalies (
        id SERIAL PRIMARY KEY,
        scope VARCHAR(50) NOT NULL,
        "user" VARCHAR(255) NOT NULL,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP NOT NULL,
        anomaly_type VARCHAR(100) NOT NULL,
        severity NUMERIC(5,2) NOT NULL,
        reason TEXT NOT NULL,
        details JSONB
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_agg_check_sql))
    
    log.info("✅ HOÀN TẤT! Schema CSDL đã được khởi tạo thành công.")
    log.info("-------------------------------------------------")

except OperationalError:
    log.error("LỖI KẾT NỐI: Không thể kết nối đến CSDL.")
    log.error("Vui lòng kiểm tra lại DATABASE_URL trong .env hoặc config.py,")
    log.error("và đảm bảo PostgreSQL đang chạy, user/password chính xác.")
except Exception as e:
    log.error(f"Lỗi nghiêm trọng khi khởi tạo CSDL: {e}")