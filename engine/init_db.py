# init_db.py
import sys
import os
import logging
from sqlalchemy.exc import OperationalError
from sqlalchemy import text

# Thêm thư mục gốc vào path để import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Lấy đường dẫn đến file hiện tại (ví dụ: .../UBA-Platform/engine/init_db.py)
CURRENT_FILE_PATH = os.path.abspath(__file__)
# Lấy đường dẫn đến thư mục chứa file này (ví dụ: .../UBA-Platform/engine)
CURRENT_DIR = os.path.dirname(CURRENT_FILE_PATH)
# Lấy đường dẫn thư mục GỐC (thư mục cha của 'engine', ví dụ: .../UBA-Platform)
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

# Thêm thư mục GỐC vào sys.path
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

try:
    # Import các thành phần CSDL từ 'backend_api'
    from backend_api.models import Base, engine
    from config import DATABASE_URL
except ImportError as e:
    print("Lỗi: Không thể import 'backend_api.models' hoặc 'config'.")
    print(f"Lỗi chi tiết: {e}")
    print("Hãy đảm bảo bạn chạy file này từ thư mục gốc (UBA-Platform) "
          "và file models.py của bạn đã định nghĩa 'Base' và 'engine' chính xác.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("InitDB")

log.info(f"Đang kết nối đến CSDL: {DATABASE_URL}")

try:
    # ⚠️ CẢNH BÁO: Dòng này XÓA TOÀN BỘ CÁC BẢNG trong metadata.
    # Chỉ dùng cho môi trường DEV/TEST.
    log.info("Đang dọn dẹp các bảng cũ (nếu có)...")
    Base.metadata.drop_all(bind=engine)
    log.info("Dọn dẹp hoàn tất.")

    log.info("Bắt đầu khởi tạo bảng từ SQLAlchemy models...")
    Base.metadata.create_all(bind=engine)
    log.info("Đã tạo xong các bảng từ models.")

    # Tạo bảng aggregate_anomalies nếu chưa tồn tại
    log.info("Đang đảm bảo bảng 'aggregate_anomalies' tồn tại...")

    create_agg_table_sql = """
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

    CREATE INDEX IF NOT EXISTS idx_agg_anom_user_time
        ON aggregate_anomalies ("user", start_time, end_time);

    CREATE INDEX IF NOT EXISTS idx_agg_anom_type
        ON aggregate_anomalies (anomaly_type);
    """

    with engine.begin() as conn:
        conn.execute(text(create_agg_table_sql))

    log.info("Bảng 'aggregate_anomalies' đã sẵn sàng trong CSDL.")

    log.info("-------------------------------------------------")
    log.info("✅ Hoàn tất! Schema CSDL đã được khởi tạo thành công.")
    log.info("Bây giờ bạn có thể chạy publisher và engine.")
    log.info("-------------------------------------------------")

except OperationalError:
    log.error("LỖI KẾT NỐI: Không thể kết nối đến CSDL.")
    log.error("Vui lòng kiểm tra lại DATABASE_URL trong .env hoặc config.py,")
    log.error("và đảm bảo PostgreSQL đang chạy, user/password chính xác.")
except Exception as e:
    log.error(f"Lỗi nghiêm trọng khi khởi tạo CSDL: {e}")
