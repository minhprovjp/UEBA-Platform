# init_db.py
import sys
import os
import logging
from sqlalchemy.exc import OperationalError

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
    # (Giả sử file models.py của bạn nằm ở backend_api/models.py)
    from backend_api.models import Base, engine 
    from config import DATABASE_URL # Import URL để xác nhận
except ImportError as e:
    print(f"Lỗi: Không thể import 'backend_api.models' hoặc 'config'.")
    print(f"Lỗi chi tiết: {e}")
    print("Hãy đảm bảo bạn chạy file này từ thư mục gốc (UBA-Platform) và file models.py của bạn đã định nghĩa 'Base' và 'engine' một cách chính xác.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("InitDB")

log.info(f"Đang kết nối đến CSDL: {DATABASE_URL}")
log.info("Bắt đầu khởi tạo bảng...")

try:
    # Đây chính là lệnh quan trọng:
    # Nó sẽ đọc tất cả các class (như Anomaly) kế thừa từ 'Base'
    # và tạo bảng tương ứng trong CSDL PostgreSQL.
    Base.metadata.create_all(bind=engine)
    
    log.info("-------------------------------------------------")
    log.info("✅ Hoàn tất! Các bảng đã được tạo thành công trong CSDL PostgreSQL.")
    log.info("Bạn bây giờ có thể chạy publisher và engine.")
    log.info("-------------------------------------------------")

except OperationalError as e:
    log.error(f"LỖI KẾT NỐI: Không thể kết nối đến CSDL.")
    log.error("Vui lòng kiểm tra lại chuỗi DATABASE_URL trong file .env hoặc config.py.")
    log.error("Hãy đảm bảo CSDL PostgreSQL đang chạy và user/password là chính xác.")
except Exception as e:
    log.error(f"Lỗi nghiêm trọng khi khởi tạo CSDL: {e}")