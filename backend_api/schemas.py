# backend_api/schemas.py
from pydantic import BaseModel
from datetime import datetime
from typing import Optional

# --- Schema cơ bản cho Anomaly ---
# Chứa các trường chung mà cả khi tạo mới và khi đọc đều cần đến.
class AnomalyBase(BaseModel):
    timestamp: datetime
    user: str
    client_ip: Optional[str] = None
    database: Optional[str] = None
    query: str
    anomaly_type: str
    score: Optional[float] = None
    reason: Optional[str] = None
    status: str

# --- Schema để trả về cho người dùng ---
# Kế thừa từ AnomalyBase và thêm các trường chỉ có sau khi đã được lưu vào CSDL (như id).
class Anomaly(AnomalyBase):
    id: int

    # Cấu hình này rất quan trọng: nó bảo Pydantic hãy đọc dữ liệu
    # từ các thuộc tính của một đối tượng SQLAlchemy (chế độ ORM).
    class Config:
        from_attributes = True # Dành cho Pydantic v2. Nếu dùng Pydantic v1, hãy dùng `orm_mode = True`

<<<<<<< Updated upstream
# --- Schema cho Yêu cầu Phân tích của LLM ---
# Định nghĩa cấu trúc dữ liệu mà frontend PHẢI gửi lên khi yêu cầu phân tích.
class AnomalyAnalysisRequest(BaseModel):
    timestamp: str
    user: str
    query: str
    anomaly_type: str
    score: Optional[float] = None
    reason: Optional[str] = None
    
# === THÊM SCHEMA MỚI CHO FEEDBACK ===
class FeedbackCreate(BaseModel):
    label: int # 0 cho bình thường, 1 cho bất thường
    anomaly_data: dict # Gửi toàn bộ dữ liệu của bất thường dưới dạng dictionary
=======
# --- Schema cho LLM Analysis Request ---
# Sử dụng cùng cấu trúc với AnomalyBase để gửi dữ liệu đến LLM
class AnomalyAnalysisRequest(AnomalyBase):
    pass

# --- Schema cho Feedback Creation ---
class FeedbackCreate(BaseModel):
    anomaly_data: dict  # Dữ liệu anomaly để gửi feedback
    label: str  # Nhãn feedback từ người dùng
>>>>>>> Stashed changes
