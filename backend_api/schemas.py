# backend_api/schemas.py
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Literal, Dict, Any, List

# --- Schema cơ bản cho Anomaly ---
# Chứa các trường chung mà cả khi tạo mới và khi đọc đều cần đến.
class AnomalyBase(BaseModel):
    timestamp: datetime
    user: Optional[str] = None
    client_ip: Optional[str] = None
    database: Optional[str] = None
    query: str
    anomaly_type: str
    behavior_group: Optional[str] = None
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

class UnifiedAnomaly(BaseModel):
    id: str
    source: Literal["event", "aggregate"]
    anomaly_type: Optional[str] = None
    behavior_group: Optional[str] = None
    timestamp: Optional[datetime] = None
    user: Optional[str] = None
    client_ip: Optional[str] = None
    database: Optional[str] = None
    query: Optional[str] = None          # chỉ event mới có
    reason: Optional[str] = None
    score: Optional[float] = None        # event: score | aggregate: severity
    scope: Optional[str] = None          # aggregate: 'session' | 'user' | ...
    details: Optional[Dict[str, Any]] = None
    ai_analysis: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True

class AnomalyStats(BaseModel):
    event_count: int
    aggregate_count: int
    total_count: int

# --- Schema cho Yêu cầu Phân tích của LLM ---
# Định nghĩa cấu trúc dữ liệu mà frontend PHẢI gửi lên khi yêu cầu phân tích.
class AnomalyAnalysisRequest(BaseModel):
    id: str
    timestamp: datetime 
    user: Optional[str] = None
    # Đổi thành Optional vì Session Anomaly không có single query
    query: Optional[str] = None 
    anomaly_type: str
    score: Optional[float] = None
    reason: Optional[str] = None
    # Thêm trường này để Backend biết xử lý kiểu event hay session
    details: Optional[Dict[str, Any]] = None
    
# === THÊM SCHEMA MỚI CHO FEEDBACK ===
class FeedbackCreate(BaseModel):
    label: int # 0 cho bình thường, 1 cho bất thường
    anomaly_data: dict # Gửi toàn bộ dữ liệu của bất thường dưới dạng dictionary
    
# --- Schema cơ bản cho AllLogs ---
class AllLogsBase(BaseModel):
    timestamp: datetime
    user: Optional[str] = None
    client_ip: Optional[str] = None
    database: Optional[str] = None
    query: str
    is_anomaly: bool
    analysis_type: Optional[str] = None
    behavior_group: Optional[str] = None
    specific_rule: Optional[str] = None

# --- Schema để trả về cho người dùng (Thêm 'id') ---
class AllLogs(AllLogsBase):
    id: int

    class Config:
        from_attributes = True # Dùng Pydantic v2
        # orm_mode = True # Dùng nếu bạn ở Pydantic v1

class AnomalyKpis(BaseModel):
    access_anomaly: int
    insider_threat: int
    technical_attack: int
    data_destruction: int
    ml_detected: int
    multi_table: int  
    behavioral_profile: int 
    total: int

class AnomalyFacetResponse(BaseModel):
    users: List[str]
    types: List[str]

class AnomalySearchItem(BaseModel):
    id: int
    timestamp: datetime
    user: Optional[str] = None
    anomaly_type: Optional[str] = None
    reason: Optional[str] = None
    query: Optional[str] = None
    score: Optional[float] = None

class AnomalySearchResponse(BaseModel):
    total: int
    items: List[AnomalySearchItem]
    
class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str