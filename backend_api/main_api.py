# backend_api/main_api.py
from fastapi import FastAPI, Depends, HTTPException, status, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from datetime import datetime

# Import các thành phần từ các file trong cùng thư mục
from . import models, schemas
from .models import SessionLocal, engine

# Import engine và trình quản lý config
import sys
import os
# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# === CÁC IMPORT CẦN THIẾT CHO CÁC API CÒN LẠI ===
# (LƯU Ý: Chúng ta vẫn cần __init__.py trong thư mục 'engine' để các lệnh này hoạt động)
try:
    from engine.config_manager import load_config, save_config
    from engine.utils import save_feedback_to_csv
    from engine.llm_analyzer import analyze_query_with_llm, analyze_session_with_llm
except ImportError as e:
    print("="*50)
    print(f"LỖI IMPORT NGHIÊM TRỌNG: {e}")
    print(">>> BẠN ĐÃ TẠO FILE TRỐNG 'engine/__init__.py' CHƯA? <<<")
    print("="*50)
    sys.exit(1)

from pydantic import BaseModel

# === BỎ LOGIC AnalysisEngine CŨ ===
# (ĐÃ XÓA) engine_instance = AnalysisEngine()

# Tạo các bảng trong CSDL nếu chúng chưa tồn tại
# models.Base.metadata.create_all(bind=engine)

# Khởi tạo ứng dụng FastAPI
app = FastAPI(
    title="User Behavior Analytics API",
    description="API để truy vấn các bất thường được phát hiện bởi Engine Phân tích Log.",
    version="1.0.0"
)

# Cấu hình CORS (Giữ nguyên, rất quan trọng cho Frontend)
origins = [
    "http://localhost:5173",  # Địa chỉ của Vite React dev server
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Security
API_KEY_NAME = "X-API-Key"
API_KEY_HEADER = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

# Lấy API Key an toàn từ biến môi trường
# Hãy đặt biến này trong file .env của bạn: API_KEY="your_super_secret_key"
EXPECTED_API_KEY = os.getenv("API_KEY", "default_secret_key_change_me")

async def get_api_key(api_key_header: str = Security(API_KEY_HEADER)):
    """Kiểm tra xem API key được gửi lên có hợp lệ không."""
    if api_key_header == EXPECTED_API_KEY:
        return api_key_header
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials",
        )

# --- Dependency Injection: Cung cấp DB Session cho các endpoint ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# === CÁC ENDPOINT LẤY DỮ LIỆU BẤT THƯỜNG (CHO FRONTEND) ===

# @app.get("/api/anomalies/", response_model=List[schemas.Anomaly], tags=["Anomalies"])
# def read_anomalies(skip: int = 0, limit: int = 100, db: Session = Depends(get_db), api_key: str = Security(get_api_key)):
#     """
#     Lấy ra một danh sách các bất thường, hỗ trợ phân trang (pagination).
#     """
#     anomalies = db.query(models.Anomaly).order_by(models.Anomaly.timestamp.desc()).offset(skip).limit(limit).all()
#     return anomalies

@app.get("/api/anomalies/",response_model=List[schemas.UnifiedAnomaly],tags=["Anomalies"])
def read_unified_anomalies(skip: int = 0,limit: int = 100,db: Session = Depends(get_db),api_key: str = Security(get_api_key)):
    """
    Lấy danh sách tất cả bất thường (event-level + aggregate),
    trả về ở dạng unified cho frontend.
    """
    # 1) Lấy event-level anomalies từ bảng anomalies
    event_q = (
        db.query(models.Anomaly)
        .order_by(models.Anomaly.timestamp.desc())
    )
    event_rows = event_q.all()

    unified: List[schemas.UnifiedAnomaly] = []

    for a in event_rows:
        unified.append(
            schemas.UnifiedAnomaly(
                id=f"event-{a.id}",
                source="event",
                anomaly_type=a.anomaly_type,
                timestamp=a.timestamp,
                user=a.user,
                database=a.database,
                query=a.query,
                reason=a.reason,
                score=a.score,
                scope="log",
                details=None,
            )
        )

    # 2) Lấy aggregate anomalies (multi_table / session-level)
    agg_q = (
        db.query(models.AggregateAnomaly)
        .order_by(
            models.AggregateAnomaly.start_time.desc().nullslast(),
            models.AggregateAnomaly.created_at.desc()
        )
    )
    agg_rows = agg_q.all()

    for a in agg_rows:
        unified.append(
            schemas.UnifiedAnomaly(
                id=f"agg-{a.id}",
                source="aggregate",
                anomaly_type=a.anomaly_type,
                timestamp=a.start_time or a.end_time or a.created_at,
                user=a.user,
                database=a.database,
                query=None,
                reason=a.reason,
                score=a.severity,
                scope=a.scope,
                details=a.details,
            )
        )

    # 3) Sort chung theo thời gian mới nhất
    def sort_key(item: schemas.UnifiedAnomaly):
        return item.timestamp or datetime.min

    unified.sort(key=sort_key, reverse=True)

    # 4) Áp dụng skip/limit ở unified list
    return unified[skip : skip + limit]

@app.get("/api/anomalies/{anomaly_id}", response_model=schemas.Anomaly, tags=["Anomalies"])
def read_anomaly_by_id(anomaly_id: int, db: Session = Depends(get_db), api_key: str = Security(get_api_key)):
    """
    Lấy thông tin chi tiết của một bất thường cụ thể bằng ID của nó.
    """
    anomaly = db.query(models.Anomaly).filter(models.Anomaly.id == anomaly_id).first()
    if anomaly is None:
        raise HTTPException(status_code=404, detail="Anomaly not found")
    return anomaly

@app.get("/api/aggregate-anomalies/{agg_id}", response_model=schemas.UnifiedAnomaly, tags=["Anomalies"])
def read_aggregate_anomaly_by_id(
    agg_id: int,
    db: Session = Depends(get_db),
    api_key: str = Security(get_api_key)
):
    """
    Lấy chi tiết một aggregate anomaly (ví dụ: multi_table session).
    """
    agg = db.query(models.AggregateAnomaly).filter(models.AggregateAnomaly.id == agg_id).first()
    if agg is None:
        raise HTTPException(status_code=404, detail="Aggregate anomaly not found")

    return schemas.UnifiedAnomaly(
        id=f"agg-{agg.id}",
        source="aggregate",
        anomaly_type=agg.anomaly_type,
        timestamp=agg.start_time or agg.end_time or agg.created_at,
        user=agg.user,
        database=agg.database,
        query=None,
        reason=agg.reason,
        score=agg.severity,
        scope=agg.scope,
        details=agg.details,
    )


# === ENDPOINT PHÂN TÍCH LLM ===

@app.post("/api/llm/analyze-anomaly", tags=["LLM Analysis"])
def analyze_anomaly_with_llm_endpoint(request: schemas.AnomalyAnalysisRequest, api_key: str = Security(get_api_key)):
    """
    Nhận thông tin về một bất thường và yêu cầu LLM phân tích nó.
    """
    try:
        engine_config = load_config()
        llm_settings = engine_config.get("llm_config", {})
        rules_settings = engine_config.get("analysis_params", {})
        anomaly_data = request.model_dump()
        
        analysis_result = analyze_query_with_llm(
            anomaly_row=anomaly_data,
            anomaly_type_from_system=anomaly_data['anomaly_type'],
            llm_config=llm_settings,
            rules_config=rules_settings
        )
        
        return analysis_result
        
    except (ValueError, ConnectionError) as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

# === CÁC ENDPOINT MỚI ĐỂ ĐIỀU KHIỂN ENGINE (ĐÃ XÓA) ===
# Các endpoint /api/engine/status, /start, /stop đã bị xóa
# vì Engine giờ là một tiến trình riêng biệt.

# === CÁC ENDPOINT ĐỂ QUẢN LÝ CẤU HÌNH (VẪN CẦN THIẾT) ===
@app.get("/api/engine/config", response_model=Dict[str, Any], tags=["Configuration"])
def get_engine_config(api_key: str = Security(get_api_key)):
    """
    Đọc và trả về nội dung hiện tại của file engine_config.json.
    """
    config = load_config()
    if not config:
        raise HTTPException(status_code=404, detail="File cấu hình không tìm thấy hoặc bị lỗi.")
    return config

@app.put("/api/engine/config", status_code=status.HTTP_202_ACCEPTED, tags=["Configuration"])
def update_engine_config(config_data: Dict[str, Any], api_key: str = Security(get_api_key)):
    """
    Nhận một đối tượng JSON và ghi đè hoàn toàn file engine_config.json.
    (Các engine độc lập sẽ cần phải được khởi động lại để nhận cấu hình này,
    hoặc chúng ta sẽ cải tiến chúng để tự đọc lại file)
    """
    success, message = save_config(config_data)
    if not success:
        raise HTTPException(status_code=500, detail=message)
    
    # (Đã Xóa) engine_instance.config = load_config()
        
    return {"message": message}

# === ENDPOINT ĐỂ NHẬN FEEDBACK ===
@app.post("/api/feedback/", status_code=status.HTTP_201_CREATED, tags=["Feedback"])
def submit_feedback(feedback: schemas.FeedbackCreate, api_key: str = Security(get_api_key)):
    """
    Nhận phản hồi từ người dùng và lưu vào file feedback.csv.
    """
    try:
        success, message = save_feedback_to_csv(
            item_data=feedback.anomaly_data,
            label=feedback.label
        )
        if not success:
            raise HTTPException(status_code=500, detail=message)
        
        return {"message": message}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi không xác định: {str(e)}")
    
# === API MỚI CHO LOG EXPLORER ===
@app.get("/api/logs/", response_model=List[schemas.AllLogs], tags=["Log Explorer"])
def read_all_logs(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db), 
    api_key: str = Security(get_api_key),
    search: str = None, # Thêm bộ lọc
    user: str = None,   # Thêm bộ lọc
    date_from: datetime = None, # Thêm bộ lọc
    date_to: datetime = None    # Thêm bộ lọc
):
    """
    Lấy ra TẤT CẢ các log đã được xử lý (bình thường + bất thường).
    Hỗ trợ phân trang.
    """
    query = db.query(models.AllLogs)
    
    if search:
        query = query.filter(models.AllLogs.query.ilike(f"%{search}%"))
    if user:
        query = query.filter(models.AllLogs.user == user)
    if date_from:
        query = query.filter(models.AllLogs.timestamp >= date_from)
    if date_to:
        query = query.filter(models.AllLogs.timestamp <= date_to)
        
    logs = query.order_by(models.AllLogs.timestamp.desc()).offset(skip).limit(limit).all()
    return logs