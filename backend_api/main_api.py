# backend_api/main_api.py
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Dict
from typing import List, Dict, Any, Optional
import sys
import os
from .routers import logs
from .routers import logs, filters
from engine.schemas.config_schema import EngineConfig
from core.config import settings

# Thêm thư mục gốc vào sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from . import models, schemas
from .models import SessionLocal, engine
from engine.config_manager import load_config, save_config
from engine.utils import save_feedback_to_csv
from engine.llm_analyzer import analyze_query_with_llm
from engine.status_manager import load_status

# Khởi tạo các bảng trong CSDL
models.Base.metadata.create_all(bind=engine)

# Biến này sẽ được engine cập nhật từ bên ngoài
# Đây là giải pháp đơn giản, trong production có thể dùng Redis hoặc DB
ENGINE_STATUS: Dict[str, any] = {
    "is_running": False,
    "status": "stopped",
    "last_run_finish_time_utc": None
}

# Khởi tạo ứng dụng FastAPI
app = FastAPI(
    
    title="User Behavior Analytics API",
    description="API để truy vấn các bất thường và điều khiển Engine Phân tích.",
    version="1.0.0"
)
#ĐĂNG KÝ ROUTER
app.include_router(logs.router)
app.include_router(filters.router)
# Cấu hình CORS
origins = ["http://localhost:5173", "http://localhost:3000"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency Injection
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# === CÁC API ENDPOINT ===

@app.get("/api/engine/status", tags=["Engine Monitoring"])
def get_engine_status():
    return load_status()

@app.get("/api/anomalies/", response_model=schemas.AnomalyPage, tags=["Anomalies"])
def read_anomalies(
    skip: int = 0, 
    limit: int = 50, 
    type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Lấy danh sách bất thường, hỗ trợ phân trang và lọc theo loại.
    """
    query = db.query(models.Anomaly)
    if type:
        query = query.filter(models.Anomaly.anomaly_type == type)
    
    total_items = query.count()
    anomalies = query.order_by(models.Anomaly.timestamp.desc()).offset(skip).limit(limit).all()
    return {"total_items": total_items, "items": anomalies}

# === ENDPOINT MỚI CHO THỐNG KÊ ===
@app.get("/api/stats/summary", tags=["Statistics"])
def get_statistics_summary(db: Session = Depends(get_db)):
    """
    Thực hiện các truy vấn đếm để lấy số liệu thống kê tổng quan
    về các loại bất thường và top users.
    """
    try:
        anomaly_counts_query = db.query(
            models.Anomaly.anomaly_type, 
            func.count(models.Anomaly.anomaly_type)
        ).group_by(models.Anomaly.anomaly_type).all()
        anomaly_counts = {atype: count for atype, count in anomaly_counts_query}

        top_users_query = db.query(
            models.Anomaly.user,
            func.count(models.Anomaly.user)
        ).group_by(models.Anomaly.user).order_by(func.count(models.Anomaly.user).desc()).limit(5).all()
        top_users = {user: count for user, count in top_users_query}

        return {
            "anomaly_counts": anomaly_counts,
            "top_users": top_users
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lấy dữ liệu thống kê: {e}")


@app.get("/api/anomalies/{anomaly_id}", response_model=schemas.Anomaly, tags=["Anomalies"])
def read_anomaly_by_id(anomaly_id: int, db: Session = Depends(get_db)):
    """
    Lấy thông tin chi tiết của một bất thường cụ thể bằng ID của nó.
    """
    anomaly = db.query(models.Anomaly).filter(models.Anomaly.id == anomaly_id).first()
    if anomaly is None:
        raise HTTPException(status_code=404, detail="Anomaly not found")
    return anomaly

@app.post("/api/llm/analyze-anomaly", tags=["LLM Analysis"])
def analyze_anomaly_with_llm_endpoint(request: schemas.AnomalyAnalysisRequest):
    """
    Nhận thông tin về một bất thường và yêu cầu LLM phân tích nó.
    """
    try:
        # 1. Đọc cấu hình engine hiện tại từ file
        engine_config = load_config()
        
        # 2. Lấy ra các phần cấu hình cần thiết
        llm_settings = engine_config.get("llm_config", {}) # Giả sử bạn sẽ tạo mục này
        rules_settings = engine_config.get("analysis_params", {})
        
        # 3. Chuyển đổi request Pydantic thành dictionary
        anomaly_data = request.model_dump()
        
        # 4. Gọi hàm phân tích đã được tái cấu trúc
        analysis_result = analyze_query_with_llm(
            anomaly_row=anomaly_data,
            anomaly_type_from_system=anomaly_data['anomaly_type'],
            llm_config=llm_settings,
            rules_config=rules_settings
        )
        
        return analysis_result
        
    except (ValueError, ConnectionError) as e:
        # Bắt các lỗi cụ thể mà hàm phân tích ném ra
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        # Bắt các lỗi không lường trước khác
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

# === CÁC ENDPOINT MỚI ĐỂ ĐIỀU KHIỂN ENGINE ===


@app.get("/api/engine/config", response_model=Dict[str, Any], tags=["Engine Control"])
def get_engine_config():
    """Đọc cấu hình hiện tại của Engine từ file engine_config.json."""
    return load_config()

# === CÁC ENDPOINT ĐỂ QUẢN LÝ CẤU HÌNH ===
@app.get("/api/engine/config", response_model=Dict[str, Any], tags=["Configuration"])
def get_engine_config():
    """
    Đọc và trả về nội dung hiện tại của file engine_config.json.
    """
    config = load_config()
    if not config:
        raise HTTPException(status_code=404, detail="File cấu hình không tìm thấy hoặc bị lỗi.")
    return config

@app.put("/api/engine/config", status_code=status.HTTP_202_ACCEPTED, tags=["Configuration"])
def update_engine_config(config_data: EngineConfig):
    """
    Nhận và xác thực cấu hình, sau đó ghi đè file engine_config.json.
    Engine sẽ tự động áp dụng cấu hình mới ở chu kỳ tiếp theo.
    """
    try:
        success, message = save_config(config_data.model_dump())
        
        if not success:
            raise HTTPException(status_code=500, detail=message)
            
        # Đơn giản chỉ trả về thông báo thành công.
        # Engine sẽ tự lo phần còn lại.
        return {"message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

# === THÊM ENDPOINT MỚI ĐỂ NHẬN FEEDBACK ===

@app.post("/api/feedback/", status_code=status.HTTP_201_CREATED, tags=["Feedback"])
def submit_feedback(feedback: schemas.FeedbackCreate):
    """
    Nhận phản hồi từ người dùng và lưu vào file feedback.csv.
    """
    try:
        # Gọi hàm logic cốt lõi từ utils
        success, message = save_feedback_to_csv(
            item_data=feedback.anomaly_data,
            label=feedback.label,
            feedback_path=settings.FEEDBACK_FILE_PATH
        )
        if not success:
            raise HTTPException(status_code=500, detail=message)
        
        return {"message": message}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi không xác định: {str(e)}")