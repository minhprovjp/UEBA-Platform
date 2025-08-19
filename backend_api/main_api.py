# backend_api/main_api.py
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Dict, Any

# Import các thành phần từ các file trong cùng thư mục
from . import models, schemas
from .models import SessionLocal, engine

# Import engine và trình quản lý config
from engine.engine_runner import AnalysisEngine
from engine.config_manager import load_config, save_config

# === TẠO MỘT INSTANCE DUY NHẤT CỦA ENGINE ===
# Instance này sẽ tồn tại suốt vòng đời của API server
engine_instance = AnalysisEngine()

# Tạo các bảng trong CSDL nếu chúng chưa tồnTAIN (chỉ chạy khi API khởi động)
models.Base.metadata.create_all(bind=engine)

# Khởi tạo ứng dụng FastAPI
app = FastAPI(
    title="User Behavior Analytics API",
    description="API để truy vấn các bất thường được phát hiện bởi Engine Phân tích Log.",
    version="1.0.0"
)

# Cấu hình CORS
origins = [
    "http://localhost:5173",  # Địa chỉ của Vite React dev server
    "http://localhost:3000",  # Thêm địa chỉ phổ biến khác của React
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Cho phép các nguồn gốc trong danh sách
    allow_credentials=True, # Cho phép gửi cookie (nếu có)
    allow_methods=["*"],    # Cho phép tất cả các phương thức (GET, POST, PUT, DELETE,...)
    allow_headers=["*"],    # Cho phép tất cả các header
)

# --- Dependency Injection: Cung cấp DB Session cho các endpoint ---
def get_db():
    """
    Tạo và cung cấp một session CSDL cho mỗi yêu cầu, và đảm bảo nó
    luôn được đóng lại sau khi yêu cầu hoàn tất.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# === ĐỊNH NGHĨA API ENDPOINT ĐẦU TIÊN ===

@app.get("/api/anomalies/", response_model=List[schemas.Anomaly], tags=["Anomalies"])
def read_anomalies(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """
    Lấy ra một danh sách các bất thường, hỗ trợ phân trang (pagination).
    - **skip**: Bỏ qua bao nhiêu bản ghi đầu tiên.
    - **limit**: Giới hạn số lượng bản ghi trả về.
    """
    anomalies = db.query(models.Anomaly).order_by(models.Anomaly.timestamp.desc()).offset(skip).limit(limit).all()
    return anomalies

@app.get("/api/anomalies/{anomaly_id}", response_model=schemas.Anomaly, tags=["Anomalies"])
def read_anomaly_by_id(anomaly_id: int, db: Session = Depends(get_db)):
    """
    Lấy thông tin chi tiết của một bất thường cụ thể bằng ID của nó.
    """
    anomaly = db.query(models.Anomaly).filter(models.Anomaly.id == anomaly_id).first()
    if anomaly is None:
        raise HTTPException(status_code=404, detail="Anomaly not found")
    return anomaly


# === CÁC ENDPOINT MỚI ĐỂ ĐIỀU KHIỂN ENGINE ===

@app.get("/api/engine/status", tags=["Engine Control"])
def get_engine_status():
    """Lấy trạng thái hiện tại của Engine Phân tích."""
    return engine_instance.get_status()

@app.post("/api/engine/start", status_code=status.HTTP_202_ACCEPTED, tags=["Engine Control"])
def start_engine():
    """Khởi động vòng lặp phân tích của Engine trong nền."""
    if engine_instance.get_status()["is_running"]:
        raise HTTPException(status_code=409, detail="Engine đã đang chạy.")
    engine_instance.start()
    return {"message": "Đã gửi yêu cầu khởi động Engine."}

@app.post("/api/engine/stop", status_code=status.HTTP_202_ACCEPTED, tags=["Engine Control"])
def stop_engine():
    """Dừng vòng lặp phân tích của Engine."""
    if not engine_instance.get_status()["is_running"]:
        raise HTTPException(status_code=409, detail="Engine đã dừng.")
    engine_instance.stop()
    return {"message": "Đã gửi yêu cầu dừng Engine."}

@app.get("/api/engine/config", response_model=Dict[str, Any], tags=["Engine Control"])
def get_engine_config():
    """Đọc cấu hình hiện tại của Engine từ file engine_config.json."""
    return load_config()

@app.put("/api/engine/config", status_code=status.HTTP_202_ACCEPTED, tags=["Engine Control"])
def update_engine_config(config_data: Dict[str, Any]):
    """
    Cập nhật và ghi đè file engine_config.json.
    Engine sẽ tự động áp dụng cấu hình mới ở chu kỳ tiếp theo.
    """
    success, message = save_config(config_data)
    if not success:
        raise HTTPException(status_code=500, detail=message)
    # Tải lại cấu hình cho instance đang chạy
    engine_instance.config = load_config()
    return {"message": message}