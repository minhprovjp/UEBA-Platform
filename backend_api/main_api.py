# backend_api/main_api.py
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from contextlib import asynccontextmanager
import asyncio
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import các thành phần từ các file trong cùng thư mục
from . import models, schemas
from .models import SessionLocal, engine

# Import engine và trình quản lý config
import sys
import os
# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from engine.engine_runner import AnalysisEngine
from engine.config_manager import load_config, save_config
from engine.utils import save_feedback_to_csv

from pydantic import BaseModel
# Import hàm phân tích LLM
from engine.llm_analyzer import analyze_query_with_llm, analyze_session_with_llm

from pydantic import BaseModel
# Import hàm phân tích LLM
from engine.llm_analyzer import analyze_query_with_llm, analyze_session_with_llm

# Global variable to store engine instance
engine_instance = None

# === LIFECYCLE MANAGEMENT ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global engine_instance
    try:
        logger.info("Starting up UBA API server...")
        engine_instance = AnalysisEngine()
        
        # Tạo các bảng trong CSDL nếu chúng chưa tồn tại
        models.Base.metadata.create_all(bind=engine)
        logger.info("Database tables initialized")
        
        logger.info("UBA API server started successfully")
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        raise
    
    yield
    
    # Shutdown
    try:
        logger.info("Shutting down UBA API server...")
        if engine_instance:
            # Stop the engine gracefully if it's running
            try:
                engine_instance.stop()
                logger.info("Analysis engine stopped")
            except Exception as e:
                logger.warning(f"Error stopping engine: {e}")
        
        # Close database connections
        try:
            engine.dispose()
            logger.info("Database connections closed")
        except Exception as e:
            logger.warning(f"Error closing database: {e}")
        
        logger.info("UBA API server shutdown complete")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

# Khởi tạo ứng dụng FastAPI với lifecycle management
app = FastAPI(
    title="User Behavior Analytics API",
    description="API để truy vấn các bất thường được phát hiện bởi Engine Phân tích Log.",
    version="1.0.0",
    lifespan=lifespan
)

# Cấu hình CORS
origins = [
    "http://localhost:5173",  # Địa chỉ của Vite React dev server
    "http://localhost:3000",  # Thêm địa chỉ phổ biến khác của React
    "http://127.0.0.1:3000",   # Next.js default
    "http://127.0.0.1:5173",   # Alternative localhost format
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Cho phép các nguồn gốc trong danh sách
    allow_credentials=True, # Cho phép gửi cookie (nếu có)
    allow_methods=["*"],    # Cho phép tất cả các phương thức (GET, POST, PUT, DELETE,...)
    allow_headers=["*"],    # Cho phép tất cả các header
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

@app.get("/api/engine/status", tags=["Engine Control"])
def get_engine_status():
    """Lấy trạng thái hiện tại của Engine Phân tích."""
    if engine_instance is None:
        raise HTTPException(status_code=503, detail="Engine instance not available")
    return engine_instance.get_status()

@app.post("/api/engine/start", status_code=status.HTTP_202_ACCEPTED, tags=["Engine Control"])
def start_engine():
    """Khởi động vòng lặp phân tích của Engine trong nền."""
    if engine_instance is None:
        raise HTTPException(status_code=503, detail="Engine instance not available")
    
    status_info = engine_instance.get_status()
    if status_info.get("is_running", False):
        raise HTTPException(status_code=409, detail="Engine đã đang chạy.")
    
    engine_instance.start()
    return {"message": "Đã gửi yêu cầu khởi động Engine."}

@app.post("/api/engine/stop", status_code=status.HTTP_202_ACCEPTED, tags=["Engine Control"])
def stop_engine():
    """Dừng vòng lặp phân tích của Engine."""
    if engine_instance is None:
        raise HTTPException(status_code=503, detail="Engine instance not available")
    
    status_info = engine_instance.get_status()
    if not status_info.get("is_running", False):
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
<<<<<<< Updated upstream
    # Tải lại cấu hình cho instance đang chạy
    engine_instance.config = load_config()
    return {"message": message}

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
def update_engine_config(config_data: Dict[str, Any]):
    """
    Nhận một đối tượng JSON và ghi đè hoàn toàn file engine_config.json.
    Engine sẽ tự động áp dụng cấu hình mới ở chu kỳ tiếp theo.
    """
    success, message = save_config(config_data)
    if not success:
        raise HTTPException(status_code=500, detail=message)
    
    # Tải lại cấu hình cho instance engine đang chạy để nó áp dụng ngay lập tức
    if 'engine_instance' in globals():
        engine_instance.config = load_config()
        
    return {"message": message}

=======
    
    # Tải lại cấu hình cho instance đang chạy (nếu có)
    if engine_instance is not None:
        try:
            engine_instance.config = load_config()
        except Exception as e:
            logger.warning(f"Could not update engine instance config: {e}")
    
    return {"message": message}

# === NOTE: Engine config endpoints are already defined above ===
# Removed duplicate endpoint definitions to avoid conflicts

>>>>>>> Stashed changes
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
            label=feedback.label
        )
        if not success:
            raise HTTPException(status_code=500, detail=message)
        
        return {"message": message}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi không xác định: {str(e)}")