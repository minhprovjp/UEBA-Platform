# backend_api/main_api.py
from fastapi import FastAPI, Depends, HTTPException, status, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy import func, or_, text, cast, Text

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

# --- Helper filter áp dụng chung ---
def _apply_common_filters(q, user: Optional[str], anomaly_type: Optional[str],
                          date_from: Optional[str], date_to: Optional[str],
                          is_aggregate: bool):
    if user:
        q = q.filter((models.AggregateAnomaly.user if is_aggregate else models.Anomaly.user) == user)
    if anomaly_type:
        col = models.AggregateAnomaly.anomaly_type if is_aggregate else models.Anomaly.anomaly_type
        q = q.filter(col == anomaly_type)
    if date_from:
        # ISO-like string -> datetime.fromisoformat (YYYY-MM-DD[THH:MM:SS[.fff]])
        from datetime import datetime
        dtf = datetime.fromisoformat(date_from)
        if is_aggregate:
            q = q.filter(or_(models.AggregateAnomaly.start_time >= dtf,
                             models.AggregateAnomaly.created_at >= dtf))
        else:
            q = q.filter(models.Anomaly.timestamp >= dtf)
    if date_to:
        from datetime import datetime
        dtt = datetime.fromisoformat(date_to)
        if is_aggregate:
            q = q.filter(or_(models.AggregateAnomaly.end_time <= dtt,
                             models.AggregateAnomaly.created_at <= dtt))
        else:
            q = q.filter(models.Anomaly.timestamp <= dtt)
    return q

# --- Stats: đếm đúng 3 con số ---
@app.get("/api/anomalies/stats", response_model=Dict[str, Any], tags=["Anomalies"])
def anomaly_stats(db: Session = Depends(get_db), api_key: str = Security(get_api_key)):
    # 1. Giữ nguyên logic cũ: Đếm tổng
    event_count = db.query(func.count(models.Anomaly.id)).scalar() or 0
    agg_count   = db.query(func.count(models.AggregateAnomaly.id)).scalar() or 0
    total_anomalies = int(event_count + agg_count)

    # 2. THÊM MỚI: Tính Critical Alerts (Ví dụ: những log có score >= 0.8)
    # Backend tự lọc và đếm luôn, Frontend chỉ việc hiển thị số
    crit_events = db.query(func.count(models.Anomaly.id)).filter(models.Anomaly.score >= 0.8).scalar() or 0
    crit_aggs   = db.query(func.count(models.AggregateAnomaly.id)).filter(models.AggregateAnomaly.severity >= 0.8).scalar() or 0
    critical_alerts = int(crit_events + crit_aggs)

    # 3. THÊM MỚI: Tìm Riskiest User (User xuất hiện nhiều nhất trong bảng Anomaly)
    # Dùng SQL sắp xếp giảm dần theo số lượng và lấy người đầu tiên (LIMIT 1)
    top_user = (
        db.query(models.Anomaly.user, func.count(models.Anomaly.id))
        .filter(models.Anomaly.user.isnot(None))
        .group_by(models.Anomaly.user)
        .order_by(func.count(models.Anomaly.id).desc())
        .first()
    )
    riskiest_user = top_user[0] if top_user else "N/A"

    # 4. THÊM MỚI: Chart Data (Dữ liệu biểu đồ)
    # Lấy timestamp của 1000 log gần nhất để phân bố vào các khung giờ
    recent_logs = (
        db.query(models.Anomaly.timestamp)
        .order_by(models.Anomaly.timestamp.desc())
        .limit(1000)
        .all()
    )
    
    # Tạo khung 24 giờ: {"0:00": 0, "1:00": 0, ...}
    hours_count = {f"{i}:00": 0 for i in range(24)}
    
    for log in recent_logs:
        if log.timestamp:
            h = log.timestamp.hour # Lấy giờ (0-23)
            hours_count[f"{h}:00"] += 1
            
    # Chuyển đổi sang format danh sách để Frontend dễ vẽ biểu đồ
    chart_data = [{"name": k, "anomalies": v} for k, v in hours_count.items()]
    # Sắp xếp lại từ 0h đến 23h
    chart_data.sort(key=lambda x: int(x["name"].split(":")[0]))
    
    # 5. THÊM MỚI: Lấy 5 log mới nhất cho danh sách bên phải
    latest_rows = db.query(models.Anomaly).order_by(models.Anomaly.timestamp.desc()).limit(5).all()
    
    latest_logs = []
    for r in latest_rows:
        latest_logs.append({
            "timestamp": r.timestamp,
            "user": r.user,
            "anomaly_type": r.anomaly_type,
            "score": r.score
        })

    # Trả về kết quả đầy đủ cho Frontend
    return {
        "totalAnomalies": total_anomalies, 
        "criticalAlerts": critical_alerts, 
        "riskiestUser": riskiest_user,      
        "chartData": chart_data, 
        "latestLogs": latest_logs,
        
        # Giữ lại các trường cũ (nếu có chỗ khác dùng thì không bị lỗi)
        "event_count": int(event_count),
        "aggregate_count": int(agg_count),
        "total_count": int(total_anomalies),
    }

# NEW: /api/anomalies/type-stats
@app.get("/api/anomalies/type-stats", tags=["Anomalies"])
def anomaly_type_stats(db: Session = Depends(get_db), api_key: str = Security(get_api_key)):
    # Đếm event theo type
    ev = db.query(models.Anomaly.anomaly_type, func.count(models.Anomaly.id))\
           .group_by(models.Anomaly.anomaly_type).all()
    # Đếm aggregate theo type
    ag = db.query(models.AggregateAnomaly.anomaly_type, func.count(models.AggregateAnomaly.id))\
           .group_by(models.AggregateAnomaly.anomaly_type).all()

    from collections import defaultdict
    counts = defaultdict(int)
    for t, c in ev: counts[(t or '').strip()] += int(c or 0)
    for t, c in ag: counts[(t or '').strip()] += int(c or 0)

    # Chuẩn hoá 5 rule + ml
    keys = ["late_night", "dump", "multi_table", "sensitive", "user_time", "ml"]
    by_type = {k: int(counts.get(k, 0)) for k in keys}
    total = sum(counts.values())
    return {"by_type": by_type, "total": int(total)}


# ===== NEW: KPI theo từng rule =====
@app.get("/api/anomalies/kpis", tags=["Anomalies"])
def anomaly_kpis(db: Session = Depends(get_db), api_key: str = Security(get_api_key)):
    # Đếm event theo type
    def ev_count(types):
        if isinstance(types, (list, tuple, set)):
            return db.query(func.count(models.Anomaly.id)).filter(models.Anomaly.anomaly_type.in_(types)).scalar() or 0
        return db.query(func.count(models.Anomaly.id)).filter(models.Anomaly.anomaly_type == types).scalar() or 0

    # Đếm aggregate theo type
    def agg_count(types):
        if isinstance(types, (list, tuple, set)):
            return db.query(func.count(models.AggregateAnomaly.id)).filter(models.AggregateAnomaly.anomaly_type.in_(types)).scalar() or 0
        return db.query(func.count(models.AggregateAnomaly.id)).filter(models.AggregateAnomaly.anomaly_type == types).scalar() or 0

    k_late   = ev_count("late_night") + agg_count("late_night")
    k_dump   = ev_count(["dump", "large_dump"]) + agg_count(["dump", "large_dump"])
    k_multi  = ev_count(["multi_table", "multi_table_access"]) + agg_count(["multi_table", "multi_table_access"])
    k_sens   = ev_count(["sensitive", "sensitive_access", "sensitive_table", "sensitive_table_access"]) + \
               agg_count(["sensitive", "sensitive_access", "sensitive_table", "sensitive_table_access"])
    k_prof   = ev_count(["user_time","profile_deviation"]) + agg_count(["user_time","profile_deviation"])

    total = (db.query(func.count(models.Anomaly.id)).scalar() or 0) + \
            (db.query(func.count(models.AggregateAnomaly.id)).scalar() or 0)

    return {
        "late_night": int(k_late),
        "large_dump": int(k_dump),
        "multi_table": int(k_multi),
        "sensitive_access": int(k_sens),
        "profile_deviation": int(k_prof),
        "total": int(total),
    }


# ===== NEW: Facets (users/types) tính trên TOÀN BỘ dataset =====
@app.get("/api/anomalies/facets", tags=["Anomalies"])
def anomaly_facets(db: Session = Depends(get_db), api_key: str = Security(get_api_key)):
    users = set(u for (u,) in db.query(models.Anomaly.user).distinct() if u) | \
            set(u for (u,) in db.query(models.AggregateAnomaly.user).distinct() if u)

    types = set(t for (t,) in db.query(models.Anomaly.anomaly_type).distinct() if t) | \
            set(t for (t,) in db.query(models.AggregateAnomaly.anomaly_type).distinct() if t)

    return {
        "users": sorted(users),
        "types": sorted(types),
    }


# ===== NEW: Search gộp (event + aggregate) + phân trang server-side =====
@app.get("/api/anomalies/search", tags=["Anomalies"])
def anomaly_search(
    skip: int = 0,
    limit: int = 20,
    search: Optional[str] = None,
    user: Optional[str] = None,
    anomaly_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    db: Session = Depends(get_db),
    api_key: str = Security(get_api_key),
):
    # Event
    q_ev = db.query(models.Anomaly)
    if search:
        q_ev = q_ev.filter(or_(models.Anomaly.query.ilike(f"%{search}%"),
                               models.Anomaly.reason.ilike(f"%{search}%")))
    q_ev = _apply_common_filters(q_ev, user, anomaly_type, date_from, date_to, is_aggregate=False)
    ev_total = q_ev.count()
    ev_rows = (q_ev.order_by(models.Anomaly.timestamp.desc())
                  .offset(skip).limit(limit).all())

    # Aggregate
    q_ag = db.query(models.AggregateAnomaly)
    if search:
        q_ag = q_ag.filter(or_(
            models.AggregateAnomaly.reason.ilike(f"%{search}%"),
            models.AggregateAnomaly.details.cast(Text).ilike(f"%{search}%")
        ))
    q_ag = _apply_common_filters(q_ag, user, anomaly_type, date_from, date_to, is_aggregate=True)
    ag_total = q_ag.count()
    ag_rows = (q_ag.order_by(models.AggregateAnomaly.start_time.desc().nullslast(),
                             models.AggregateAnomaly.created_at.desc())
                  .offset(skip).limit(limit).all())

    # Hợp nhất + sort thời gian
    items = []
    for r in ev_rows:
        items.append(schemas.UnifiedAnomaly(
            id=f"event-{r.id}", source="event",
            anomaly_type=r.anomaly_type, timestamp=r.timestamp,
            user=r.user, database=r.database, query=r.query,
            reason=r.reason, score=r.score, scope="log", details=None
        ))
    for r in ag_rows:
        items.append(schemas.UnifiedAnomaly(
            id=f"agg-{r.id}", source="aggregate",
            anomaly_type=r.anomaly_type,
            timestamp=r.start_time or r.end_time or r.created_at,
            user=r.user, database=r.database, query=None,
            reason=r.reason, score=r.severity, scope=r.scope, details=r.details
        ))

    items.sort(key=lambda x: x.timestamp or datetime.min, reverse=True)
    return {"items": items, "total": int(ev_total + ag_total)}



# --- Danh sách Event-level anomalies (chuẩn paging) ---
@app.get("/api/anomalies/events", response_model=List[schemas.UnifiedAnomaly], tags=["Anomalies"])
def list_event_anomalies(skip: int = 0, limit: int = 100,
                         search: Optional[str] = None,
                         user: Optional[str] = None,
                         anomaly_type: Optional[str] = None,
                         date_from: Optional[str] = None,
                         date_to: Optional[str] = None,
                         db: Session = Depends(get_db),
                         api_key: str = Security(get_api_key)):
    q = db.query(models.Anomaly)
    if search:
        q = q.filter(or_(models.Anomaly.query.ilike(f"%{search}%"),
                         models.Anomaly.reason.ilike(f"%{search}%")))
    q = _apply_common_filters(q, user, anomaly_type, date_from, date_to, is_aggregate=False)
    rows = (q.order_by(models.Anomaly.timestamp.desc())
              .offset(skip).limit(limit).all())
    return [schemas.UnifiedAnomaly(
        id=f"event-{r.id}",
        source="event",
        anomaly_type=r.anomaly_type,
        timestamp=r.timestamp,
        user=r.user,
        database=r.database,
        query=r.query,
        reason=r.reason,
        score=r.score,
        scope="log",
        details=None
    ) for r in rows]

# --- Danh sách Aggregate-level anomalies (chuẩn paging) ---
@app.get("/api/aggregate-anomalies", response_model=List[schemas.UnifiedAnomaly], tags=["Anomalies"])
def list_aggregate_anomalies(skip: int = 0, limit: int = 100,
                             search: Optional[str] = None,
                             user: Optional[str] = None,
                             anomaly_type: Optional[str] = None,
                             date_from: Optional[str] = None,
                             date_to: Optional[str] = None,
                             db: Session = Depends(get_db),
                             api_key: str = Security(get_api_key)):
    q = db.query(models.AggregateAnomaly)
    if search:
        # tìm trong reason hoặc details (cast text)
        q = q.filter(or_(
            models.AggregateAnomaly.reason.ilike(f"%{search}%"),
            cast(models.AggregateAnomaly.details, Text).ilike(f"%{search}%")
        ))
    q = _apply_common_filters(q, user, anomaly_type, date_from, date_to, is_aggregate=True)
    rows = (q.order_by(models.AggregateAnomaly.start_time.desc().nullslast(),
                       models.AggregateAnomaly.created_at.desc())
              .offset(skip).limit(limit).all())
    return [schemas.UnifiedAnomaly(
        id=f"agg-{r.id}",
        source="aggregate",
        anomaly_type=r.anomaly_type,
        timestamp=r.start_time or r.end_time or r.created_at,
        user=r.user,
        database=r.database,
        query=None,
        reason=r.reason,
        score=r.severity,
        scope=r.scope,
        details=r.details
    ) for r in rows]

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