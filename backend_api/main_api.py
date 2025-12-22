# backend_api/main_api.py
from fastapi import FastAPI, Depends, HTTPException, status, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import func, or_, text, cast, Text
import re 
from pathlib import Path
from deep_translator import GoogleTranslator

# Import self_monitoring router
from . import self_monitoring_api

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
    from engine.llm_analyzer import analyze_query_with_llm
except ImportError as e:
    print("="*50)
    print(f"LỖI IMPORT NGHIÊM TRỌNG: {e}")
    print(">>> BẠN ĐÃ TẠO FILE TRỐNG 'engine/__init__.py' CHƯA? <<<")
    print("="*50)
    sys.exit(1)

from pydantic import BaseModel
from fastapi.security import OAuth2PasswordRequestForm
from . import auth

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
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ENGINE_START_TIME = datetime.now()

# --- Dependency Injection: Cung cấp DB Session cho các endpoint ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 1. Endpoint Login (Nhận username/password -> Trả về Token)
@app.post("/api/login", tags=["Authentication"])
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    # Tìm user trong DB
    user = db.query(models.User).filter(models.User.username == form_data.username).first()
    
    # Kiểm tra user và pass
    if not user or not auth.verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Tạo token
    access_token_expires = timedelta(minutes=auth.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth.create_access_token(
        data={"sub": user.username, "role": user.role},
        expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

# 2. Endpoint tạo user đầu tiên (Để bạn test, sau này xóa đi cũng được)
@app.post("/api/register", tags=["Authentication"])
def register_user(username: str, password: str, db: Session = Depends(get_db)):
    hashed_password = auth.get_password_hash(password)
    new_user = models.User(username=username, hashed_password=hashed_password, role="admin")
    try:
        db.add(new_user)
        db.commit()
    except Exception as e:
        db.rollback() # Quan trọng! Hoàn tác nếu lỗi
        raise HTTPException(status_code=500, detail=str(e))
    return {"msg": "User created"}

@app.post("/api/change-password", tags=["Authentication"])
def change_password(
    request: schemas.ChangePasswordRequest, 
    current_user: models.User = Depends(auth.get_current_user), 
    db: Session = Depends(get_db)
):
    # 1. Kiểm tra Mật khẩu cũ
    if not auth.verify_password(request.current_password, current_user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Mật khẩu hiện tại không chính xác."
        )

    # 2. Validate độ dài
    if len(request.new_password) < 6:
         raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Mật khẩu mới phải có ít nhất 6 ký tự."
        )

    # 3. [FIX QUAN TRỌNG] Truy vấn lại user từ DB session hiện tại để đảm bảo UPDATE được ghi nhận
    user_in_db = db.query(models.User).filter(models.User.id == current_user.id).first()
    
    if not user_in_db:
        raise HTTPException(status_code=404, detail="User not found")

    # 4. Hash mật khẩu mới và cập nhật
    user_in_db.hashed_password = auth.get_password_hash(request.new_password)
    
    # 5. Lưu thay đổi
    db.add(user_in_db) # Đánh dấu object này cần được update
    db.commit()        # Thực thi lệnh COMMIT xuống database
    db.refresh(user_in_db) # Làm mới object (tùy chọn)
    
    return {"msg": "Đổi mật khẩu thành công! Vui lòng đăng nhập lại."}

app.include_router(self_monitoring_api.router)

# === CÁC ENDPOINT LẤY DỮ LIỆU BẤT THƯỜNG (CHO FRONTEND) ===

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

@app.get("/api/anomalies/stats", response_model=Dict[str, Any], tags=["Anomalies"])
def anomaly_stats(
    time_range: str = "D",
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user)
):
    # 1. Xử lý thời gian lọc (Giữ nguyên logic cũ)
    now = datetime.now()
    if time_range == "W":
        start_date = now - timedelta(days=7)
        group_mode = "day"
    elif time_range == "M":
        start_date = now - timedelta(days=30)
        group_mode = "day"
    elif time_range == "6M":
        start_date = now - timedelta(days=180)
        group_mode = "month"
    elif time_range == "Y":
        start_date = now - timedelta(days=365)
        group_mode = "month"
    else: # Default 24h
        start_date = now - timedelta(hours=24)
        group_mode = "hour"

    # 2. KPI Tổng quan (Giữ nguyên)
    total_scanned = db.query(func.count(models.AllLogs.id)).scalar() or 0
    event_count = db.query(func.count(models.Anomaly.id)).scalar() or 0
    agg_count = db.query(func.count(models.AggregateAnomaly.id)).scalar() or 0
    total_anomalies = int(event_count + agg_count)
    
    crit_events = db.query(func.count(models.Anomaly.id)).filter(
        or_(
            # models.Anomaly.score >= 0.9, 
            models.Anomaly.behavior_group.in_(['TECHNICAL_ATTACK', 'DATA_DESTRUCTION', 'SQL Injection'])
        )
    ).scalar() or 0
    crit_aggs = db.query(func.count(models.AggregateAnomaly.id)).filter(models.AggregateAnomaly.anomaly_type.in_(['TECHNICAL_ATTACK', 'DATA_DESTRUCTION', 'SQL Injection'])).scalar() or 0
    critical_alerts = int(crit_events + crit_aggs)

    # 3. Chart Data (Giữ nguyên logic cũ)
    event_query = db.query(models.Anomaly).filter(models.Anomaly.timestamp >= start_date).all()
    agg_query = db.query(models.AggregateAnomaly).filter(models.AggregateAnomaly.created_at >= start_date).all()

    chart_query = event_query + agg_query

    data_map = {}
    if group_mode == "hour":
        for i in range(24):
            h = (now - timedelta(hours=23-i)).hour
            key = f"{h}:00"
            data_map[key] = 0
    elif group_mode == "month":
        curr = start_date.replace(day=1)
        end_date = now.replace(day=1)
        while curr <= end_date:
            key = curr.strftime("%m/%Y")
            data_map[key] = 0
            if curr.month == 12:
                curr = curr.replace(year=curr.year + 1, month=1)
            else:
                curr = curr.replace(month=curr.month + 1)
    else:
        days = (now - start_date).days + 1
        for i in range(days):
            d = (start_date + timedelta(days=i)).strftime("%d/%m")
            data_map[d] = 0
            
    for log in chart_query:
        # Lấy thời gian an toàn: Ưu tiên timestamp -> created_at
        log_time = getattr(log, 'timestamp', getattr(log, 'created_at', None))
        
        if not log_time: continue # Bỏ qua nếu lỗi dữ liệu

        if group_mode == "hour":
            key = f"{log_time.hour}:00"
        elif group_mode == "month":
            key = log_time.strftime("%m/%Y")
        else:
            key = log_time.strftime("%d/%m")
        
        if key in data_map: 
            data_map[key] += 1
            
    chart_data = [{"name": k, "anomalies": v} for k, v in data_map.items()]

    # 4. Top Risky Users (Giữ nguyên)
    risky_users_query = (
        db.query(
            models.Anomaly.user, 
            func.sum(models.Anomaly.score).label("total_score"),
            func.count(models.Anomaly.id).label("violation_count")
        )
        .filter(models.Anomaly.user.isnot(None))
        .group_by(models.Anomaly.user)
        .order_by(text("total_score DESC"))
        .limit(10)
        .all()
    )
    top_risky_users = [{"user": u, "score": float(s or 0), "count": c} for u, s, c in risky_users_query]

    # 5. Detection Stats (CẬP NHẬT MAPPING MỚI)
    group_counts = (
        db.query(models.Anomaly.behavior_group, func.count(models.Anomaly.id))
        .filter(models.Anomaly.behavior_group.isnot(None))
        .group_by(models.Anomaly.behavior_group)
        .all()
    )

    # Config màu sắc theo NHÓM
    GROUP_CONFIG = {
        "TECHNICAL_ATTACK": {"label": "Technical Attacks", "color": "#ef4444"},   # Đỏ
        "INSIDER_THREAT": {"label": "Insider Threats", "color": "#f97316"},       # Cam
        "DATA_DESTRUCTION": {"label": "Data Destruction", "color": "#dc2626"},    # Đỏ đậm
        "ACCESS_ANOMALY": {"label": "Access Anomalies", "color": "#eab308"},      # Vàng
        "MULTI_TABLE_ACCESS": {"label": "Mass/Multi-Table", "color": "#3b82f6"}, # Xanh dương
        "UNUSUAL_BEHAVIOR": {"label": "Behavioral Profile", "color": "#d946ef"},  # Hồng
        "ML_DETECTED": {"label": "AI/ML Detected", "color": "#8b5cf6"},           # Tím
        "UNKNOWN": {"label": "Other", "color": "#71717a"}                         # Xám
    }

    detection_stats = []
    for group_code, count in group_counts:
        # Nếu group code không có trong config thì fallback về UNKNOWN
        config = GROUP_CONFIG.get(group_code, GROUP_CONFIG["UNKNOWN"])
        
        detection_stats.append({
            "name": config["label"],
            "value": count,
            "color": config["color"]
        })

    # Cộng thêm Aggregate Anomalies vào nhóm Multi-table cho biểu đồ (nếu chưa có)
    agg_count = db.query(func.count(models.AggregateAnomaly.id)).scalar() or 0
    if agg_count > 0:
        found = False
        for item in detection_stats:
            if item["name"] == GROUP_CONFIG["MULTI_TABLE_ACCESS"]["label"]:
                item["value"] += agg_count
                found = True
                break
        if not found:
             detection_stats.append({
                "name": GROUP_CONFIG["MULTI_TABLE_ACCESS"]["label"],
                "value": agg_count,
                "color": GROUP_CONFIG["MULTI_TABLE_ACCESS"]["color"]
            })

    detection_stats.sort(key=lambda x: x['value'], reverse=True)

    # 6. Targeted DBs & Latest Feed (Giữ nguyên)
    targeted_dbs_query = (
        db.query(models.Anomaly.database, func.count(models.Anomaly.id))
        .filter(models.Anomaly.database.isnot(None))
        .group_by(models.Anomaly.database)
        .order_by(func.count(models.Anomaly.id).desc())
        .limit(5)
        .all()
    )
    targeted_entities = [{"name": db_name, "value": count} for db_name, count in targeted_dbs_query]

    latest_events = (
        db.query(models.Anomaly)
        .order_by(models.Anomaly.timestamp.desc())
        .limit(10)
        .all()
    )
    latest_logs = [
        {
            "id": f"evt-{r.id}", 
            "timestamp": r.timestamp, 
            "user": r.user,
            "anomaly_type": r.anomaly_type, 
            "behavior_group": r.behavior_group,
            "score": r.score, 
            "source": "event",
            "query": r.query,        
            "reason": r.reason,      
            "database": r.database,  
            "client_ip": r.client_ip 
        } for r in latest_events
    ]

    current_time = datetime.now()
    uptime_delta = current_time - ENGINE_START_TIME
    uptime_seconds = int(uptime_delta.total_seconds())

    return {
        "total_scanned": total_scanned,
        "total_anomalies": total_anomalies,
        "critical_alerts": critical_alerts,
        "riskiest_users": top_risky_users,
        "detection_stats": detection_stats,
        "targeted_entities": targeted_entities,
        "chartData": chart_data,
        "latestLogs": latest_logs,
        "system_status": {
            "uptime_seconds": uptime_seconds,
            "status": "ONLINE",
            "logs_processed": total_scanned
            }
    }


@app.get("/api/anomalies/kpis", response_model=schemas.AnomalyKpis, tags=["Anomalies"])
def anomaly_kpis(db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
    """
    Thống kê KPI theo Nhóm Hành Vi (Behavior Group) được định nghĩa trong Data Processor.
    """
    
    # 1. Query Group-by trên bảng Anomaly để lấy số lượng từng nhóm
    # Kết quả trả về dạng: [('ACCESS_ANOMALY', 5), ('TECHNICAL_ATTACK', 10), ...]
    group_counts = (
        db.query(models.Anomaly.behavior_group, func.count(models.Anomaly.id))
        .group_by(models.Anomaly.behavior_group)
        .all()
    )
    
    # Chuyển list tuple thành dict cho dễ truy xuất: {'ACCESS_ANOMALY': 5, ...}
    # Sử dụng (g or 'UNKNOWN') để tránh lỗi nếu behavior_group bị null
    counts_map = { (g or 'UNKNOWN'): c for g, c in group_counts }

    # 2. Đếm Aggregate Anomaly (Thường là Multi-table sessions)
    # Bảng AggregateAnomaly không có behavior_group, nhưng bản chất nó là MULTI_TABLE_ACCESS
    agg_count = db.query(func.count(models.AggregateAnomaly.id)).scalar() or 0

    # 3. Tổng hợp số liệu vào Schema
    # Lưu ý: 'UNUSUAL_BEHAVIOR' trong DB tương ứng với 'behavioral_profile' hiển thị
    
    stats = {
        "access_anomaly": counts_map.get("ACCESS_ANOMALY", 0),
        "insider_threat": counts_map.get("INSIDER_THREAT", 0),
        "technical_attack": counts_map.get("TECHNICAL_ATTACK", 0),
        "data_destruction": counts_map.get("DATA_DESTRUCTION", 0),
        "ml_detected": counts_map.get("ML_DETECTED", 0),
        
        # Multi-table = (Số lượng trong bảng Anomaly nếu có) + (Số lượng Sessions gộp)
        "multi_table": counts_map.get("MULTI_TABLE_ACCESS", 0) + agg_count,
        
        # Behavioral Profile = Nhóm UNUSUAL_BEHAVIOR
        "behavioral_profile": counts_map.get("UNUSUAL_BEHAVIOR", 0)
    }

    # Tính tổng Total
    total_events = db.query(func.count(models.Anomaly.id)).scalar() or 0
    stats["total"] = total_events + agg_count

    return stats

# ===== NEW: Facets (users/types) tính trên TOÀN BỘ dataset =====
@app.get("/api/anomalies/facets", tags=["Anomalies"])
def anomaly_facets(
    behavior_group: Optional[str] = None, # [NEW] Thêm tham số này
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Trả về danh sách Users và Types (Rules) có dữ liệu thực tế.
    Nếu có behavior_group, chỉ trả về users/types thuộc nhóm đó.
    """
    # 1. Định nghĩa Query cơ bản
    q_ev_user = db.query(models.Anomaly.user)
    q_ev_type = db.query(models.Anomaly.anomaly_type) # [NEW] Query cho Type
    
    q_ag_user = db.query(models.AggregateAnomaly.user)
    q_ag_type = db.query(models.AggregateAnomaly.anomaly_type) # [NEW] Query cho Type

    # 2. Áp dụng bộ lọc Behavior Group
    if behavior_group:
        # Lọc cho Event Anomaly
        q_ev_user = q_ev_user.filter(models.Anomaly.behavior_group == behavior_group)
        q_ev_type = q_ev_type.filter(models.Anomaly.behavior_group == behavior_group)
        
        # Lọc cho Aggregate Anomaly (Hiện tại chỉ có Multi-table)
        if behavior_group == "MULTI_TABLE_ACCESS":
             # Aggregate giữ nguyên, không cần filter thêm vì nó mặc định là multi-table
             pass 
        else:
             # Nếu nhóm khác -> Aggregate rỗng
             q_ag_user = q_ag_user.filter(text("1=0"))
             q_ag_type = q_ag_type.filter(text("1=0"))

    # 3. Lấy Distinct (Chỉ lấy những giá trị có tồn tại)
    # Users
    users_ev = set(u for (u,) in q_ev_user.distinct() if u)
    users_ag = set(u for (u,) in q_ag_user.distinct() if u)
    final_users = sorted(list(users_ev | users_ag))

    # Types (Rules) - [NEW LOGIC]
    types_ev = set(t for (t,) in q_ev_type.distinct() if t)
    types_ag = set(t for (t,) in q_ag_type.distinct() if t)
    final_types = sorted(list(types_ev | types_ag))

    return {
        "users": final_users,
        "types": final_types,
    }


# ===== NEW: Search gộp (event + aggregate) + phân trang server-side =====
@app.get("/api/anomalies/search", tags=["Anomalies"])
def anomaly_search(
    skip: int = 0,
    limit: int = 20,
    search: Optional[str] = None,
    user: Optional[str] = None,
    anomaly_type: Optional[str] = None,
    behavior_group: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user),
):
    # Event
    q_ev = db.query(models.Anomaly)
    if search:
        q_ev = q_ev.filter(or_(models.Anomaly.query.ilike(f"%{search}%"),
                               models.Anomaly.reason.ilike(f"%{search}%")))
    if behavior_group:
        q_ev = q_ev.filter(models.Anomaly.behavior_group == behavior_group)
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
    if behavior_group:
        if behavior_group == "MULTI_TABLE_ACCESS":
             # Lấy tất cả aggregate vì hiện tại aggregate chỉ dùng cho multi-table
             pass 
        else:
             # Nếu lọc nhóm khác thì aggregate = 0 (vì aggregate hiện tại chỉ là multi-table)
             q_ag = q_ag.filter(text("1=0"))
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
            anomaly_type=r.anomaly_type, behavior_group=r.behavior_group, timestamp=r.timestamp,
            user=r.user, client_ip=r.client_ip, database=r.database, query=r.query,
            reason=r.reason, score=r.score, scope="log", details=None, ai_analysis=r.ai_analysis
        ))
    for r in ag_rows:
        items.append(schemas.UnifiedAnomaly(
            id=f"agg-{r.id}", source="aggregate",
            anomaly_type=r.anomaly_type,
            behavior_group="MULTI_TABLE_ACCESS",
            timestamp=r.start_time or r.end_time or r.created_at,
            user=r.user, client_ip=getattr(r, 'client_ip', None), database=r.database, query=None,
            reason=r.reason, score=r.severity, scope=r.scope, details=r.details, ai_analysis=r.ai_analysis
        ))

    items.sort(key=lambda x: x.timestamp or datetime.min, reverse=True)

    paginated_items = items[:limit]

    return {"items": paginated_items, "total": int(ev_total + ag_total)}



# --- Danh sách Event-level anomalies (chuẩn paging) ---
@app.get("/api/anomalies/events", response_model=List[schemas.UnifiedAnomaly], tags=["Anomalies"])
def list_event_anomalies(skip: int = 0, limit: int = 100,
                         search: Optional[str] = None,
                         user: Optional[str] = None,
                         anomaly_type: Optional[str] = None,
                         date_from: Optional[str] = None,
                         date_to: Optional[str] = None,
                         db: Session = Depends(get_db),
                         current_user: models.User = Depends(auth.get_current_user)):
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
        behavior_group=r.behavior_group,
        timestamp=r.timestamp,
        user=r.user,
        database=r.database,
        query=r.query,
        reason=r.reason,
        score=r.score,
        scope="log",
        details=None,
        ai_analysis=r.ai_analysis
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
                             current_user: models.User = Depends(auth.get_current_user)):
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
        details=r.details,
        ai_analysis=r.ai_analysis
    ) for r in rows]

@app.get("/api/anomalies/",response_model=List[schemas.UnifiedAnomaly],tags=["Anomalies"])
def read_unified_anomalies(skip: int = 0,limit: int = 100,db: Session = Depends(get_db),current_user: models.User = Depends(auth.get_current_user)):
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
                behavior_group=a.behavior_group,
                timestamp=a.timestamp,
                user=a.user,
                database=a.database,
                query=a.query,
                reason=a.reason,
                score=a.score,
                scope="log",
                details=None,
                ai_analysis=a.ai_analysis
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
                behavior_group="MULTI_TABLE_ACCESS",
                timestamp=a.start_time or a.end_time or a.created_at,
                user=a.user,
                database=a.database,
                query=None,
                reason=a.reason,
                score=a.severity,
                scope=a.scope,
                details=a.details,
                ai_analysis=a.ai_analysis
            )
        )

    # 3) Sort chung theo thời gian mới nhất
    def sort_key(item: schemas.UnifiedAnomaly):
        return item.timestamp or datetime.min

    unified.sort(key=sort_key, reverse=True)

    # 4) Áp dụng skip/limit ở unified list
    return unified[skip : skip + limit]

@app.get("/api/anomalies/{anomaly_id}", response_model=schemas.Anomaly, tags=["Anomalies"])
def read_anomaly_by_id(anomaly_id: int, db: Session = Depends(get_db), current_user: models.User = Depends(auth.get_current_user)):
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
    current_user: models.User = Depends(auth.get_current_user)
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
def analyze_anomaly_with_llm_endpoint(
    request: schemas.AnomalyAnalysisRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(auth.get_current_user)
):
    try:
        # Load config
        engine_config = load_config()
        llm_settings = engine_config.get("llm_config", {})
        rules_settings = engine_config.get("analysis_params", {})
        
        req_id = request.id
        anomaly_data = None
        record = None # Biến lưu object DB để update sau này
        
        # --- 1. LẤY DỮ LIỆU ---
        if req_id.startswith("log-"):
            # Log thường không lưu lại kết quả AI vào bảng logs vì quá lớn
            db_id = int(req_id.split("-")[1])
            log_record = db.query(models.AllLogs).filter(models.AllLogs.id == db_id).first()
            if log_record:
                anomaly_data = {c.name: getattr(log_record, c.name) for c in log_record.__table__.columns}
                anomaly_data['anomaly_type'] = "Manual Investigation (Log Explorer)"
        
        elif req_id.startswith("event-"):
            db_id = int(req_id.split("-")[1])
            record = db.query(models.Anomaly).filter(models.Anomaly.id == db_id).first()
            if record:
                anomaly_data = {c.name: getattr(record, c.name) for c in record.__table__.columns}

        elif req_id.startswith("agg-"):
            db_id = int(req_id.split("-")[1])
            record = db.query(models.AggregateAnomaly).filter(models.AggregateAnomaly.id == db_id).first()
            if record:
                anomaly_data = {c.name: getattr(record, c.name) for c in record.__table__.columns}
                if not anomaly_data.get('query') and anomaly_data.get('details'):
                     anomaly_data['query'] = f"SESSION DATA: {str(anomaly_data['details'])[:500]}..."

        if not anomaly_data:
             raise HTTPException(status_code=404, detail=f"Data not found for ID: {req_id}")

        if not anomaly_data.get('query'):
             anomaly_data['query'] = "N/A (No query provided)"

        # --- 2. GỌI AI PHÂN TÍCH ---
        print(f"Start analyzing {req_id}...")
        analysis_result = analyze_query_with_llm(
            anomaly_row=anomaly_data,
            anomaly_type_from_system=anomaly_data.get('anomaly_type', 'Unknown'),
            llm_config=llm_settings,
            rules_config=rules_settings
        )
        
        if not analysis_result:
            raise ValueError("Analyzer returned None")

        # --- PHẦN 3: XỬ LÝ NGÔN NGỮ ---
        
        # Kiểm tra ngôn ngữ yêu cầu từ Frontend
        user_lang = request.language 
        print(f"Request Language: {user_lang}") # Debug log

        # CHỈ DỊCH NẾU LÀ TIẾNG VIỆT ('vi')
        if user_lang == 'vi':
            try:
                target_data = analysis_result.get("final_analysis")
                if target_data:
                    # Import ở đầu file hoặc import tại đây nếu chưa có
                    from deep_translator import GoogleTranslator 
                    translator = GoogleTranslator(source='auto', target='vi')
                    
                    fields_to_translate = ["summary", "detailed_analysis", "recommendation"]
                    
                    for field in fields_to_translate:
                        if field in target_data and isinstance(target_data[field], str):
                            translated_text = translator.translate(target_data[field])
                            target_data[field] = translated_text
                            
                    analysis_result["final_analysis"] = target_data
                    print(f"Log {request.id}: Translated to Vietnamese.")
            except Exception as trans_error:
                print(f"Translation Error: {trans_error}")

        # --- 4. LƯU VÀO DB ---
        # Logic an toàn: Ưu tiên lấy 'final_analysis', nếu không có thì lấy toàn bộ
        data_to_save = analysis_result.get("final_analysis") or analysis_result
        
        # Import flag_modified để ép buộc SQLAlchemy nhận biết thay đổi trong cột JSON
        from sqlalchemy.orm.attributes import flag_modified

        if req_id.startswith("event-"):
            record = db.query(models.Anomaly).filter(models.Anomaly.id == int(req_id.split("-")[1])).first()
            if record:
                record.ai_analysis = data_to_save
                flag_modified(record, "ai_analysis") # <--- QUAN TRỌNG: Đánh dấu đã sửa
                db.commit()
                db.refresh(record) # <--- QUAN TRỌNG: Làm tươi dữ liệu
                
        elif req_id.startswith("agg-"):
            record = db.query(models.AggregateAnomaly).filter(models.AggregateAnomaly.id == int(req_id.split("-")[1])).first()
            if record:
                record.ai_analysis = data_to_save
                flag_modified(record, "ai_analysis") # <--- QUAN TRỌNG
                db.commit()
                db.refresh(record)

        return analysis_result
        
    except Exception as e:
        print(f"Error in AI API: {str(e)}")
        # Trả về lỗi giả để UI không bị crash
        return {
            "final_analysis": {
                "summary": "Analysis Process Failed",
                "detailed_analysis": f"System error: {str(e)}",
                "is_anomalous": False,
                "confidence_score": 0.0,
                "recommendation": "Check backend logs.",
                "security_risk_level": "Unknown"
            }
        }

# === CÁC ENDPOINT MỚI ĐỂ ĐIỀU KHIỂN ENGINE (ĐÃ XÓA) ===
# Các endpoint /api/engine/status, /start, /stop đã bị xóa
# vì Engine giờ là một tiến trình riêng biệt.

# === CÁC ENDPOINT ĐỂ QUẢN LÝ CẤU HÌNH (VẪN CẦN THIẾT) ===
@app.get("/api/engine/config", response_model=Dict[str, Any], tags=["Configuration"])
def get_engine_config(current_user: models.User = Depends(auth.get_current_user)):
    """
    Đọc và trả về nội dung hiện tại của file engine_config.json.
    """
    config = load_config()
    if not config:
        raise HTTPException(status_code=404, detail="Configuration file not found or corrupted.")
    return config

@app.put("/api/engine/config", status_code=status.HTTP_202_ACCEPTED, tags=["Configuration"])
def update_engine_config(config_data: Dict[str, Any], current_user: models.User = Depends(auth.get_current_user)):
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
def submit_feedback(feedback: schemas.FeedbackCreate, current_user: models.User = Depends(auth.get_current_user)):
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
        raise HTTPException(status_code=500, detail=f"Unknown error: {str(e)}")
    
# === API MỚI CHO LOG EXPLORER ===
@app.get("/api/logs/", response_model=List[schemas.AllLogs], tags=["Log Explorer"])
def read_all_logs(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db), 
    current_user: models.User = Depends(auth.get_current_user),
    search: str = None, # Thêm bộ lọc
    user: str = None,   # Thêm bộ lọc
    date_from: datetime = None, # Thêm bộ lọc
    date_to: datetime = None,    # Thêm bộ lọc
    behavior_group: str = None
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
    if behavior_group:
        query = query.filter(models.AllLogs.behavior_group == behavior_group)
    if date_from:
        query = query.filter(models.AllLogs.timestamp >= date_from)
    if date_to:
        query = query.filter(models.AllLogs.timestamp <= date_to)
        
    logs = query.order_by(models.AllLogs.timestamp.desc()).offset(skip).limit(limit).all()
    return logs


@app.get("/api/system/audit-logs", response_model=List[schemas.AuditLogEntry], tags=["System"])
def get_active_response_audit_logs(
    limit: int = 100,
    current_user: models.User = Depends(auth.get_current_user)
):
    """
    Đọc file active_response_audit.log và trả về danh sách log đã được parse.
    """
    log_path = Path("active_response_audit.log") # Đường dẫn file log ở root folder
    
    if not log_path.exists():
        return []

    logs = []
    # Regex để tách các trường: Timestamp, Action, Target, Reason
    # Mẫu log: 2025-12-16 22:18:27,486 - ACTION: LOCK_ACCOUNT | TARGET: ... | REASON: ...
    pattern = re.compile(r"^([\d\-\s:,]+) - ACTION:\s(.*?)\s\|\sTARGET:\s(.*?)\s\|\sREASON:\s(.*)$")

    try:
        # Đọc file và lấy N dòng cuối cùng
        with open(log_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        # Đảo ngược để lấy log mới nhất trước
        for line in reversed(lines):
            line = line.strip()
            if not line: continue
            
            match = pattern.match(line)
            if match:
                logs.append(schemas.AuditLogEntry(
                    timestamp=match.group(1).strip(),
                    action=match.group(2).strip(),
                    target=match.group(3).strip(),
                    reason=match.group(4).strip()
                ))
            
            if len(logs) >= limit:
                break
                
    except Exception as e:
        print(f"Error parsing audit log: {e}")
        # Có thể trả về list rỗng hoặc raise HTTP Exception tùy bạn
        
    return logs

