#backend_api/routers/logs.py

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date, timedelta

# Dùng relative import để import các module trong cùng package backend_api
from .. import models, schemas
from ..models import SessionLocal

# Khởi tạo router
router = APIRouter(
    prefix="/api/logs",
    tags=["Logs Explorer"]
)

# Dependency để lấy DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Định nghĩa API endpoint
@router.get("/search", response_model=schemas.LogPage)
def search_logs(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    users: Optional[str] = Query(None, description="Danh sách user, phân tách bằng dấu phẩy. Ví dụ: root,admin"),
    client_ips: Optional[str] = Query(None, description="Danh sách IP, phân tách bằng dấu phẩy."),
    databases: Optional[str] = Query(None, description="Danh sách database, phân tách bằng dấu phẩy."),
    query_contains: Optional[str] = Query(None, description="Tìm kiếm một chuỗi trong câu lệnh SQL."),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None)
):
    """
    Tìm kiếm và phân trang trong các bản ghi log đã được phân tích.
    """
    query = db.query(models.ParsedLog)

    # Áp dụng các bộ lọc động
    if users:
        query = query.filter(models.ParsedLog.user.in_(users.split(',')))
    if client_ips:
        query = query.filter(models.ParsedLog.client_ip.in_(client_ips.split(',')))
    if databases:
        query = query.filter(models.ParsedLog.database.in_(databases.split(',')))
    if query_contains:
        query = query.filter(models.ParsedLog.query.ilike(f"%{query_contains}%"))
    if start_date:
        query = query.filter(models.ParsedLog.timestamp >= start_date)
    if end_date:
        query = query.filter(models.ParsedLog.timestamp < (end_date + timedelta(days=1)))

    # Lấy tổng số lượng trước khi phân trang
    total_items = query.count()
    
    # Áp dụng sắp xếp, phân trang và lấy kết quả
    logs = query.order_by(models.ParsedLog.timestamp.desc()).offset(skip).limit(limit).all()

    return {"total_items": total_items, "items": logs}