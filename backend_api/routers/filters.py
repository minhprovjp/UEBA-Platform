# Nội dung cho file backend_api/routers/filters.py

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

# Dùng relative import để import các module trong cùng package backend_api
from .. import models
from ..models import SessionLocal

# Khởi tạo router
router = APIRouter(
    prefix="/api/filters",
    tags=["Filters"]
)

# Dependency để lấy DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Tạo một dictionary để ánh xạ tên field từ API request
# đến cột tương ứng trong SQLAlchemy model.
# Điều này giúp code an toàn hơn, tránh SQL Injection
# và chỉ cho phép lọc trên các cột được chỉ định.
ALLOWED_FIELDS_TO_QUERY = {
    "user": models.ParsedLog.user,
    "client_ip": models.ParsedLog.client_ip,
    "database": models.ParsedLog.database,
    "anomaly_type": models.Anomaly.anomaly_type
    # Bạn có thể thêm các trường khác ở đây trong tương lai
}

@router.get("/available-values", response_model=List[str])
def get_available_filter_values(field: str, db: Session = Depends(get_db)):
    """
    Lấy danh sách các giá trị duy nhất cho một trường được chỉ định
    để sử dụng trong các bộ lọc của frontend.
    
    Các giá trị hợp lệ cho `field`: `user`, `client_ip`, `database`, `anomaly_type`.
    """
    # 1. Kiểm tra xem field được yêu cầu có hợp lệ không
    if field not in ALLOWED_FIELDS_TO_QUERY:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Filtering by field '{field}' is not allowed. "
                   f"Allowed fields are: {list(ALLOWED_FIELDS_TO_QUERY.keys())}"
        )

    # 2. Lấy ra đối tượng cột SQLAlchemy từ dictionary
    column_to_query = ALLOWED_FIELDS_TO_QUERY[field]

    # 3. Thực hiện truy vấn DISTINCT để lấy các giá trị duy nhất
    try:
        results = db.query(column_to_query).distinct().order_by(column_to_query).all()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {e}"
        )

    # 4. Trích xuất kết quả và trả về
    # results sẽ là một list các tuple, ví dụ: [('root',), ('admin',)].
    # Chúng ta cần trích xuất phần tử đầu tiên của mỗi tuple.
    return [result[0] for result in results if result[0] is not None]