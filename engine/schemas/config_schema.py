# Nội dung cho file engine/schemas/config_schema.py

from pydantic import BaseModel, Field
from typing import List, Union

# --- Định nghĩa các schema con để code có cấu trúc hơn ---

class AnalysisParams(BaseModel):
    p_late_night_start_time: str = Field(..., example="22:00:00", description="Thời gian bắt đầu khung giờ khuya (HH:MM:SS)")
    p_late_night_end_time: str = Field(..., example="05:00:00", description="Thời gian kết thúc khung giờ khuya (HH:MM:SS)")
    p_known_large_tables: List[str] = []
    p_time_window_minutes: int = Field(..., gt=0, description="Cửa sổ thời gian (phút) để xét truy cập nhiều bảng") # gt=0: phải lớn hơn 0
    p_min_distinct_tables: int = Field(..., gt=1, description="Số bảng riêng biệt tối thiểu để coi là bất thường") # gt=1: phải lớn hơn 1
    p_sensitive_tables: List[str] = []
    p_allowed_users_sensitive: List[str] = []
    p_safe_hours_start: int = Field(..., ge=0, le=23, description="Giờ bắt đầu làm việc an toàn (0-23)") # ge=0: >=0, le=23: <=23
    p_safe_hours_end: int = Field(..., ge=0, le=23, description="Giờ kết thúc làm việc an toàn (0-23)")
    p_safe_weekdays: List[int] = []
    p_quantile_start: float = Field(..., ge=0.0, lt=0.5, description="Phân vị bắt đầu cho profile user (0.0 < x < 0.5)")
    p_quantile_end: float = Field(..., gt=0.5, le=1.0, description="Phân vị kết thúc cho profile user (0.5 < x <= 1.0)")
    p_min_queries_for_profile: int = Field(..., ge=5, description="Số query tối thiểu để xây dựng profile")

class LLMConfig(BaseModel):
    enable_ollama: bool = True
    ollama_host: str = "http://localhost:11434"
    ollama_timeout: int = 3600
    enable_openai: bool = False
    openai_api_key: str = ""
    enable_anthropic: bool = False
    anthropic_api_key: str = ""
    # Thêm các cấu hình LLM khác nếu có

# --- Định nghĩa schema chính cho toàn bộ file config ---

class EngineConfig(BaseModel):
    engine_sleep_interval_seconds: int = Field(default=60, gt=0, description="Số giây engine nghỉ giữa các chu kỳ phân tích")
    analysis_params: AnalysisParams
    llm_config: LLMConfig

    class Config:
        # Ví dụ về dữ liệu mẫu để hiển thị trên Swagger UI cho dễ kiểm thử
        json_schema_extra = {
            "example": {
                "engine_sleep_interval_seconds": 300,
                "analysis_params": {
                    "p_late_night_start_time": "23:00:00",
                    "p_late_night_end_time": "06:00:00",
                    "p_known_large_tables": ["orders", "logs"],
                    "p_time_window_minutes": 10,
                    "p_min_distinct_tables": 5,
                    "p_sensitive_tables": ["customers", "payments"],
                    "p_allowed_users_sensitive": ["db_admin"],
                    "p_safe_hours_start": 9,
                    "p_safe_hours_end": 17,
                    "p_safe_weekdays": [0, 1, 2, 3, 4],
                    "p_quantile_start": 0.15,
                    "p_quantile_end": 0.85,
                    "p_min_queries_for_profile": 20
                },
                "llm_config": {
                    "enable_ollama": True,
                    "ollama_host": "http://172.16.0.221:11434",
                    "ollama_timeout": 3600,
                    "enable_openai": False,
                    "openai_api_key": "YOUR_API_KEY_HERE",
                    "enable_anthropic": False,
                    "anthropic_api_key": "YOUR_API_KEY_HERE"
                }
            }
        }