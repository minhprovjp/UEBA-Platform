# engine/config_manager.py
import json
import os

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "engine_config.json")

def load_config():
    """Đọc và trả về nội dung của file engine_config.json."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Nếu file không tồn tại hoặc lỗi, trả về một cấu trúc rỗng để tránh crash
        return {}

def save_config(config_data):
    """Ghi một dictionary Python vào file engine_config.json."""
    try:
        with open(CONFIG_PATH, 'w') as f:
            json.dump(config_data, f, indent=4)
        return True, "Lưu cấu hình thành công."
    except Exception as e:
        return False, f"Lỗi khi lưu cấu hình: {e}"