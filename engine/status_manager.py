# engine/status_manager.py
import json
import os
from datetime import datetime, timezone

STATUS_FILE_PATH = os.path.join(os.path.dirname(__file__), ".engine_status.json")

def update_status(is_running: bool, status: str, last_run_time: str = None):
    current_status = load_status()
    current_status["is_running"] = is_running
    current_status["status"] = status
    if last_run_time:
        current_status["last_run_finish_time_utc"] = last_run_time
    
    try:
        with open(STATUS_FILE_PATH, 'w') as f:
            json.dump(current_status, f)
    except Exception:
        pass # Bỏ qua nếu không ghi được

def load_status():
    try:
        with open(STATUS_FILE_PATH, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "is_running": False,
            "status": "unknown",
            "last_run_finish_time_utc": None
        }