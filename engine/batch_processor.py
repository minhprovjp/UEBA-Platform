# engine/batch_processor.py
import time
import sys
import os
import pandas as pd
import logging
import glob
import shutil
from datetime import datetime, timezone
import json
from config_manager import load_config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import *
    from data_processor import load_and_process_data
    from db_writer import save_results_to_db
except ImportError:
    print("Lỗi: Không thể import 'config', 'data_processor' hoặc 'db_writer'.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [BatchProcessor] - %(message)s', force=True)

def _normalize_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    if 'timestamp' not in df.columns:
        return df
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    except Exception:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    df = df[df['timestamp'].notna()].reset_index(drop=True)
    return df

def run_analysis_cycle(CFG: dict):
    """
    Quét thư mục staging, xử lý file, lưu kết quả, và dọn dẹp.
    """
    logging.info("=== Bắt đầu Chu kỳ Xử lý Batch (Staging) ===")
    
    # 1. Gom file staging (chỉ parquet)
    parquet_files = glob.glob(os.path.join(STAGING_DATA_DIR, "*.parquet"))
    if not parquet_files:
        logging.info("Không có file dữ liệu mới trong staging để xử lý.")
        return

    logging.info(f"Tìm thấy {len(parquet_files)} file parquet mới đang chờ xử lý.")
    dfs, files_to_archive = [], []

    for f_path in parquet_files:
        try:
            df = pd.read_parquet(f_path)
            df = _normalize_timestamp(df)
            if not df.empty:
                dfs.append(df)
            files_to_archive.append(f_path)
        except Exception as e:
            logging.error(f"Lỗi khi đọc file {os.path.basename(f_path)}: {e}")

    if not dfs:
        logging.warning("Các file staging đều rỗng hoặc lỗi. Không có dữ liệu để phân tích.")
        return

    df_combined_logs = pd.concat(dfs, ignore_index=True)
    logging.info(f"Tổng hợp được {len(df_combined_logs)} dòng log (ví dụ: từ PostgreSQL) để phân tích.")

    # 2. Chạy phân tích
    results = load_and_process_data(df_combined_logs, CFG)

    # 3. Lưu bất thường vào CSDL
    try:
        save_results_to_db(results)
    except Exception as e:
        logging.error(f"Failed to save batch anomalies to DB: {e}", exc_info=True)

    # 4. Dọn staging -> archive
    logging.info("Đang dọn dẹp thư mục staging...")
    os.makedirs(ARCHIVE_DATA_DIR, exist_ok=True)
    for f_path in files_to_archive:
        try:
            shutil.move(f_path, os.path.join(ARCHIVE_DATA_DIR, os.path.basename(f_path)))
        except Exception as e:
            logging.error(f"Không thể di chuyển file {os.path.basename(f_path)}: {e}")

    logging.info("Hoàn thành chu kỳ xử lý batch.")

def main_loop():
    logging.info("Batch Processor (Staging) started.")
    logging.info("Processor đang chạy... Nhấn Ctrl+C để dừng.")
    
    full_config = load_config()
    CFG = full_config.get("analysis_params", {})
    if not CFG:
        logging.error("KHÔNG TÌM THẤY 'analysis_params' trong engine_config.json. Sử dụng config rỗng.")
    
    while True:
        try:
            run_analysis_cycle(CFG)
            logging.info(f"Batch processor đang ngủ {ENGINE_SLEEP_INTERVAL_SECONDS} giây...")
            time.sleep(ENGINE_SLEEP_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logging.info("Đã nhận tín hiệu (Ctrl+C). Batch Processor đang dừng...")
            break
        except Exception as e:
            logging.error(f"Lỗi nghiêm trọng trong main loop (batch): {e}", exc_info=True)
            if "config" in str(e).lower():
                CFG = load_rules_config()
            time.sleep(30) # Chờ lâu hơn nếu có lỗi
    
    logging.info("Batch Processor đã dừng.")

if __name__ == "__main__":
    main_loop()