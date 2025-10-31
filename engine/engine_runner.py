# engine/engine_runner.py (Phiên bản "Máy hút bụi Parquet")
import time
import subprocess
import sys
import os
import pandas as pd
import logging
import threading
import glob
import shutil
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *
from engine.data_processor import load_and_process_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [EngineRunner] - %(message)s')

def _normalize_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    if 'timestamp' not in df.columns:
        return df
    s = df['timestamp']
    try:
        # Nếu là số (epoch ms)
        if pd.api.types.is_numeric_dtype(s):
            df['timestamp'] = pd.to_datetime(s, unit='ms', utc=True, errors='coerce')
        else:
            df['timestamp'] = pd.to_datetime(s, utc=True, errors='coerce')
    except Exception:
        df['timestamp'] = pd.to_datetime(s, utc=True, errors='coerce')
    # loại NaT để tránh nổ downstream
    df = df[df['timestamp'].notna()].reset_index(drop=True)
    return df

class AnalysisEngine:
    def __init__(self):
        self._is_running = False
        self._thread = None
        self.status = "stopped"
        self.last_run_finish_time = None

    def _run_all_parsers_catch_up(self):
        logging.info("Bắt đầu chạy các parser ở chế độ catch-up...")
        parsers_to_run = [
            {"name": "MySQL", "script": MYSQL_PARSER_SCRIPT_PATH},
        ]
        for parser_job in parsers_to_run:
            try:
                logging.info(f"Đang thực thi parser cho: {parser_job['name']}")
                command = [sys.executable, parser_job["script"], "--mode", "catch-up"]
                env = os.environ.copy()
                env["PYTHONIOENCODING"] = "utf-8"
                cp = subprocess.run(command, check=True, capture_output=True, text=True, errors='ignore', env=env)
                if cp.stdout:
                    logging.info(f"[{parser_job['name']} stdout]\n{cp.stdout.strip()}")
                if cp.stderr:
                    logging.warning(f"[{parser_job['name']} stderr]\n{cp.stderr.strip()}")
            except subprocess.CalledProcessError as e:
                stderr = e.stderr if hasattr(e, 'stderr') else str(e)
                logging.error(f"Lỗi khi chạy parser {parser_job['name']}: {stderr}")

    def _run_analysis_cycle(self):
        logging.info("=== Bắt đầu Chu kỳ Phân tích Mới ===")
        # 1) gọi parser
        self.status = "running_parsers"
        self._run_all_parsers_catch_up()

        # 2) gom file staging
        self.status = "reading_staging_data"
        parquet_files = glob.glob(os.path.join(STAGING_DATA_DIR, "*.parquet"))
        csv_files = glob.glob(os.path.join(STAGING_DATA_DIR, "*.csv"))  # fallback
        files = sorted(parquet_files + csv_files)
        if not files:
            logging.info("Không có file dữ liệu mới trong staging để xử lý.")
            self.last_run_finish_time = datetime.now(timezone.utc).isoformat()
            return

        logging.info(f"Tìm thấy {len(files)} file dữ liệu mới đang chờ xử lý.")
        dfs, files_to_archive = [], []

        for f_path in files:
            try:
                if f_path.lower().endswith(".parquet"):
                    df = pd.read_parquet(f_path)
                else:
                    df = pd.read_csv(f_path, encoding='utf-8', on_bad_lines='skip')
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
        logging.info(f"Tổng hợp được {len(df_combined_logs)} dòng log để phân tích.")

        # 3) chạy phân tích
        config_params = {}
        self.status = "processing_data"
        results = load_and_process_data(df_combined_logs, config_params)

        if not results or "all_logs" not in results:
            logging.error("Hàm xử lý dữ liệu không trả về kết quả hợp lệ.")
            return

        # 4) TODO: lưu kết quả anomalies

        # 5) dọn staging -> archive
        logging.info("Đang dọn dẹp thư mục staging...")
        os.makedirs(ARCHIVE_DATA_DIR, exist_ok=True)
        for f_path in files_to_archive:
            try:
                shutil.move(f_path, os.path.join(ARCHIVE_DATA_DIR, os.path.basename(f_path)))
            except Exception as e:
                logging.error(f"Không thể di chuyển file {os.path.basename(f_path)}: {e}")

        logging.info("Hoàn thành chu kỳ phân tích.")
        self.last_run_finish_time = datetime.now(timezone.utc).isoformat()

    def _main_loop(self):
        logging.info("Engine main loop started (Polling mode).")
        while self._is_running:
            try:
                self._run_analysis_cycle()
                logging.info(f"Engine sleeping for {ENGINE_SLEEP_INTERVAL_SECONDS} seconds...")
                time.sleep(ENGINE_SLEEP_INTERVAL_SECONDS)
            except Exception as e:
                logging.error(f"Lỗi nghiêm trọng trong main loop: {e}", exc_info=True)
                self.stop()
        self.status = "stopped"
        logging.info("Engine main loop has stopped.")

    def start(self):
        if self._is_running:
            logging.warning("Engine đã đang chạy.")
            return
        self._is_running = True
        self._thread = threading.Thread(target=self._main_loop, daemon=True)
        self._thread.start()
        logging.info("Engine đã được khởi động.")

    def stop(self):
        if not self._is_running:
            logging.warning("Engine chưa được khởi động.")
            return
        self._is_running = False
        logging.info("Đang gửi tín hiệu dừng đến Engine...")

    def get_status(self):
        return {
            "is_running": self._is_running,
            "status": self.status,
            "last_run_finish_time_utc": self.last_run_finish_time
        }

if __name__ == "__main__":
    engine_instance = AnalysisEngine()
    engine_instance.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        engine_instance.stop()
        logging.info("Đã dừng Engine từ dòng lệnh.")
