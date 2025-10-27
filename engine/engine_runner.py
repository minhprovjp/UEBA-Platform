# engine/engine_runner.py
import time
import subprocess
import sys
import os
import pandas as pd
import logging
import threading
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *
from engine.data_processor import load_and_process_data
from engine.config_manager import load_config
from backend_api.models import Base, Anomaly, engine, SessionLocal

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [EngineRunner] - %(message)s')

class AnalysisEngine:
    def __init__(self):
        self._is_running = False
        self._thread = None
        self.status = "stopped"
        self.last_run_finish_time = None
        self.config = load_config()
        
    def _run_all_parsers(self):
        """Chạy tất cả các script parser đã được định nghĩa trong config."""
        logging.info("Bắt đầu chạy các parser...")
        
        # --- CẤU TRÚC MỚI: DỄ DÀNG THÊM PARSER MỚI ---
        parsers_to_run = [
            {
                "name": "MySQL",
                "script": MYSQL_PARSER_SCRIPT_PATH,
                "input": SOURCE_MYSQL_LOG_PATH,
                "output": PARSED_MYSQL_LOG_FILE_PATH
            },
            {
                "name": "PostgreSQL",
                "script": POSTGRES_PARSER_SCRIPT_PATH,
                "input": SOURCE_POSTGRES_LOG_PATH,
                "output": PARSED_POSTGRES_LOG_FILE_PATH
            },
            # {
            #     "name": "MongoDB",
            #     "script": MONGO_PARSER_SCRIPT_PATH,
            #     "input": SOURCE_MONGO_LOG_PATH,
            #     "output": PARSED_MONGO_LOG_FILE_PATH
            # }
        ]
        
        for parser_job in parsers_to_run:
            # Chỉ chạy nếu đường dẫn input tồn tại
            if not os.path.exists(parser_job["input"]):
                logging.warning(f"Bỏ qua parser {parser_job['name']} vì không tìm thấy đường dẫn input: {parser_job['input']}")
                continue
                
            try:
                logging.info(f"Đang thực thi parser cho: {parser_job['name']}")
                command = [sys.executable, parser_job["script"], "--input", parser_job["input"], "--output", parser_job["output"]]
                env = os.environ.copy()
                env["PYTHONIOENCODING"] = "utf-8"
                
                subprocess.run(command, check=True, capture_output=True, text=True, 
                            encoding='utf-8', errors='ignore', env=env)
                            
            except Exception as e:
                logging.error(f"Lỗi khi chạy parser {parser_job['name']}: {e}")
    
    def _run_analysis_cycle(self):
        """Thực hiện một chu kỳ phân tích hoàn chỉnh cho tất cả các nguồn."""
        logging.info("=== Bắt đầu Chu kỳ Phân tích Mới ===")
        
        self.status = "running_parsers"
        self._run_all_parsers()

        # --- Đọc và kết hợp TẤT CẢ các file CSV ---
        all_parsed_logs_dfs = []
        csv_files_to_read = [
            PARSED_MYSQL_LOG_FILE_PATH,
            PARSED_POSTGRES_LOG_FILE_PATH,
            # PARSED_MONGO_LOG_FILE_PATH
        ]
        
        for csv_file in csv_files_to_read:
            if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
                try:
                    all_parsed_logs_dfs.append(pd.read_csv(csv_file))
                except Exception as e:
                    logging.error(f"Không thể đọc file {os.path.basename(csv_file)}: {e}")

        if not all_parsed_logs_dfs:
            logging.warning("Không tìm thấy file CSV nào có dữ liệu để phân tích.")
            return

        df_combined_logs = pd.concat(all_parsed_logs_dfs, ignore_index=True)
        
        # 3. Gọi hàm xử lý dữ liệu chính
        # Chúng ta sẽ truyền các giá trị mặc định từ config.py vào
        # (Trong tương lai, có thể đọc các cấu hình này từ CSDL hoặc file JSON)
        config_params = {
            # Rule 1
            "p_late_night_start_time": LATE_NIGHT_START_TIME_DEFAULT,
            "p_late_night_end_time": LATE_NIGHT_END_TIME_DEFAULT,
            # Rule 2
            "p_known_large_tables": KNOWN_LARGE_TABLES_DEFAULT,
            # Rule 3
            "p_time_window_minutes": TIME_WINDOW_DEFAULT_MINUTES,
            "p_min_distinct_tables": MIN_DISTINCT_TABLES_THRESHOLD_DEFAULT,
            # Rule 4
            "p_sensitive_tables": SENSITIVE_TABLES_DEFAULT,
            "p_allowed_users_sensitive": ALLOWED_USERS_FOR_SENSITIVE_DEFAULT,
            "p_safe_hours_start": SAFE_HOURS_START_DEFAULT,
            "p_safe_hours_end": SAFE_HOURS_END_DEFAULT,
            "p_safe_weekdays": SAFE_WEEKDAYS_DEFAULT,
            # Rule 5
            "p_quantile_start": QUANTILE_START_DEFAULT,
            "p_quantile_end": QUANTILE_END_DEFAULT,
            "p_min_queries_for_profile": MIN_QUERIES_FOR_PROFILE_DEFAULT,
        }
        self.status = "processing_data"
        results = load_and_process_data(df_combined_logs, self.config.get("analysis_params", {}))

        if not results or "all_logs" not in results:
            logging.error("Hàm xử lý dữ liệu không trả về kết quả hợp lệ.")
            return

        # 4. Lưu kết quả vào Cơ sở dữ liệu
        self.status = "saving_to_db"
        db = SessionLocal()
        try:
            logging.info("Đang lưu các bất thường mới vào cơ sở dữ liệu...")
            # Đơn giản nhất là xóa hết và thêm mới. Nâng cấp sau có thể dùng logic update.
            db.query(Anomaly).delete()
            
            # Lặp qua các loại bất thường từ kết quả
            for anomaly_key, df_anomaly in results.items():
                if "anomalies_" in anomaly_key and not df_anomaly.empty:
                    anomaly_type_name = anomaly_key.replace("anomalies_", "")
                    for _, row in df_anomaly.iterrows():
                        new_anomaly_record = Anomaly(
                            timestamp=row.get('timestamp') or row.get('start_time'),
                            user=row.get('user'),
                            client_ip=row.get('client_ip'),
                            database=row.get('database'),
                            query=row.get('query', 'Session-based anomaly, see details.'),
                            anomaly_type=anomaly_type_name,
                            score=row.get('anomaly_score') or row.get('deviation_score'),
                            reason=row.get('violation_reason') or row.get('unusual_activity_reason') or row.get('reasons')
                        )
                        db.add(new_anomaly_record)
            
            db.commit()
            logging.info("Lưu kết quả vào CSDL thành công.")
        except Exception as e:
            logging.error(f"Lỗi khi lưu vào CSDL: {e}")
            db.rollback()
        finally:
            db.close()
            
        self.last_run_finish_time = datetime.now(timezone.utc).isoformat()
        logging.info("Hoàn thành chu kỳ phân tích.")
        
    def _main_loop(self):
        logging.info("Engine main loop started.")
        while self._is_running:
            try:
                self._run_analysis_cycle()
                sleep_interval = self.config.get("engine_sleep_interval_seconds", 60)
                self.status = f"sleeping for {sleep_interval}s"
                time.sleep(sleep_interval)
            except Exception as e:
                logging.error(f"Lỗi nghiêm trọng trong main loop: {e}")
                self.stop() # Dừng lại nếu có lỗi nghiêm trọng
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
    # Phần này chỉ để chạy thử nghiệm độc lập
    logging.info("Khởi tạo Cơ sở dữ liệu...")
    Base.metadata.create_all(bind=engine)
    
    engine_instance = AnalysisEngine()
    engine_instance.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        engine_instance.stop()
        logging.info("Đã dừng Engine từ dòng lệnh.")