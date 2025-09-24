# Nội dung cuối cùng cho file engine/engine_runner.py

import time
import subprocess
import sys
import os
import pandas as pd
import logging
import threading
from datetime import datetime, timezone

# --- Thêm đường dẫn thư mục gốc vào sys.path ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# --- Sử dụng absolute imports ---
from core.config import settings
from engine.data_processor import load_and_process_data
from engine.config_manager import load_config
from backend_api.models import Base, Anomaly, ParsedLog, engine, SessionLocal
from engine.status_manager import update_status

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [EngineRunner] - %(message)s')

class AnalysisEngine:
    def __init__(self):
        self._is_running = False
        self._thread = None
        self.last_run_finish_time = None
        # Tải cấu hình động từ file JSON khi khởi tạo
        self.config = load_config()
        logging.info("AnalysisEngine initialized.")

    def _run_all_parsers(self):
        """Chạy tất cả các script parser đã được định nghĩa trong settings."""
        logging.info("Starting parser execution...")
        
        parsers_to_run = [
            {
                "name": "MySQL",
                "script": os.path.join(PROJECT_ROOT, "engine", "mysql_log_parser.py"),
                "input": settings.SOURCE_MYSQL_LOG_PATH,
                "output": os.path.join(PROJECT_ROOT, "logs", "parsed_mysql_logs.csv")
            },
            # Bạn có thể bỏ comment các parser khác khi cần
            # {
            #     "name": "PostgreSQL",
            #     "script": os.path.join(PROJECT_ROOT, "engine", "postgres_log_parser.py"),
            #     "input": settings.SOURCE_POSTGRES_LOG_PATH,
            #     "output": os.path.join(PROJECT_ROOT, "logs", "parsed_postgres_logs.csv")
            # },
        ]
        
        for parser_job in parsers_to_run:
            if not os.path.exists(parser_job["input"]):
                logging.warning(f"Skipping {parser_job['name']} parser, input path not found: {parser_job['input']}")
                continue
                
            try:
                logging.info(f"Executing parser for: {parser_job['name']}")
                command = [sys.executable, parser_job["script"], "--input", parser_job["input"], "--output", parser_job["output"]]
                subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8', errors='ignore')
            except subprocess.CalledProcessError as e:
                logging.error(f"Error running {parser_job['name']} parser. Stderr: {e.stderr}")
            except Exception as e:
                logging.error(f"An unexpected error occurred while running {parser_job['name']} parser: {e}")

    def _run_analysis_cycle(self):
        """Thực hiện một chu kỳ phân tích hoàn chỉnh."""
        logging.info("=== Starting new analysis cycle ===")
        
        # 1. Chạy parsers
        update_status(True, "running_parsers")
        self._run_all_parsers()

        # 2. Đọc và kết hợp các file CSV
        update_status(True, "aggregating_data")
        all_parsed_logs_dfs = []
        csv_files = [
            os.path.join(PROJECT_ROOT, "logs", "parsed_mysql_logs.csv"),
            # os.path.join(PROJECT_ROOT, "logs", "parsed_postgres_logs.csv")
        ]
        
        for csv_file in csv_files:
            if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
                try:
                    df = pd.read_csv(csv_file)
                    if 'mysql' in os.path.basename(csv_file):
                        df['source_type'] = 'mysql'
                    elif 'postgres' in os.path.basename(csv_file):
                        df['source_type'] = 'postgres'
                    all_parsed_logs_dfs.append(df)
                except Exception as e:
                    logging.error(f"Could not read {os.path.basename(csv_file)}: {e}")

        if not all_parsed_logs_dfs:
            logging.warning("No parsed log data found to analyze.")
            self.last_run_finish_time = datetime.now(timezone.utc).isoformat()
            return

        df_combined_logs = pd.concat(all_parsed_logs_dfs, ignore_index=True)
        
        # 3. Chạy phân tích
        update_status(True, "processing_data")
        # Sử dụng cấu hình động đã được load
        results = load_and_process_data(df_combined_logs, self.config.get("analysis_params", {}))

        if not results:
            logging.error("Data processing function returned no results.")
            self.last_run_finish_time = datetime.now(timezone.utc).isoformat()
            return

        # 4. Lưu kết quả vào DB
        update_status(True, "saving_to_db")
        db = SessionLocal()
        try:
            # Lưu Parsed Logs
            db.query(ParsedLog).delete()
            logs_to_insert = df_combined_logs.to_dict(orient='records')
            if logs_to_insert:
                db.bulk_insert_mappings(ParsedLog, logs_to_insert)
            
            # Lưu Anomalies
            db.query(Anomaly).delete()
            for anomaly_key, df_anomaly in results.items():
                if "anomalies_" in anomaly_key and not df_anomaly.empty:
                    anomaly_type_name = anomaly_key.replace("anomalies_", "")
                    for _, row in df_anomaly.iterrows():
                        ts = row.get('timestamp') or row.get('start_time')
                        if isinstance(ts, str):
                            ts = pd.to_datetime(ts)

                        # Lấy reason một cách an toàn
                        reason_text = row.get('violation_reason') or row.get('unusual_activity_reason')
    
                        # Lấy score một cách an toàn, đảm bảo nó là số hoặc None
                        score_val = row.get('anomaly_score')
                        if not pd.api.types.is_number(score_val):
                            score_val = None

                        new_anomaly_record = Anomaly(
                            timestamp=ts,
                            user=str(row.get('user', 'unknown')),
                            client_ip=str(row.get('client_ip', 'unknown')),
                            database=str(row.get('database', 'unknown')),
                            query=str(row.get('query', 'Session-based anomaly')),
                            anomaly_type=str(anomaly_type_name),
                            score=score_val,
                            reason=str(reason_text) if pd.notna(reason_text) else None,
                            status='new'
                        )
                        db.add(new_anomaly_record)    
            db.commit()
            logging.info("Successfully saved results to the database.")
        except Exception as e:
            logging.error(f"Error saving to database: {e}", exc_info=True)
            db.rollback()
        finally:
            db.close()
            
        self.last_run_finish_time = datetime.now(timezone.utc).isoformat()
        logging.info("=== Analysis cycle finished ===")

    def _main_loop(self):
        """Hàm chứa vòng lặp chính, liên tục chạy chu kỳ phân tích."""
        logging.info("Engine main loop started.")
        while self._is_running:
            try:
                self._run_analysis_cycle()
                
                self.config = load_config()
                sleep_interval = self.config.get("engine_sleep_interval_seconds", 60)
                
                update_status(True, f"sleeping for {sleep_interval}s", self.last_run_finish_time)
                time.sleep(sleep_interval)
            except Exception as e:
                logging.error(f"Critical error in main loop: {e}", exc_info=True)
                update_status(False, f"error: {e}", self.last_run_finish_time)
                self.stop()
        
        update_status(False, "stopped", self.last_run_finish_time)
        logging.info("Engine main loop has stopped.")

    def start(self):
        if self._is_running:
            logging.warning("Engine is already running.")
            return
        self._is_running = True
        update_status(True, "starting")
        self._thread = threading.Thread(target=self._main_loop, daemon=True)
        self._thread.start()
        logging.info("Engine has been started.")

    def stop(self):
        if not self._is_running:
            logging.warning("Engine is not running.")
            return
        self._is_running = False
        update_status(False, "stopping", self.last_run_finish_time)
        logging.info("Stop signal sent to Engine...")

if __name__ == "__main__":
    logging.info("Initializing Database (if needed)...")
    Base.metadata.create_all(bind=engine)
    
    engine_instance = AnalysisEngine()
    engine_instance.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        engine_instance.stop()
        logging.info("Engine stopped from command line.")