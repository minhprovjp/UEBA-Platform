# engine/realtime_engine.py
import os, json, logging, sys
import time
import pandas as pd
from redis import Redis, ResponseError
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.config_manager import load_config
from engine.data_processor import load_and_process_data
from engine.db_writer import save_results_to_db
from email_alert import send_email_alert
from config import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [RealtimeEngine] - %(message)s")


    
# --- HÀM KẾT NỐI REDIS TIN CẬY ---
def connect_redis():
    """Kết nối đến Redis với cơ chế thử lại vô hạn."""
    while True:
        try:
            r = Redis.from_url(REDIS_URL, decode_responses=True)
            r.ping()
            logging.info("Kết nối Redis (Engine) thành công.")
            return r
        except ConnectionError as e:
            logging.error(f"Kết nối Redis (Engine) thất bại: {e}. Thử lại sau 5 giây...")
            time.sleep(5)

def ensure_group(r: Redis, stream: str, group: str):
    """Đảm bảo Consumer Group tồn tại"""
    try:
        r.xgroup_create(stream, group, id="0-0", mkstream=True)
        logging.info(f"Created consumer group {group} on {stream}")
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info(f"Consumer group {group} already exists on {stream}.")
        else:
            raise

def start_engine():
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    for stream in STREAMS.values():
        try:
            r.xgroup_create(stream, REDIS_GROUP_ENGINE, id="0", mkstream=True)
        except:
            pass

    logging.info("Realtime UBA Engine STARTED — Monitoring MySQL Performance Schema")

    while True:
        try:
            msgs = r.xreadgroup(
                groupname=REDIS_GROUP_ENGINE,
                consumername=REDIS_CONSUMER_NAME,
                streams=STREAMS,
                count=100,
                block=5000
            )

            if not msgs:
                continue

            records = []
            ack_ids = []

            for stream, entries in msgs:
                for msg_id, fields in entries:
                    data = fields.get("data")
                    if data:
                        records.append(json.loads(data))
                        ack_ids.append((stream, msg_id))

            if records:
                df = pd.DataFrame(records)
                results = load_and_process_data(df, {})

                # Save to DB
                save_results_to_db(results)

                # ACK messages
                for stream, msg_id in ack_ids:
                    r.xack(stream, REDIS_GROUP_ENGINE, msg_id)

        except KeyboardInterrupt:
            logging.info("Engine stopped by user")
            break
        except Exception as e:
            logging.error(f"Engine error: {e}", exc_info=True)

if __name__ == "__main__":
    start_engine()