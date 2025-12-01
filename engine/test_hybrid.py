# engine/perf_log_publisher.py
import os, json, logging, sys, time, signal
import pandas as pd
from redis import Redis
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, timezone
import math
from collections import Counter

# Setup path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import *
    from engine.utils import save_logs_to_parquet 
except ImportError:
    print("Lỗi: Không thể import config/utils.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [HybridPublisher] - %(message)s")

STATE_FILE = os.path.join(LOGS_DIR, ".mysql_hybrid_state.json")
STREAM_KEY = f"{REDIS_STREAM_LOGS}:mysql"
is_running = True

def handle_shutdown(signum, frame):
    global is_running
    logging.info("🛑 Stopping publisher...")
    is_running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# === 1. State Management ===
def load_state():
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except:
        return {"last_event_id": 0, "boot_signature": "", "last_event_ts": "2024-01-01T00:00:00"}

def save_state(event_id, boot_sig, event_ts):
    state = {"last_event_id": event_id, "boot_signature": boot_sig, "last_event_ts": event_ts}
    try:
        with open(STATE_FILE, 'w') as f: json.dump(state, f)
    except Exception as e: logging.error(f"Save state failed: {e}")

# === 2. Connection ===
def connect_db():
    try:
        # [QUAN TRỌNG] Kết nối vào uba_db để đọc bảng persistent
        url = MYSQL_LOG_DATABASE_URL.replace("/mysql", "/uba_db") if "/mysql" in MYSQL_LOG_DATABASE_URL else MYSQL_LOG_DATABASE_URL
        engine = create_engine(url)
        with engine.connect() as conn: conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        logging.error(f"DB Connect failed: {e}")
        return None

def connect_redis():
    try:
        return Redis.from_url(REDIS_URL, decode_responses=True)
    except: return None

# === 3. Helpers ===
def calculate_entropy(text):
    if not text: return 0.0
    counter = Counter(text)
    length = len(text)
    return -sum((count/length) * math.log2(count/length) for count in counter.values())

def process_and_push(rows, redis_client, source_type="RAM"):
    new_records = []
    max_id = 0
    max_ts = ""

    for row in rows:
        r = row._mapping
        
        # Chuẩn hóa ID và TS
        if source_type == "RAM":
            current_id = int(r['e_EVENT_ID']) # Alias từ query RAM
            ts_iso = r['timestamp_calculated'] # Inject từ loop
        else:
            current_id = 0 
            ts_val = r['event_ts']
            ts_iso = ts_val.isoformat() if isinstance(ts_val, datetime) else str(ts_val)

        # Tracking
        if source_type == "RAM" and current_id > max_id: max_id = current_id
        if ts_iso > max_ts: max_ts = ts_iso

        # Helper lấy value an toàn
        def g(k, default=0):
            return r.get(k) if r.get(k) is not None else default

        # SQL Text
        sql_txt = str(g('SQL_TEXT') or g('sql_text') or '')
        if "uba_persistent_log" in sql_txt: continue

        # Metrics
        rows_sent = int(g('ROWS_SENT') or g('rows_sent') or 0)
        rows_exam = int(g('ROWS_EXAMINED') or g('rows_examined') or 0)
        scan_eff = rows_sent / (rows_exam + 1)
        
        # Time metrics (RAM: pico -> ms, Disk: đã là pico -> ms)
        # Chú ý: Bảng persistent lưu pico (bigint), cần chia.
        # RAM query lấy TIMER_WAIT (pico), cần chia.
        exec_ms = float(g('TIMER_WAIT') or g('timer_wait') or 0) / 1000000.0
        lock_ms = float(g('LOCK_TIME') or g('lock_time') or 0) / 1000000.0
        
        # Flags
        db_name = str(g('CURRENT_SCHEMA') or g('current_schema') or 'unknown')
        sql_up = sql_txt.upper()
        
        record = {
            "timestamp": ts_iso,
            "event_id": int(g('e_EVENT_ID') or g('event_id') or 0),
            "user": str(g('PROCESSLIST_USER') or g('processlist_user') or 'unknown'),
            "client_ip": str(g('PROCESSLIST_HOST') or g('processlist_host') or 'unknown').split(':')[0],
            "database": db_name,
            "query": sql_txt,
            
            # Features
            "query_length": len(sql_txt),
            "query_entropy": float(f"{calculate_entropy(sql_txt):.4f}"),
            "scan_efficiency": float(f"{scan_eff:.6f}"),
            "is_system_table": 1 if db_name.lower() in ['mysql','sys','information_schema','performance_schema'] else 0,
            "is_admin_command": 1 if any(k in sql_up for k in ['GRANT','REVOKE','CREATE USER']) else 0,
            "is_risky_command": 1 if any(k in sql_up for k in ['DROP','TRUNCATE']) else 0,
            "has_comment": 1 if ('--' in sql_txt or '/*' in sql_txt) else 0,
            
            # Metrics
            "execution_time_ms": exec_ms,
            "lock_time_ms": lock_ms,
            "rows_returned": rows_sent,
            "rows_examined": rows_exam,
            "rows_affected": int(g('ROWS_AFFECTED') or g('rows_affected') or 0),
            
            # Error
            "error_code": int(g('MYSQL_ERRNO') or g('mysql_errno') or 0),
            "error_count": int(g('ERRORS') or g('errors') or 0),
            "has_error": 1 if (int(g('ERRORS') or g('errors') or 0) > 0) else 0,
            
            # Optimizer
            "created_tmp_disk_tables": int(g('CREATED_TMP_DISK_TABLES') or g('created_tmp_disk_tables') or 0),
            "select_full_join": int(g('SELECT_FULL_JOIN') or g('select_full_join') or 0),
            "select_scan": int(g('SELECT_SCAN') or g('select_scan') or 0),
            "no_index_used": int(g('NO_INDEX_USED') or g('no_index_used') or 0),
            
            "source_dbms": "MySQL"
        }
        new_records.append(record)

    if new_records:
        pipe = redis_client.pipeline()
        for rec in new_records:
            pipe.xadd(STREAM_KEY, {"data": json.dumps(rec, default=str)})
        pipe.execute()
        save_logs_to_parquet(new_records, source_dbms="MySQL")
    
    return len(new_records), max_id, max_ts

# === 4. Main Logic ===
def monitor_hybrid():
    global is_running
    engine = connect_db()
    redis = connect_redis()
    if not engine or not redis: return

    state = load_state()
    last_id = state['last_event_id']
    last_ts = state['last_event_ts']
    saved_boot = state['boot_signature']

    logging.info(f"🚀 Hybrid Publisher Started. RAM ID: {last_id}, Disk TS: {last_ts}")

    # Query RAM (Lấy trực tiếp từ performance_schema)
    # [FIX] Explicit column names to match g() helper
    sql_ram = text("""
        SELECT 
            e.EVENT_ID AS e_EVENT_ID, 
            e.TIMER_START, e.TIMER_WAIT, e.LOCK_TIME,
            e.SQL_TEXT, e.DIGEST, e.CURRENT_SCHEMA, e.EVENT_NAME,
            e.ROWS_SENT, e.ROWS_EXAMINED, e.ROWS_AFFECTED,
            e.MYSQL_ERRNO, e.MESSAGE_TEXT, e.ERRORS, e.WARNINGS,
            e.CREATED_TMP_DISK_TABLES, e.SELECT_FULL_JOIN, e.SELECT_SCAN, e.NO_INDEX_USED,
            t.PROCESSLIST_USER, t.PROCESSLIST_HOST
        FROM performance_schema.events_statements_history_long e
        LEFT JOIN performance_schema.threads t ON e.THREAD_ID = t.THREAD_ID
        WHERE e.EVENT_ID > :lid 
          AND e.SQL_TEXT IS NOT NULL 
          AND (t.PROCESSLIST_USER IS NULL OR t.PROCESSLIST_USER NOT IN ('event_scheduler', 'uba_user', 'boot'))
          AND (e.CURRENT_SCHEMA IS NULL OR e.CURRENT_SCHEMA != 'uba_db')
          AND e.SQL_TEXT NOT LIKE '%uba_persistent_log%'
        ORDER BY e.EVENT_ID ASC LIMIT 5000
    """)

    # Query Disk (Recovery)
    # [FIX] Lấy các cột tương ứng từ bảng vật lý
    sql_disk = text("""
        SELECT * FROM uba_db.uba_persistent_log
        WHERE event_ts > :lts
          AND processlist_user NOT IN ('event_scheduler', 'uba_user', 'boot')
          AND (current_schema IS NULL OR current_schema != 'uba_db')
          AND sql_text NOT LIKE '%uba_persistent_log%'
        ORDER BY event_ts ASC LIMIT 5000
    """)
    
    sql_uptime = text("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='UPTIME'")
    sql_bounds = text("SELECT MIN(EVENT_ID), MAX(EVENT_ID) FROM performance_schema.events_statements_history_long")

    while is_running:
        try:
            with engine.connect() as conn:
                # 1. Boot Check
                uptime = float(conn.execute(sql_uptime).scalar() or 0)
                boot_dt = datetime.now(timezone.utc) - timedelta(seconds=uptime)
                curr_boot = boot_dt.strftime("%Y-%m-%d %H:%M")

                # 2. Recovery Check
                min_id_ram, max_id_ram = conn.execute(sql_bounds).fetchone()
                if min_id_ram is None: min_id_ram, max_id_ram = 0, 0
                
                need_recovery = False
                
                # Case 1: DB Restarted
                if saved_boot and curr_boot != saved_boot:
                    logging.warning(f"⚠️ DB Restart! Boot changed: {saved_boot} -> {curr_boot}")
                    need_recovery = True
                
                # Case 2: RAM Wrap-around (ID trôi mất)
                elif last_id < min_id_ram and max_id_ram > 0:
                    logging.warning(f"⚠️ RAM Wrap: Saved {last_id} < Min {min_id_ram}")
                    need_recovery = True

                # --- RECOVERY MODE ---
                if need_recovery:
                    logging.info(f"🔄 RECOVERY MODE (From TS: {last_ts})")
                    while is_running:
                        rows = conn.execute(sql_disk, {"lts": last_ts}).fetchall()
                        if not rows:
                            logging.info("✅ Catch-up done. Back to RAM.")
                            # Reset state RAM về mới nhất
                            last_id = max_id_ram
                            saved_boot = curr_boot
                            save_state(last_id, saved_boot, last_ts)
                            break
                        
                        cnt, _, max_ts_batch = process_and_push(rows, redis, "DISK")
                        
                        # Update timestamp con trỏ
                        if max_ts_batch > last_ts: last_ts = max_ts_batch
                        logging.info(f"📥 Recovered {cnt} logs.")
                        save_state(last_id, saved_boot, last_ts)
                        time.sleep(0.1)
                    continue

                # --- REALTIME MODE ---
                rows = conn.execute(sql_ram, {"lid": last_id}).fetchall()
                
                if rows:
                    # Wrapper object để inject timestamp
                    mutable_rows = []
                    for r in rows:
                        d = dict(r._mapping)
                        t_start = float(d['TIMER_START'] or 0) / 1e12 
                        # Tính TS chính xác
                        d['timestamp_calculated'] = (boot_dt + timedelta(seconds=t_start)).isoformat()
                        type('RowWrapper', (object,), {'_mapping': d})
                        mutable_rows.append(type('RowWrapper', (object,), {'_mapping': d}))

                    cnt, max_id_batch, max_ts_batch = process_and_push(mutable_rows, redis, "RAM")
                    
                    # Update State
                    if max_id_batch > last_id:
                        last_id = max_id_batch
                        if max_ts_batch > last_ts: last_ts = max_ts_batch
                        save_state(last_id, curr_boot, last_ts)
                        logging.info(f"⚡ Realtime: {cnt} logs (New ID: {last_id})")
                else:
                    # [FIX] Vẫn phải update last_id nếu DB đã trôi đi (do filter)
                    # Nếu không, lần sau query > last_id cũ sẽ vẫn trả về các dòng đã bị filter -> Loop rỗng
                    if max_id_ram > last_id:
                        last_id = max_id_ram
                        save_state(last_id, curr_boot, last_ts)

        except Exception as e:
            logging.error(f"Loop Error: {e}")
            time.sleep(5)
            try: engine = connect_db(); redis = connect_redis()
            except: pass
        
        time.sleep(1) # Poll interval

if __name__ == "__main__":
    monitor_hybrid()