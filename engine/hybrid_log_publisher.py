# engine/perf_log_publisher.py
import os, json, logging, sys, time, signal
import pandas as pd
from redis import Redis, ConnectionError as RedisConnectionError, RedisError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
from datetime import datetime, timedelta, timezone
import math
from collections import Counter

# Setup path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config import *
    from engine.utils import save_logs_to_parquet, configure_redis_for_reliability, handle_redis_misconf_error, extract_db_from_sql 
except ImportError:
    print("Lỗi: Không thể import config/utils.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [HybridPublisher] - %(message)s")

# Đổi tên file state để tránh xung đột với version cũ dùng ID
STATE_FILE = os.path.join(LOGS_DIR, ".mysql_hybrid_timer.json")
STREAM_KEY = f"{REDIS_STREAM_LOGS}:mysql"
is_running = True

def handle_shutdown(signum, frame):
    global is_running
    logging.info("🛑 Stopping publisher...")
    is_running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# === 1. State Management (TIMER_START based) ===
def load_state():
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except:
        # last_timer_start: Con trỏ cho RAM (picoseconds)
        # last_event_ts: Con trỏ cho Disk (datetime string)
        return {"last_timer_start": 0, "boot_signature": "", "last_event_ts": "2024-01-01T00:00:00.000000"}

def save_state(timer_start, boot_sig, event_ts):
    state = {
        "last_timer_start": timer_start,
        "boot_signature": boot_sig,
        "last_event_ts": event_ts
    }
    try:
        with open(STATE_FILE, 'w') as f: json.dump(state, f)
    except Exception as e: logging.error(f"Save state failed: {e}")

# === 2. Connection ===
def connect_db():
    try:
        url = MYSQL_LOG_DATABASE_URL.replace("/mysql", "/uba_db") if "/mysql" in MYSQL_LOG_DATABASE_URL else MYSQL_LOG_DATABASE_URL
        engine = create_engine(
            url,
            pool_pre_ping=True,  # Test connections before using them
            pool_recycle=3600,   # Recycle connections after 1 hour
            pool_size=5,
            max_overflow=10,
            connect_args={
                'connect_timeout': 10,
                'autocommit': True
            }
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        logging.error(f"DB Connect failed: {e}")
        return None

def connect_redis():
    try:
        r = Redis.from_url(
            REDIS_URL, 
            decode_responses=True,
            socket_keepalive=True,
            socket_keepalive_options={},
            health_check_interval=30,
            retry_on_timeout=True,
            socket_connect_timeout=5
        )
        r.ping()
        
        # Configure Redis for better reliability
        configure_redis_for_reliability(r)
        
        return r
    except Exception as e:
        logging.error(f"Redis connection failed: {e}")
        return None

# === 3. Helpers ===
def calculate_entropy(text):
    if not text: return 0.0
    counter = Counter(text)
    length = len(text)
    return -sum((count/length) * math.log2(count/length) for count in counter.values())

def get_client_ip_safe(host_str):
    if not host_str: return 'unknown'
    try:
        if host_str == 'localhost': return 'localhost'
        if ':' in host_str:
            parts = host_str.split(':')
            if len(parts) > 0: return parts[0]
        return str(host_str)
    except: return 'unknown'

def process_and_push(rows, redis_client, source_type="RAM"):
    """Hàm xử lý chung cho cả RAM và Disk rows"""
    new_records = []
    max_timer_in_batch = 0
    max_ts_in_batch = ""

    for row in rows:
        try:
            r = row._mapping
            
            # --- 1. Chuẩn hóa Timestamp & Cursor ---
            if source_type == "RAM":
                # RAM: Dùng TIMER_START làm con trỏ
                current_timer = int(r['TIMER_START'] or 0)
                ts_iso = r['timestamp_calculated']
            else:
                # Disk: Dùng event_ts làm con trỏ
                current_timer = 0 
                ts_val = r['event_ts']
                ts_iso = ts_val.isoformat() if isinstance(ts_val, datetime) else str(ts_val)

            # Tracking Max
            if source_type == "RAM" and current_timer > max_timer_in_batch: 
                max_timer_in_batch = current_timer
            
            # Luôn track max timestamp để dùng cho Recovery
            if ts_iso > max_ts_in_batch: 
                max_ts_in_batch = ts_iso

            # Helper lấy value
            def g(k, default=0):
                val = r.get(k)
                if val is not None: return val
                val = r.get(k.lower())
                if val is not None: return val
                return default

            # --- 2. Lọc rác ---
            sql_txt = str(g('SQL_TEXT') or g('sql_text') or '')
            # user_name = str(g('PROCESSLIST_USER') or g('processlist_user') or 'unknown')
            # if user_name == 'uba_user' and 'rollback' in sql_txt.lower():
            #     continue

            # --- 3. Features ---
            rows_sent = int(g('ROWS_SENT') or g('rows_sent') or 0)
            rows_exam = int(g('ROWS_EXAMINED') or g('rows_examined') or 0)
            scan_eff = rows_sent / (rows_exam + 1)
            
            exec_ms = float(g('TIMER_WAIT') or g('timer_wait') or 0) / 1000000.0
            lock_ms = float(g('LOCK_TIME') or g('lock_time') or 0) / 1000000.0
            
            sql_up = sql_txt.upper()
            
            raw_db = str(g('CURRENT_SCHEMA') or g('current_schema'))
            db_name = "unknown"
                    
            # Check if current_schema provides valid database name
            if raw_db and raw_db.lower() not in ['unknown', 'none', '', 'null']:
                db_name = raw_db.lower()
            else:
                # 2. If MySQL doesn't track it, parse from SQL text
                # This handles cases where queries use fully qualified table names
                # like "SELECT * FROM sales_db.orders" without "USE sales_db"
                detected_db = extract_db_from_sql(sql_txt)
                if detected_db:
                    db_name = detected_db
                    # Log when we successfully detect DB from SQL that MySQL missed
                    logging.debug(f"Detected database '{detected_db}' from SQL text where current_schema was '{raw_db}'")
                                
                                
            # Cập nhật flag hệ thống dựa trên db_name mới tìm được
            is_system = 1 if db_name in ['mysql','sys','information_schema','performance_schema'] else 0
            host_str = str(g('PROCESSLIST_HOST') or g('processlist_host') or 'unknown')
            client_ip = get_client_ip_safe(host_str)

            record = {
                "timestamp": ts_iso,
                # Event ID vẫn lưu để truy vết, nhưng không dùng làm state nữa
                "event_id": int(g('e_EVENT_ID') or g('event_id') or 0), 
                "event_name": str(g('EVENT_NAME') or g('event_name') or ''),
                
                "user": str(g('PROCESSLIST_USER') or g('processlist_user') or 'unknown'),
                "client_ip": client_ip,
                "client_port": 0,
                "database": db_name,
                "query": sql_txt,
                "normalized_query": str(g('DIGEST_TEXT') or g('digest_text') or ''),
                "query_digest": str(g('DIGEST') or g('digest') or ''),
                
                "query_length": len(sql_txt),
                "query_entropy": float(f"{calculate_entropy(sql_txt):.4f}"),
                "is_system_table": is_system,
                "scan_efficiency": float(f"{scan_eff:.6f}"),
                
                "is_admin_command": 1 if any(k in sql_up for k in ['GRANT','REVOKE','CREATE USER']) else 0,
                "is_risky_command": 1 if any(k in sql_up for k in ['DROP','TRUNCATE']) else 0,
                "has_comment": 1 if ('--' in sql_txt or '/*' in sql_txt) else 0,
                
                "execution_time_ms": exec_ms,
                "lock_time_ms": lock_ms,
                "cpu_time_ms": float(g('CPU_TIME') or g('cput_time') or 0) / 1000000.0, # Pico -> ms
                "program_name": str(g('program_name') or (g('PROGRAM_NAME')) or 'unknown'),
                "connector_name": str(g('connector_name') or g('CONNECTOR_NAME') or 'unknown'),
                "client_os": str(g('client_os') or g('CLIENT_OS') or 'unknown'),
                "source_host": str(g('source_host') or g('SOURCE_HOST') or 'unknown'),
                "rows_returned": rows_sent,
                "rows_examined": rows_exam,
                "rows_affected": int(g('ROWS_AFFECTED') or g('rows_affected') or 0),
                
                "error_code": int(g('MYSQL_ERRNO') or g('mysql_errno') or 0),
                "error_count": int(g('ERRORS') or g('errors') or 0),
                "has_error": 1 if int(g('ERRORS') or g('errors') or 0) > 0 else 0,
                "warning_count": int(g('WARNINGS') or g('warnings') or 0),
                
                "created_tmp_disk_tables": int(g('CREATED_TMP_DISK_TABLES') or g('created_tmp_disk_tables') or 0),
                "created_tmp_tables": int(g('CREATED_TMP_TABLES') or g('created_tmp_tables') or 0),
                "select_full_join": int(g('SELECT_FULL_JOIN') or g('select_full_join') or 0),
                "select_scan": int(g('SELECT_SCAN') or g('select_scan') or 0),
                "sort_merge_passes": int(g('SORT_MERGE_PASSES') or g('sort_merge_passes') or 0),
                "no_index_used": int(g('NO_INDEX_USED') or g('no_index_used') or 0),
                "no_good_index_used": int(g('NO_GOOD_INDEX_USED') or g('no_good_index_used') or 0),
                
                "connection_type": str(g('CONNECTION_TYPE') or g('connection_type') or 'unknown'),
                "thread_os_id": int(g('THREAD_OS_ID') or g('thread_os_id') or 0),
                "source_dbms": "MySQL"
            }
            new_records.append(record)
        except Exception as e:
            logging.error(f"Row Error: {e}")
            continue

    if new_records:
        try:
            pipe = redis_client.pipeline()
            for rec in new_records:
                pipe.xadd(STREAM_KEY, {"data": json.dumps(rec, default=str)})
            pipe.execute()
        except (RedisConnectionError, ConnectionResetError, BrokenPipeError) as e:
            logging.error(f"Redis connection error while pushing logs: {e}")
            # Continue to save to parquet even if Redis fails
        except RedisError as e:
            # Handle MISCONF and other Redis errors
            logging.error(f"Redis error while pushing logs: {e}")
            if "MISCONF" in str(e):
                logging.info(handle_redis_misconf_error(str(e)))
            # Continue to save to parquet even if Redis fails
        except Exception as e:
            logging.error(f"Unexpected error while pushing to Redis: {e}")
            # Continue to save to parquet even if Redis fails
        
        save_logs_to_parquet(new_records, source_dbms="MySQL")
    
    return len(new_records), max_timer_in_batch, max_ts_in_batch

# === 4. Main Logic ===
def monitor_hybrid():
    global is_running
    engine = connect_db()
    redis = connect_redis()
    if not engine or not redis: return

    state = load_state()
    last_timer = state['last_timer_start'] # Con trỏ RAM
    last_ts = state['last_event_ts']       # Con trỏ Disk
    saved_boot = state['boot_signature']

    logging.info(f"🚀 Hybrid Publisher Started. RAM Timer: {last_timer}, Disk TS: {last_ts}")

    # Query RAM (Dùng TIMER_START làm điều kiện)
    sql_ram = text("""
        SELECT 
            e.EVENT_ID AS e_EVENT_ID, 
            e.TIMER_START, e.TIMER_WAIT, e.LOCK_TIME,
            e.SQL_TEXT, e.DIGEST, e.DIGEST_TEXT, e.CURRENT_SCHEMA, e.EVENT_NAME,
            e.CPU_TIME,
            (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs a 
            WHERE a.PROCESSLIST_ID = t.PROCESSLIST_ID AND a.ATTR_NAME = 'program_name' LIMIT 1) AS program_name,
            (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs a 
            WHERE a.PROCESSLIST_ID = t.PROCESSLIST_ID AND a.ATTR_NAME = '_connector_name' LIMIT 1) AS connector_name,
            (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs a 
            WHERE a.PROCESSLIST_ID = t.PROCESSLIST_ID AND a.ATTR_NAME = '_os' LIMIT 1) AS client_os,
            (SELECT ATTR_VALUE FROM performance_schema.session_connect_attrs a 
            WHERE a.PROCESSLIST_ID = t.PROCESSLIST_ID AND a.ATTR_NAME = '_source_host' LIMIT 1) AS source_host,
            e.ROWS_SENT, e.ROWS_EXAMINED, e.ROWS_AFFECTED,
            e.MYSQL_ERRNO, e.MESSAGE_TEXT, e.ERRORS, e.WARNINGS,
            e.CREATED_TMP_DISK_TABLES, e.CREATED_TMP_TABLES, e.SELECT_FULL_JOIN, e.SELECT_SCAN, e.NO_INDEX_USED,
            t.PROCESSLIST_USER, t.PROCESSLIST_HOST, t.CONNECTION_TYPE, t.THREAD_OS_ID
        FROM performance_schema.events_statements_history_long e
        LEFT JOIN performance_schema.threads t ON e.THREAD_ID = t.THREAD_ID
        WHERE e.TIMER_START > :ltimer 
          AND e.SQL_TEXT IS NOT NULL 
          AND (e.CURRENT_SCHEMA IS NULL OR e.CURRENT_SCHEMA != 'uba_db')
          AND e.SQL_TEXT NOT LIKE '%UBA_EVENT%'
          AND (t.PROCESSLIST_USER IS NULL OR t.PROCESSLIST_USER NOT IN ('uba_user'))
        ORDER BY e.TIMER_START ASC LIMIT 5000
    """)

    # Query Disk (Recovery via Timestamp)
    sql_disk = text("""
        SELECT * FROM uba_db.uba_persistent_log
        WHERE event_ts > :lts 
          AND SQL_TEXT IS NOT NULL 
          -- AND (t.PROCESSLIST_USER IS NULL OR t.PROCESSLIST_USER NOT IN ('event_scheduler', 'uba_user', 'boot'))
          AND (CURRENT_SCHEMA IS NULL OR CURRENT_SCHEMA != 'uba_db')
          -- AND SQL_TEXT NOT LIKE '%uba_persistent_log%'
          AND SQL_TEXT NOT LIKE '%UBA_EVENT%'
          -- AND SQL_TEXT NOT LIKE '%rollback%'
          -- AND SQL_TEXT NOT LIKE '%version_comment%'
          -- AND SQL_TEXT != '%auto_commit%'
          -- AND EVENT_NAME != 'statement/sql/rollback'
          AND (PROCESSLIST_USER IS NULL OR PROCESSLIST_USER NOT IN ('uba_user'))
        ORDER BY event_ts ASC LIMIT 5000
    """)
    
    sql_uptime = text("SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='UPTIME' AND 'UBA_EVENT' = 'UBA_EVENT'")
    # Kiểm tra Bounds dựa trên TIMER_START
    sql_bounds = text("SELECT MIN(TIMER_START), MAX(TIMER_START) FROM performance_schema.events_statements_history_long  WHERE 'UBA_EVENT' = 'UBA_EVENT'")
    sql_disk_max = text("SELECT MAX(event_ts) FROM uba_db.uba_persistent_log WHERE 'UBA_EVENT' = 'UBA_EVENT'")

    while is_running:
        try:
            # Check if engine is valid before attempting connection
            if not engine:
                logging.warning("⚠️ Engine is None, attempting to reconnect...")
                engine = connect_db()
                if not engine:
                    time.sleep(5)
                    continue
            
            # Check if Redis is valid
            if not redis:
                logging.warning("⚠️ Redis is None, attempting to reconnect...")
                redis = connect_redis()
                if not redis:
                    logging.error("❌ Redis reconnection failed, continuing without Redis...")
            
            with engine.connect() as conn:
                # 1. Boot Check
                uptime = float(conn.execute(sql_uptime).scalar() or 0)
                boot_dt = datetime.now(timezone.utc) - timedelta(seconds=uptime)
                curr_boot = boot_dt.strftime("%Y-%m-%d %H:%M")

                # 2. Recovery Check (Dựa trên TIMER_START)
                min_timer_ram, max_timer_ram = conn.execute(sql_bounds).fetchone()
                if min_timer_ram is None: min_timer_ram, max_timer_ram = 0, 0
                
                # Check Disk Max TS
                disk_max_ts_raw = conn.execute(sql_disk_max).scalar()
                disk_max_ts = str(disk_max_ts_raw) if disk_max_ts_raw else ""
                
                need_recovery = False
                
                # Case 1: DB Restarted (Boot thay đổi -> Timer reset về 0)
                if saved_boot and curr_boot != saved_boot:
                    logging.warning(f"⚠️ DB Restart! Boot changed. Switching to Disk Recovery.")
                    need_recovery = True
                
                # Case 2: RAM Wrap-around (Timer đã lưu nhỏ hơn Min Timer trong RAM)
                elif last_timer < min_timer_ram and max_timer_ram > 0:
                    logging.warning(f"⚠️ RAM Wrap: Saved Timer {last_timer} < Min {min_timer_ram}. Switching to Disk Recovery.")
                    need_recovery = True
                
                # Case 3: Disk có dữ liệu mới hơn local (Catchup lúc khởi động)
                elif disk_max_ts > last_ts and last_ts != "":
                    logging.warning(f"⚠️ Disk has newer logs (Disk {disk_max_ts} > Local {last_ts}). Switching to Disk Recovery.")
                    need_recovery = True

                # --- RECOVERY MODE (Disk) ---
                if need_recovery:
                    logging.info(f"🔄 RECOVERY MODE (From TS: {last_ts})...")
                    while is_running:
                        rows = conn.execute(sql_disk, {"lts": last_ts}).fetchall()
                        if not rows:
                            # [FIX] Khi hết log Disk, cập nhật lại RAM Timer mới nhất
                            curr_max_ram = conn.execute(sql_bounds).fetchone()[1] or 0
                            last_timer = int(curr_max_ram)
                            saved_boot = curr_boot
                            
                            logging.info(f"✅ Catch-up done. Sync RAM Timer: {last_timer}.")
                            save_state(last_timer, saved_boot, last_ts) 
                            break
                        
                        cnt, _, max_ts_batch = process_and_push(rows, redis, "DISK")
                        
                        if max_ts_batch > last_ts: last_ts = max_ts_batch
                        
                        logging.info(f"📥 Recovered {cnt} logs.")
                        save_state(last_timer, saved_boot, last_ts)
                        time.sleep(0.1) 
                    continue

                # --- REALTIME MODE (RAM) ---
                rows = conn.execute(sql_ram, {"ltimer": last_timer}).fetchall()
                
                if rows:
                    mutable_rows = []
                    for r in rows:
                        d = dict(r._mapping)
                        t_start = float(d['TIMER_START'] or 0) / 1e12 
                        # Tính timestamp chính xác: Boot + TimerStart
                        d['timestamp_calculated'] = (boot_dt + timedelta(seconds=t_start)).isoformat()
                        type('RowWrapper', (object,), {'_mapping': d})
                        mutable_rows.append(type('RowWrapper', (object,), {'_mapping': d}))

                    cnt, max_timer_batch, max_ts_batch = process_and_push(mutable_rows, redis, "RAM")
                    
                    if max_timer_batch > last_timer:
                        last_timer = max_timer_batch
                        if max_ts_batch > last_ts: last_ts = max_ts_batch
                        
                        saved_boot = curr_boot
                        save_state(last_timer, saved_boot, last_ts)
                        if cnt > 0:
                            logging.info(f"⚡ Realtime: {cnt} logs (New Timer: {last_timer})")
                else:
                    # Catch-up con trỏ RAM nếu bị filter
                    if max_timer_ram > last_timer:
                        last_timer = max_timer_ram
                        save_state(last_timer, curr_boot, last_ts)

        except (OperationalError, DBAPIError) as e:
            # Database connection errors - attempt reconnection
            logging.error(f"Database connection error: {e}")
            logging.info("🔄 Attempting to reconnect to MySQL...")
            time.sleep(5)
            try:
                if engine:
                    engine.dispose()  # Close all connections in the pool
                engine = connect_db()
                redis = connect_redis()
                if engine and redis:
                    logging.info("✅ MySQL & Redis reconnection successful")
                elif engine:
                    logging.info("✅ MySQL reconnection successful (Redis failed)")
                else:
                    logging.error("❌ MySQL reconnection failed, will retry...")
            except Exception as reconnect_error:
                logging.error(f"Reconnect error: {reconnect_error}")
        
        except (RedisConnectionError, ConnectionResetError, BrokenPipeError) as e:
            # Redis connection errors - attempt reconnection
            logging.error(f"Redis connection error: {e}")
            logging.info("🔄 Attempting to reconnect to Redis...")
            time.sleep(3)
            try:
                if redis:
                    redis.close()
                redis = connect_redis()
                if redis:
                    logging.info("✅ Redis reconnection successful")
                else:
                    logging.error("❌ Redis reconnection failed, will retry...")
            except Exception as reconnect_error:
                logging.error(f"Redis reconnect error: {reconnect_error}")
        
        except Exception as e:
            # Other unexpected errors - log but continue
            logging.error(f"Unexpected error: {e}", exc_info=True)
            time.sleep(2)
        
        time.sleep(1)

if __name__ == "__main__":
    monitor_hybrid()