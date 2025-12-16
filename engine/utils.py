import pandas as pd
import os
import sqlglot
import sqlglot.errors as errors
import hashlib
import json
import re
from datetime import time as dt_time
import logging
import uuid
from datetime import datetime
from typing import Set
from redis import Redis, RedisError

# Thêm cấu hình logging ở đầu file
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Utils] - %(message)s')

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import *

# --- Import GeoIP (Xử lý nếu chưa cài thư viện) ---
try:
    import geoip2.database
    from geopy.distance import geodesic
    HAS_GEOIP = True
except ImportError:
    HAS_GEOIP = False

try:
    import sqlglot
    from sqlglot import exp
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    
# ==============================================================================
#   CÁC HÀM HỖ TRỢ FEATURE ENGINEERING VÀ FEEDBACK
# ==============================================================================
# Các hàm trong mục này hỗ trợ việc trích xuất đặc trưng cho AI và xử lý feedback.

def extract_db_from_sql(sql_text):
    """
    Tự động trích xuất tên Database từ câu lệnh SQL.
    Ưu tiên dùng SQLGlot, fallback sang Regex.
    Ví dụ: "SELECT * FROM sales_db.orders" -> "sales_db"
    
    Improvements:
    - Better regex patterns to catch more cases
    - Handle multiple databases in one query (returns first non-system DB)
    - Better error handling and logging
    - Support for more SQL statement types
    """
    if not sql_text or not isinstance(sql_text, str):
        return None
    
    # Clean and normalize the SQL text
    sql_text = sql_text.strip()
    if not sql_text:
        return None

    # Cách 1: Dùng SQLGlot (Chính xác nhất)
    if SQLGLOT_AVAILABLE:
        try:
            # Parse query (limit độ dài để tránh treo)
            parsed = sqlglot.parse_one(sql_text[:5000], read="mysql")
            if parsed:
                # Collect all database names, prioritize non-system databases
                databases = []
                for table in parsed.find_all(exp.Table):
                    if table.db and table.db.lower() not in ['mysql', 'sys', 'information_schema', 'performance_schema']:
                        databases.append(table.db.lower())
                    elif table.db:  # System database as fallback
                        databases.append(table.db.lower())
                
                # Return first non-system database, or first database if all are system
                if databases:
                    return databases[0]
        except Exception as e:
            # Log parsing errors for debugging but continue with regex fallback
            logging.debug(f"SQLGlot parsing failed for query: {e}")

    # Cách 2: Dùng Regex (Nhanh, dự phòng) - Improved patterns
    # Enhanced regex patterns to catch more SQL statement types and formats
    patterns = [
        # Standard table references with database prefix
        r'(?:FROM|JOIN|UPDATE|INTO|TABLE|REPLACE\s+INTO)\s+[`\'"]?([a-zA-Z0-9_]+)[`\'"]?\s*\.\s*[`\'"]?[a-zA-Z0-9_]+[`\'"]?',
        
        # USE statement
        r'USE\s+[`\'"]?([a-zA-Z0-9_]+)[`\'"]?(?:\s|;|$)',
        
        # CREATE/DROP/ALTER DATABASE
        r'(?:CREATE|DROP|ALTER)\s+(?:DATABASE|SCHEMA)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?[`\'"]?([a-zA-Z0-9_]+)[`\'"]?',
        
        # SHOW statements with database context
        r'SHOW\s+(?:TABLES|COLUMNS|INDEX)\s+(?:FROM|IN)\s+[`\'"]?([a-zA-Z0-9_]+)[`\'"]?',
        
        # INSERT INTO with database prefix
        r'INSERT\s+(?:IGNORE\s+)?INTO\s+[`\'"]?([a-zA-Z0-9_]+)[`\'"]?\s*\.\s*[`\'"]?[a-zA-Z0-9_]+[`\'"]?',
        
        # DELETE FROM with database prefix
        r'DELETE\s+FROM\s+[`\'"]?([a-zA-Z0-9_]+)[`\'"]?\s*\.\s*[`\'"]?[a-zA-Z0-9_]+[`\'"]?',
        
        # CALL stored procedure with database prefix
        r'CALL\s+[`\'"]?([a-zA-Z0-9_]+)[`\'"]?\s*\.\s*[`\'"]?[a-zA-Z0-9_]+[`\'"]?',
        
        # DESCRIBE/DESC with database prefix
        r'(?:DESCRIBE|DESC)\s+[`\'"]?([a-zA-Z0-9_]+)[`\'"]?\s*\.\s*[`\'"]?[a-zA-Z0-9_]+[`\'"]?'
    ]
    
    # Collect all matches and prioritize non-system databases
    found_databases = []
    system_databases = {'mysql', 'sys', 'information_schema', 'performance_schema'}
    
    for pattern in patterns:
        matches = re.finditer(pattern, sql_text, re.IGNORECASE)
        for match in matches:
            db_name = match.group(1).lower()
            if db_name not in system_databases:
                found_databases.append(db_name)
            elif not found_databases:  # Only add system DB if no user DB found yet
                found_databases.append(db_name)
    
    # Return first non-system database, or first database if all are system
    if found_databases:
        return found_databases[0]

    return None

def get_tables_with_sqlglot(sql_query):
    """Trích xuất tên các bảng từ một câu lệnh SQL sử dụng thư viện sqlglot."""
    tables = set() # Dùng set để tự động loại bỏ các tên bảng trùng lặp.
    
    # Trả về danh sách rỗng nếu query không hợp lệ.
    if pd.isna(sql_query) or not isinstance(sql_query, str) or not sql_query.strip():
        return []
        
    try:
        # Phân tích cú pháp câu lệnh SQL theo dialect của MySQL.
        parsed_expression = sqlglot.parse_one(sql_query, read='mysql')
        if parsed_expression:
            # Tìm tất cả các node là Table trong cây cú pháp trừu tượng.
            for table_node in parsed_expression.find_all(exp.Table):
                final_table_name = table_node.name
                # Xử lý trường hợp tên bảng có alias.
                if hasattr(table_node, 'this') and table_node.this and isinstance(table_node.this, exp.Identifier):
                     final_table_name = table_node.this.name
                if final_table_name:
                    tables.add(final_table_name.lower()) # Chuẩn hóa về chữ thường.
    except (errors.ParseError, Exception):
        # Bỏ qua nếu sqlglot không thể phân tích cú pháp câu lệnh để chương trình không bị dừng.
        pass
        
    return list(tables)

def extract_query_features(sql_query):
    """
    Trích xuất một tập hợp các đặc trưng số học từ một câu lệnh SQL.
    Sử dụng sqlglot để phân tích cú pháp một cách hiệu quả và an toàn.
    Trả về một dictionary các đặc trưng số học.
    """
    # Khởi tạo giá trị mặc định cho tất cả các đặc trưng
    features = {
        'num_joins': 0,
        'num_where_conditions': 0,
        'num_group_by_cols': 0,
        'num_order_by_cols': 0,
        'has_limit': 0,
        'has_subquery': 0,
        'has_union': 0,
        'has_where': 0
    }
    
    # Trả về mặc định nếu query không hợp lệ
    if pd.isna(sql_query) or not isinstance(sql_query, str) or not sql_query.strip():
        return features

    try:
        parsed = sqlglot.parse_one(sql_query, read='mysql')
        if parsed:
            # === Kỹ thuật tối ưu từ code của bạn bạn ===
            # Đếm số lượng JOIN
            features['num_joins'] = sum(1 for _ in parsed.find_all(exp.Join))
            # Kiểm tra sự tồn tại (hiệu quả hơn .find is not None)
            features['has_limit'] = 1 if parsed.find(exp.Limit) else 0
            features['has_subquery'] = 1 if parsed.find(exp.Subquery) else 0
            features['has_union'] = 1 if parsed.find(exp.Union) else 0
            
            # === Trích xuất các đặc trưng chi tiết hơn ===
            # Đếm số lượng điều kiện trong WHERE
            where_clause = parsed.find(exp.Where)
            if where_clause:
                features['has_where'] = 1
                # Ước tính số điều kiện bằng cách đếm các toán tử logic
                # (AND, OR) và cộng 1. find_all nhanh hơn walk() cho mục đích này.
                # conditions = len(where_clause.find_all(exp.And, exp.Or))
                # Đếm số lượng phần tử mà không cần tạo list
                conditions = sum(1 for _ in where_clause.find_all(exp.And, exp.Or))
                features['num_where_conditions'] = conditions + 1
            
            # Đếm số cột trong GROUP BY
            group_by_clause = parsed.find(exp.Group)
            if group_by_clause:
                # `group_by_clause.expressions` là danh sách các cột
                features['num_group_by_cols'] = len(group_by_clause.expressions)

            # Đếm số cột trong ORDER BY
            order_by_clause = parsed.find(exp.Order)
            if order_by_clause:
                features['num_order_by_cols'] = len(order_by_clause.expressions)
            
    except errors.ParseError:
        # Nếu sqlglot không parse được, giữ nguyên giá trị mặc định
        pass
    except Exception:
        # Bắt các lỗi không lường trước khác
        pass
        
    return features

def save_feedback_to_csv(item_data: dict, label: int) -> tuple[bool, str]:
    try:
        # item_data đã là dict:
        identifier_string = f"{item_data.get('timestamp')}{item_data.get('user')}{item_data.get('query')}"
        feedback_id = hashlib.md5(str(identifier_string).encode()).hexdigest()

        new_feedback_data = dict(item_data)  # clone dict
        new_feedback_data['feedback_id'] = feedback_id
        new_feedback_data['is_anomaly_label'] = label

        for k, v in list(new_feedback_data.items()):
            if isinstance(v, (list, tuple)):
                new_feedback_data[k] = json.dumps(v, ensure_ascii=False)

        # đảm bảo thư mục tồn tại
        os.makedirs(os.path.dirname(FEEDBACK_FILE_PATH) or ".", exist_ok=True)

        df_feedback = pd.DataFrame()
        if os.path.exists(FEEDBACK_FILE_PATH) and os.path.getsize(FEEDBACK_FILE_PATH) > 0:
            df_feedback = pd.read_csv(FEEDBACK_FILE_PATH)

        message = ""
        if (not df_feedback.empty and
            'feedback_id' in df_feedback.columns and
            feedback_id in df_feedback['feedback_id'].values):
            message = f"Đã CẬP NHẬT phản hồi cho mục #{feedback_id[:8]}..."
            idx = df_feedback.index[df_feedback['feedback_id'] == feedback_id][0]
            for col, value in new_feedback_data.items():
                if col in df_feedback.columns:
                    df_feedback.loc[idx, col] = value
                else:
                    df_feedback[col] = pd.NA
                    df_feedback.loc[idx, col] = value
        else:
            message = f"Đã GHI NHẬN phản hồi mới cho mục #{feedback_id[:8]}..."
            new_row_df = pd.DataFrame([new_feedback_data])
            df_feedback = pd.concat([df_feedback, new_row_df], ignore_index=True)

        ordered_cols = ['feedback_id', 'timestamp', 'user', 'query', 'is_anomaly_label']
        all_columns = sorted(list(set(df_feedback.columns.tolist() + list(new_feedback_data.keys()))))
        final_cols = [c for c in ordered_cols if c in all_columns] + [c for c in all_columns if c not in ordered_cols]

        df_feedback.to_csv(FEEDBACK_FILE_PATH, mode='w', header=True, index=False, columns=final_cols, encoding='utf-8')
        logging.info(message)
        return True, message
    except Exception as e:
        logging.error(f"Đã xảy ra lỗi khi lưu phản hồi: {e}")
        import traceback; traceback.print_exc()
        return False, f"Đã xảy ra lỗi khi lưu phản hồi: {e}"



def save_logs_to_parquet(records: list, source_dbms: str) -> int:
    if not records:
        return 0
    try:
        df = pd.DataFrame(records)
        if 'source_dbms' not in df.columns:
            df['source_dbms'] = source_dbms
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', utc=True)

        os.makedirs(STAGING_DATA_DIR, exist_ok=True)  # <-- thêm dòng này

        filename = f"{source_dbms}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
        file_path = os.path.join(STAGING_DATA_DIR, filename)
        df.to_parquet(file_path, engine='pyarrow', index=False)
        # logging.info(f"Đã lưu {len(df)} bản ghi từ '{source_dbms}' vào file: {filename}")
        return len(df)
    except Exception as e:
        logging.error(f"Lỗi khi lưu file Parquet: {e}")
        return 0

def get_normalized_query(query: str) -> str:
    """Extract DIGEST_TEXT-like normalized query"""
    if not query:
        return ""
    # Simple normalization (you can use sqlglot for better)
    query = re.sub(r'"\w+"', '"?"', query)
    query = re.sub(r"'\w+'", "'?'", query)
    query = re.sub(r'\d+', '?', query)
    return query.strip()

def count_sensitive_tables(tables: list) -> int:
    if not tables:
        return 0
    return len([t for t in tables if any(st in t.lower() for st in SENSITIVE_TABLES)])

def is_late_night(ts):
    from datetime import datetime
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return ts.hour <= 5 or ts.hour >= 23


# ============================================================
#   ACTIVE RESPONSE AUDIT LOGGER
# ============================================================

# Cấu hình logger
audit_logger = logging.getLogger('ActiveResponseAudit')
audit_logger.setLevel(logging.INFO)
audit_logger.propagate = False

# Chỉ thêm handler nếu nó chưa có để tránh log lặp lại
if not audit_logger.hasHandlers():
    try:
        # Sử dụng 'a' để ghi nối tiếp, 'utf-8'
        file_handler = logging.FileHandler(ACTIVE_RESPONSE_AUDIT_LOG_PATH, mode='a', encoding='utf-8')
        # Định dạng log: thời gian và nội dung
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        audit_logger.addHandler(file_handler)
    except Exception as e:
        print(f"LỖI: Không thể tạo file audit log tại {ACTIVE_RESPONSE_AUDIT_LOG_PATH}: {e}")

def log_active_response_action(action: str, target: str, reason: str):
    """
    Ghi lại một hành động phản ứng chủ động vào file audit log.

    Args:
        action (str): Loại hành động (ví dụ: "LOCK_ACCOUNT", "KILL_SESSION").
        target (str): Đối tượng bị tác động (ví dụ: "user@host", "Session 123").
        reason (str): Lý do thực hiện.
    """
    try:
        message = f"ACTION: {action} | TARGET: {target} | REASON: {reason}"
        audit_logger.info(message)
        for handler in audit_logger.handlers:
            handler.flush()
    except Exception as e:
        print(f"[Active Response] Lỗi khi ghi audit log: {e}")

def generate_html_alert(violation_summary: list):
    """
    Tạo nội dung HTML cho email cảnh báo.
    Args:
        violation_summary: List các dict [{'title': 'Giờ Khuya', 'count': 4, 'time': '...', 'desc': '...'}]
    """

    # CSS Inline
    html_template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max_width: 600px; margin: 0 auto; padding: 20px; background-color: #f9f9f9; }}
            .header {{ background-color: #d32f2f; color: white; padding: 20px; text-align: center; border-radius: 5px 5px 0 0; }}
            .alert-box {{ background-color: #fff; padding: 20px; border: 1px solid #ddd; border-top: none; border-radius: 0 0 5px 5px; }}
            .stat-box {{ background-color: #fff3cd; border-left: 5px solid #ffc107; padding: 15px; margin-bottom: 20px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th {{ background-color: #f2f2f2; text-align: left; padding: 10px; border-bottom: 2px solid #ddd; font-size: 12px; text-transform: uppercase; color: #555; }}
            td {{ padding: 12px 10px; border-bottom: 1px solid #eee; font-size: 14px; }}
            .severity-high {{ color: #d32f2f; font-weight: bold; }}
            .footer {{ text-align: center; font-size: 12px; color: #777; margin-top: 20px; }}
            .btn {{ display: inline-block; background-color: #d32f2f; color: white; padding: 10px 20px; text-decoration: none; border-radius: 3px; margin-top: 20px; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2 style="margin:0;">🚨 Security Alert Triggered</h2>
                <p style="margin:5px 0 0 0; font-size: 14px;">UEBA Detection System</p>
            </div>

            <div class="alert-box">
                <p>The UEBA system has detected abnormal behaviors that require your attention.</p>

                <div class="stat-box">
                    <strong>Overview:</strong> Detected <strong>{len(violation_summary)}</strong> type/s of anomalies in the latest scan.
                </div>

                <table width="100%">
                    <thead>
                        <tr>
                            <th>Type of Violation</th>
                            <th style="text-align: center;">Count</th>
                            <th>Entity (User@IP)</th>
                            <th>Occurrence (First - Last)</th>
                        </tr>
                    </thead>
                    <tbody>
    """

    # Loop để tạo các dòng trong bảng
    for item in violation_summary:
        html_template += f"""
            <tr>
                <td>
                    <span class="severity-high">{item['title']}</span><br>
                    <span style="font-size: 11px; color: #777;">{item['desc']}</span>
                </td>
                <td style="text-align: center;"><strong>{item['count']}</strong></td>
                <td style="font-size: 13px; color: #333;">
                    {item.get('target_str', 'N/A')}
                </td>
                <td style="font-size: 13px; white-space: nowrap;">
                    {item.get('time_range', 'N/A')}
                </td>
            </tr>
        """
    html_template += """
                    </tbody>
                </table>

            </div>

            <div class="footer" style="
                margin-top: 25px;
                text-align: center;
                background: #f5f5f5;
                padding: 15px 10px;
                border-top: 2px solid #d0d0d0;
                font-size: 12px;
                font-style: italic;
                color: #555;
            ">
                <p>
                    This is an automated email from the UEBA Platform system.<br>
                    Please do not reply to this email.
                </p>
            </div>

        </div>
    </body>
    </html>
    """
    return html_template

# ==============================================================================
#   CÁC HÀM HỖ TRỢ PHÂN TÍCH THEO LUẬT (RULE-BASED ANALYSIS)
# ==============================================================================

# ==============================================================================
# 1. NHÓM ACCESS ANOMALIES (Bất thường truy cập)
# Bao gồm: Concurrent Login, Brute-force, Impossible Travel
# ==============================================================================
def check_access_anomalies(df, rule_config):
    """Nhóm 1: Bất thường truy cập"""
    anomalies = {}
    thresholds = rule_config.get('thresholds', {})
    settings = rule_config.get('settings', {})
    
    # Rule 1. Concurrent Login
    limit_ips = thresholds.get('concurrent_ips_limit', 1)
    df_sorted = df.sort_values('timestamp')
    grouped = df_sorted.groupby(['user', pd.Grouper(key='timestamp', freq='5Min')], observed=False)
    idx_concurrent = []
    for (user, time), group in grouped:
        if group['client_ip'].nunique() > limit_ips:
            idx_concurrent.extend(group.index.tolist()) 
    if idx_concurrent:
        anomalies['Concurrent Login'] = list(set(idx_concurrent))
                
    # Rule 2. Brute-force Success
    limit_attempts = thresholds.get('brute_force_attempts', 5)
    failed_attempts = df[df['error_code'] != 0] # Error code != 0 là lỗi
    idx_bruteforce = [] 
    if not failed_attempts.empty:
        ip_error_counts = failed_attempts.groupby(['client_ip', pd.Grouper(key='timestamp', freq='1Min')], observed=False).size()
        suspicious_ips = ip_error_counts[ip_error_counts > limit_attempts].index.get_level_values(0).unique()     
        # Tìm log thành công ngay sau chuỗi lỗi từ IP đó
        brute_force_success = df[
            (df['client_ip'].isin(suspicious_ips)) & 
            (df['error_code'] == 0) & 
            (df['event_name'] == 'connect')
        ]
        idx_bruteforce.extend(brute_force_success.index.tolist())      
    if idx_bruteforce:
        anomalies['Brute-force Success'] = list(set(idx_bruteforce))
        
    # Rule 3. Impossible Travel
    if HAS_GEOIP:
        max_speed = thresholds.get('impossible_travel_speed_kmh', 800)
        db_path = settings.get('geoip_db_path', 'engine/geoip/GeoLite2-City.mmdb')
        idx_travel = []  
        if os.path.exists(db_path):        
            try:
                reader = geoip2.database.Reader(db_path)
                def get_lat_lon(ip):
                    try:
                        if ip in ['127.0.0.1', 'localhost', '::1', '0.0.0.0']: return None
                        res = reader.city(ip)
                        return (res.location.latitude, res.location.longitude)
                    except: return None              
                # Group by User để check hành trình
                grouped_user = df_sorted.groupby('user', observed=False)
                for user, group in grouped_user:
                    if len(group) < 2: continue
                    prev_row = None
                    for idx, row in group.iterrows():
                        curr_loc = get_lat_lon(row['client_ip'])
                        if prev_row and curr_loc and prev_row['loc']:
                            dist = geodesic(prev_row['loc'], curr_loc).km
                            time_diff = (row['timestamp'] - prev_row['time']).total_seconds() / 3600
                            if time_diff > 0 and dist > 50:
                                speed = dist / time_diff
                                if speed > max_speed:
                                    idx_travel.append(idx)
                        if curr_loc:
                            prev_row = {'loc': curr_loc, 'time': row['timestamp']}
                reader.close()
            except Exception as e:
                logging.error(f"GeoIP Logic Error: {e}")             
        if idx_travel:
            anomalies['Impossible Travel'] = list(set(idx_travel))

    return anomalies

# ============================================================================================================================================================
# 2. NHÓM INSIDER THREATS (Mối đe dọa nội bộ)
# Bao gồm: Service Account, Admin Privilege Escalation, Sensitive Access, Late Night, Ghost Account, System Table Modification, Insecure Connection
# ============================================================================================================================================================
def check_insider_threats(df, rule_config):
    """Nhóm 2: Insider Threat"""
    anomalies = {}
    service_accounts = rule_config.get('service_accounts', {})
    signatures = rule_config.get('signatures', {})
    settings = rule_config.get('settings', {})
    hr_users = set(signatures.get('hr_authorized_users', []))
    
    # Rule 4. Service Account Misuse
    idx_service = []
    overtime_schedule = signatures.get('overtime_schedule', [])
    for user, config in service_accounts.items():
        user_logs = df[df['user'] == user]
        if user_logs.empty: continue
        # 1. Check IP (Giữ nguyên logic cũ - IP sai là bắt luôn, không có ngoại lệ overtime cho IP)
        invalid_ip = user_logs[~user_logs['client_ip'].isin(config.get('allowed_ips', []))]
        idx_service.extend(invalid_ip.index.tolist())
        # 2. Check Giờ (Logic mới: Sai giờ chuẩn -> Check tiếp vé Overtime)
        # Lấy ra những dòng log vi phạm giờ chuẩn
        potential_hour_violations = user_logs[~user_logs['timestamp'].dt.hour.isin(config.get('allowed_hours', []))]
        if potential_hour_violations.empty:
            continue
        # Duyệt qua từng log vi phạm giờ để xem có "vé" overtime không
        for idx, row in potential_hour_violations.iterrows():
            is_excused = False
            # Nếu có danh sách overtime, đi kiểm tra
            if overtime_schedule:
                current_time = row['timestamp'].time()
                current_date_str = row['timestamp'].strftime('%Y-%m-%d')
                for shift in overtime_schedule:
                    # So khớp User và Ngày
                    if shift.get('user') == user and shift.get('date') == current_date_str:
                        try:
                            allowed_start = dt_time.fromisoformat(shift['start'])
                            allowed_end = dt_time.fromisoformat(shift['end'])
                            
                            # Kiểm tra giờ log có nằm trong khung giờ xin phép không
                            if allowed_start <= allowed_end:
                                if allowed_start <= current_time <= allowed_end:
                                    is_excused = True
                                    break
                            else: # Xử lý qua đêm (ví dụ 23h đến 2h sáng)
                                if allowed_start <= current_time or current_time <= allowed_end:
                                    is_excused = True
                                    break
                        except:
                            continue
            # Nếu KHÔNG được tha (không khớp lịch overtime nào) -> Thêm vào danh sách lỗi
            if not is_excused:
                idx_service.append(idx)    
    if idx_service: anomalies['Service Account Misuse'] = list(set(idx_service))

    # Rule 5. Admin Privilege Escalation
    idx_admin = []
    admin_kws = signatures.get('admin_keywords', [])
    if admin_kws:
        pattern = "|".join(re.escape(k) for k in admin_kws)
        admin_actions = df[
            (df['query'].str.contains(pattern, case=False, na=False)) &
            (df['user'] != 'root')
        ]
        idx_admin.extend(admin_actions.index.tolist())       
    if idx_admin: anomalies['Admin Privilege Abuse'] = list(set(idx_admin))
        
    # Rule 6. Sensitive Table Access 
    idx_sensitive = []
    sensitive_tables = signatures.get('sensitive_tables', [])
    allowed_users = settings.get('sensitive_allowed_users', [])   
    safe_start = settings.get('sensitive_safe_hours_start', 8)
    safe_end = settings.get('sensitive_safe_hours_end', 17) 
    # Hàm check logic
    def is_violation(row):
        # 1. Kiểm tra xem query có đụng vào bảng nhạy cảm không?
        is_sensitive_query = False
        for tbl in sensitive_tables:
            if tbl in str(row['query']):
                is_sensitive_query = True
                break
        if not is_sensitive_query:
            return False # Không vi phạm vì không đụng bảng nhạy cảm
        # 2. Kiểm tra User
        if row['user'] not in allowed_users:
            return True # Vi phạm nghiêm trọng: User không có quyền mà truy cập
        # 3. Kiểm tra Thời gian (Dành cho User ĐÃ CÓ QUYỀN)
        # Ngay cả khi có quyền, nếu truy cập ngoài giờ hành chính cũng bị coi là bất thường
        try:
            hour = row['timestamp'].hour
            # Nếu giờ hiện tại nhỏ hơn giờ bắt đầu HOẶC lớn hơn giờ kết thúc
            if hour < safe_start or hour >= safe_end:
                return True # Vi phạm: User có quyền nhưng truy cập sai giờ (ví dụ: kế toán truy cập bảng lương lúc 3h sáng)
        except:
            pass
        return False # Hợp lệ (Đúng người, đúng giờ)
    if sensitive_tables:
        sensitive_violation = df[df.apply(is_violation, axis=1)]
        idx_sensitive.extend(sensitive_violation.index.tolist())   
    if idx_sensitive: anomalies['Sensitive Table Access'] = list(set(idx_sensitive))

    # Rule 7. Late Night Query 
    # Logic: Truy cập ngoài giờ hành chính (22h - 5h sáng)
    idx_latenight = []
    try:
        # Lấy cấu hình giờ khuya
        s_str = settings.get('late_night_start', '22:00:00')
        e_str = settings.get('late_night_end', '05:00:00')
        start_time_limit = dt_time.fromisoformat(s_str)
        end_time_limit = dt_time.fromisoformat(e_str)
        # Lấy danh sách đăng ký làm thêm giờ/bảo trì
        overtime_schedule = signatures.get('overtime_schedule', [])
        def is_late_night_violation(row):
            ts = row['timestamp']
            current_time = ts.time()
            current_date_str = ts.strftime('%Y-%m-%d')
            user_name = row['user']
            current_ip = row['client_ip'] # Lấy IP hiện tại
            # 1. Kiểm tra khung giờ khuya
            is_night = False
            if start_time_limit <= end_time_limit:
                is_night = start_time_limit <= current_time <= end_time_limit
            else:
                is_night = start_time_limit <= current_time or current_time <= end_time_limit           
            if not is_night:
                return False # Không phải giờ khuya -> Không vi phạm
            # 2. Check Overtime Schedule (Vé thông hành)
            if overtime_schedule:
                for shift in overtime_schedule:
                    # Check User và Date
                    if shift.get('user') == user_name and shift.get('date') == current_date_str:
                        # Nếu trong lịch có đăng ký IP, thì bắt buộc phải khớp
                        registered_ip = shift.get('ip')
                        if registered_ip and registered_ip.strip():
                            # Nếu IP đăng ký khác IP đang truy cập -> Không chấp nhận vé này
                            if current_ip != registered_ip.strip():
                                continue 
                        try:
                            allowed_start = dt_time.fromisoformat(shift['start'])
                            allowed_end = dt_time.fromisoformat(shift['end'])                            
                            is_authorized = False
                            if allowed_start <= allowed_end:
                                is_authorized = allowed_start <= current_time <= allowed_end
                            else:
                                is_authorized = allowed_start <= current_time or current_time <= allowed_end                            
                            if is_authorized:
                                return False # Hợp lệ (Đúng người, đúng giờ, đúng IP)
                        except ValueError:
                            continue
            # 3. Không có vé hoặc vé không khớp -> Vi phạm
            return True
        late_night_logs = df[df.apply(is_late_night_violation, axis=1)]
        idx_latenight.extend(late_night_logs.index.tolist())
    except Exception as e:
        logging.error(f"Rule 7 Logic Error: {e}")   
    if idx_latenight: anomalies['Late Night Query'] = list(set(idx_latenight))
    
    # Rule 8. Ghost Account Creation
    idx_ghost = []
    create_cmds = df[df['query'].str.contains("CREATE USER", case=False, na=False)]
    if not create_cmds.empty:
        pattern = re.compile(r"CREATE\s+USER\s+['\"`]?([a-zA-Z0-9_]+)['\"`]?", re.IGNORECASE)
        for idx, row in create_cmds.iterrows():
            match = pattern.search(row['query'])
            if match and match.group(1) not in hr_users:
                idx_ghost.append(idx)               
    if idx_ghost: anomalies['Ghost Account Creation'] = list(set(idx_ghost))

    # Rule 9. System Table Modification
    # Logic: Can thiệp bảng hệ thống nhưng không phải lệnh SELECT/SHOW
    idx_sys_mod = []
    if 'is_system_table' in df.columns:
        # Lọc các dòng tác động bảng hệ thống
        sys_access = df[df['is_system_table'] == 1]
        if not sys_access.empty:
            # Loại trừ các lệnh chỉ đọc (SELECT, SHOW, DESCRIBE)
            # Lưu ý: event_name thường là 'statement/sql/update', 'statement/sql/insert'...
            # Cách đơn giản: query không bắt đầu bằng SELECT/SHOW
            sys_mod = sys_access[~sys_access['query'].str.match(r'^\s*(SELECT|SHOW|DESC)', case=False, na=False)]
            idx_sys_mod.extend(sys_mod.index.tolist())
    if idx_sys_mod: anomalies['System Table Modification'] = list(set(idx_sys_mod))

    # Rule 10. Insecure Connection
    # Logic: Các user quan trọng (như root) kết nối qua TCP/IP (thường không an toàn nếu không có SSL) thay vì Socket
    idx_insecure = []
    restricted_users = signatures.get('restricted_connection_users', ['root', 'admin'])
    if 'connection_type' in df.columns:
        insecure_conns = df[
            (df['user'].isin(restricted_users)) &
            (df['connection_type'] == 'TCP/IP')  # Socket thường là 'Localhost via UNIX socket'
        ]
        idx_insecure.extend(insecure_conns.index.tolist())
    if idx_insecure: anomalies['Insecure Connection'] = list(set(idx_insecure))
    
    return anomalies

# ============================================================================================================================================================
# 3. NHÓM TECHNICAL ATTACKS (Tấn công kỹ thuật)
# Bao gồm: SQLi, DoS, High CPU Usage, Scan Efficiency, Config Change, Entropy, Client Mismatch, Disk Temp Table Abuse,
#          Excessive Locking, Suspicious Comment, Warning Flooding, Password Hash Attack, JSON Data Extraction
# ============================================================================================================================================================
def check_technical_attacks(df, rule_config):
    """Nhóm 3: Technical Attacks"""
    anomalies = {}
    thresholds = rule_config.get('thresholds', {})
    signatures = rule_config.get('signatures', {})
    
    # Rule 11. SQL Injection
    idx_sqli = []
    sqli_kws = signatures.get('sqli_keywords', [])
    if sqli_kws:
        pattern_sqli = "|".join(re.escape(k) for k in sqli_kws)
        sqli_logs = df[df['query'].str.contains(pattern_sqli, case=False, na=False)]
        idx_sqli.extend(sqli_logs.index.tolist())
    if idx_sqli: anomalies['SQL Injection'] = list(set(idx_sqli))
    
    # Rule 12. DoS / Resource Exhaustion
    idx_dos = []
    max_time = thresholds.get('execution_time_limit_ms', 5000)
    idx_dos.extend(df[df['execution_time_ms'] > max_time].index.tolist())
    if idx_dos: anomalies['DoS / Resource Exhaustion'] = list(set(idx_dos))
    
    # Rule 13. High CPU Usage 
    idx_cpu = []
    max_cpu_time = thresholds.get('cpu_time_limit_ms', 1000) # 1s CPU
    if 'cpu_time_ms' in df.columns:
        high_cpu = df[df['cpu_time_ms'] > max_cpu_time]
        idx_cpu.extend(high_cpu.index.tolist())
    if idx_cpu: anomalies['High CPU Usage'] = list(set(idx_cpu))
    
    # Rule 14. Scan Efficiency
    idx_scan = []
    min_eff = thresholds.get('scan_efficiency_min', 0.01)
    min_rows = thresholds.get('scan_efficiency_min_rows', 1000)
    inefficient = df[
        (df['rows_examined'] > min_rows) & 
        (df['rows_returned'] < (df['rows_examined'] * min_eff))
    ]
    idx_scan.extend(inefficient.index.tolist())
    if idx_scan: anomalies['Scan Efficiency'] = list(set(idx_scan))
    
    # Rule 15. Config Change
    idx_config = []
    config_change = df[
        df['query'].str.contains("SET GLOBAL|general_log", regex=True, case=False, na=False)
    ]
    idx_config.extend(config_change.index.tolist())
    if idx_config: anomalies['Config Change'] = list(set(idx_config))
    
    # Rule 16. High Entropy Queries
    idx_entropy = []
    max_entropy = thresholds.get('max_query_entropy', 4.8)
    if 'query_entropy' in df.columns:
        high_entropy = df[df['query_entropy'] > max_entropy]
        idx_entropy.extend(high_entropy.index.tolist())
    if idx_entropy: anomalies['High Entropy Query'] = list(set(idx_entropy))

    # Rule 17. Client/OS Mismatch
    # Phát hiện tool tấn công trong blacklist (sqlmap, nmap...)
    idx_client = []
    whitelist = signatures.get('allowed_programs', [])
    if 'program_name' in df.columns and not whitelist:
        pattern_bad = "|".join(re.escape(p) for p in whitelist)
        bad_clients = df[df['program_name'].str.contains(pattern_bad, case=False, na=False)]
        idx_client.extend(bad_clients.index.tolist())
    if idx_client: anomalies['Client Mismatch'] = list(set(idx_client))

    # Rule 18. Disk Temp Table Abuse
    # Logic: Query tạo bảng tạm trên ổ cứng (gây chậm)
    idx_disk_abuse = []
    if 'created_tmp_disk_tables' in df.columns:
        disk_abusers = df[df['created_tmp_disk_tables'] > 0]
        idx_disk_abuse.extend(disk_abusers.index.tolist())
    if idx_disk_abuse: anomalies['Disk Temp Table Abuse'] = list(set(idx_disk_abuse))

    # Rule 19. Index Evasion / Full Join
    # Logic: Không dùng index hoặc Full Join
    idx_no_index = []
    if 'no_index_used' in df.columns and 'select_full_join' in df.columns:
        bad_scans = df[(df['no_index_used'] == 1) | (df['select_full_join'] == 1)]
        # Chỉ bắt nếu quét nhiều dòng để tránh false positive cho bảng nhỏ
        bad_scans = bad_scans[bad_scans['rows_examined'] > 100] 
        idx_no_index.extend(bad_scans.index.tolist())
    if idx_no_index: anomalies['Index Evasion'] = list(set(idx_no_index))

    # Rule 20. Excessive Locking
    # Logic: Lock quá lâu
    idx_locking = []
    lock_limit = thresholds.get('lock_time_limit_ms', 500) # 0.5s
    if 'lock_time_ms' in df.columns:
        long_locks = df[df['lock_time_ms'] > lock_limit]
        idx_locking.extend(long_locks.index.tolist())
    if idx_locking: anomalies['Excessive Locking'] = list(set(idx_locking))

    # Rule 21. Suspicious Comment
    # Logic: Có comment nhưng không phải job hệ thống
    idx_comments = []
    if 'has_comment' in df.columns and 'is_system_table' in df.columns:
        suspicious_cmts = df[
            (df['has_comment'] == 1) & 
            (df['is_system_table'] == 0)
        ]
        idx_comments.extend(suspicious_cmts.index.tolist())
    if idx_comments: anomalies['Suspicious Comment'] = list(set(idx_comments))

    # Rule 22. Warning Flooding
    # Logic: Query sinh ra quá nhiều warning
    idx_warnings = []
    warn_thresh = thresholds.get('warning_count_threshold', 5)
    if 'warning_count' in df.columns:
        warn_floods = df[df['warning_count'] > warn_thresh]
        idx_warnings.extend(warn_floods.index.tolist())
    if idx_warnings: anomalies['Warning Flooding'] = list(set(idx_warnings))

    # Rule 23. Password Hash Attack
    # Logic: Cố tình select chuỗi xác thực
    idx_pass_attack = []
    pass_attack = df[df['query'].str.contains(r'authentication_string|password_expired', regex=True, case=False, na=False)]
    idx_pass_attack.extend(pass_attack.index.tolist())
    if idx_pass_attack: anomalies['Password Hash Attack'] = list(set(idx_pass_attack))

    # Rule 24. JSON Data Extraction
    # Logic: Dùng hàm JSON extract
    idx_json = []
    json_extract = df[df['query'].str.contains(r'JSON_EXTRACT|JSON_UNQUOTE|->>', regex=True, case=False, na=False)]
    idx_json.extend(json_extract.index.tolist())
    if idx_json: anomalies['JSON Data Extraction'] = list(set(idx_json))

    return anomalies

# ============================================================================================================================================================
# 4. NHÓM DATA DESTRUCTION (Phá hoại dữ liệu)
# Bao gồm: Mass Delete, Old Data, Large Dump, Audit Log Manipulation, Hidden View / Rename
# ============================================================================================================================================================
def check_data_destruction(df, rule_config):
    """Nhóm 4: Data Destruction"""
    anomalies = {}
    thresholds = rule_config.get('thresholds', {})
    signatures = rule_config.get('signatures', {})
    
    # Rule 25. Mass Deletion (Xóa hàng loạt)
    idx_delete = []
    limit_rows = thresholds.get('mass_deletion_rows', 500)
    mass_delete = df[
        (df['event_name'].isin(['statement/sql/delete', 'statement/sql/drop'])) &
        (df['rows_affected'] > limit_rows)
    ]
    idx_delete.extend(mass_delete.index.tolist())
    if idx_delete: anomalies['Mass Deletion'] = list(set(idx_delete))
    
    # Rule 26. Old Data Modification (Sửa dữ liệu cũ)
    idx_old_data = []
    old_data_access = df[df['query'].str.contains("2019|2020|2021|2022|2023|2024", regex=True, na=False)]
    idx_old_data.extend(old_data_access.index.tolist())
    if idx_old_data: anomalies['Old Data Modification'] = list(set(idx_old_data))

    # Rule 27. Large Data Dump 
    # Logic: Select bảng quan trọng mà trả về quá nhiều dòng
    idx_dump = []
    large_dump_tables = signatures.get('large_dump_tables', [])
    if large_dump_tables:
        pattern_dump = "|".join(re.escape(k) for k in large_dump_tables)
        # Điều kiện: Query chứa tên bảng quan trọng VÀ trả về > 1000 dòng
        dump_logs = df[
            (df['query'].str.contains(pattern_dump, case=False, na=False)) &
            (df['rows_returned'] > 1000)
        ]
        idx_dump.extend(dump_logs.index.tolist())
    if idx_dump: anomalies['Large Data Dump'] = list(set(idx_dump))

    # Rule 28. Audit Log Manipulation
    # Logic: Update/Delete trên bảng log/audit/history
    idx_audit = []
    # Regex tìm tên bảng chứa chữ log, audit, history
    audit_tables_pattern = r'(?:general_log|audit_log|slow_log|history)'
    audit_manipulation = df[
        (df['query'].str.contains(audit_tables_pattern, regex=True, case=False, na=False)) &
        (df['event_name'].isin(['statement/sql/delete', 'statement/sql/update', 'statement/sql/truncate']))
    ]
    idx_audit.extend(audit_manipulation.index.tolist())
    if idx_audit: anomalies['Audit Log Manipulation'] = list(set(idx_audit))

    # Rule 29. Hidden View / Rename
    # Logic: Đổi tên bảng hoặc tạo View
    idx_obfuscation = []
    # Tìm lệnh RENAME TABLE hoặc CREATE VIEW
    obfuscation_cmds = df[df['query'].str.contains(r'RENAME\s+TABLE|CREATE\s+VIEW', regex=True, case=False, na=False)]
    idx_obfuscation.extend(obfuscation_cmds.index.tolist())
    if idx_obfuscation: anomalies['Hidden View / Rename'] = list(set(idx_obfuscation))
    
    return anomalies

# ==============================================================================
# 5. RULE 30: MULTI-TABLE ACCESS 
# ==============================================================================
def check_multi_table_anomalies(df, rule_config):
    """
    Rule 30: Multi-table Access
    """
    anomalies = []
    thresholds = rule_config.get('thresholds', {})
    
    # Lấy tham số
    window_min = thresholds.get('multi_table_window_minutes', 4)
    min_tables = thresholds.get('multi_table_min_count', 3)
    
    def extract_tables_list(q):
        if not isinstance(q, str) or not q.strip(): return []
        
        # 1. Ưu tiên SQLGlot
        if SQLGLOT_AVAILABLE:
            try:
                tables = get_tables_with_sqlglot(q)
                if tables: return tables
            except: pass

        # 2. Fallback Regex
        try:
            pattern = r'(?:\bFROM\b|\bJOIN\b|\bUPDATE\b|\bINTO\b)\s+(?!\()([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)'
            matches = re.findall(pattern, q, re.IGNORECASE)
            clean_tables = []
            for m in matches:
                clean_name = m.replace('`', '').replace("'", "").replace('"', "").lower()
                clean_tables.append(clean_name)
            return clean_tables
        except:
            return []

    target_events = ['SELECT', 'SHOW', 'DESCRIBE']
    mask = df['query'].str.contains('|'.join(target_events), case=False, na=False)
    df_target = df[mask].copy()
    
    if df_target.empty: return {} # <-- SỬA: Trả về dict rỗng thay vì list rỗng

    df_target['accessed_tables_list'] = df_target['query'].apply(extract_tables_list)
    df_target = df_target[df_target['accessed_tables_list'].map(len) > 0]
    df_target = df_target.sort_values('timestamp')
    
    grouped = df_target.groupby(['user', pd.Grouper(key='timestamp', freq=f'{window_min}Min')], observed=False)

    for (user, time_window), group in grouped:
        unique_tables_in_window = set()
        for tbl_list in group['accessed_tables_list']:
            unique_tables_in_window.update(tbl_list)
            
        if len(unique_tables_in_window) > min_tables:
            anomalies.extend(group.index.tolist())

    # --- [QUAN TRỌNG] SỬA ĐOẠN RETURN TẠI ĐÂY ---
    if anomalies:
        # Trả về Dictionary: { 'Tên Rule': [Danh sách Index] }
        return {'Multi-Table Access': list(set(anomalies))}
    
    return {}

# ==============================================================================
# 5. RULE 31: BEHAVIORAL PROFILE
# ==============================================================================
def update_behavior_redis(redis_client, df_logs):
    """
    Học thói quen: Cập nhật tần suất hoạt động của User theo giờ vào Redis.
    Sử dụng Pipeline để tăng tốc độ ghi.
    """
    if df_logs.empty or redis_client is None:
        return

    try:
        pipe = redis_client.pipeline()
        
        # Chỉ quan tâm các cột cần thiết
        for _, row in df_logs.iterrows():
            user = row.get('user')
            timestamp = row.get('timestamp')
            
            if user and timestamp:
                # Key: uba:profile:thanh.nguyen
                key = f"{REDIS_PROFILE_KEY_PREFIX}{user}"
                
                # Field: Giờ (0-23)
                hour = str(timestamp.hour)
                
                # Tăng biến đếm số lần xuất hiện tại giờ này thêm 1
                pipe.hincrby(key, hour, 1)
                
                # Gia hạn thời gian sống cho Key (nếu cần)
                if PROFILE_TTL_SECONDS:
                    pipe.expire(key, PROFILE_TTL_SECONDS)
        
        # Thực thi hàng loạt lệnh
        pipe.execute()
        logging.info(f"Updated behavior profiles for {len(df_logs)} logs.")
        
    except Exception as e:
        logging.error(f"Error updating Redis profile: {e}")
        
def check_behavior_redis(redis_client, df_logs, min_threshold=5):
    """
    Kiểm tra bất thường: So sánh log hiện tại với lịch sử trong Redis.
    min_threshold: Số lần xuất hiện tối thiểu để coi là bình thường.
    """
    anomalies_indices = []
    
    if df_logs.empty or redis_client is None:
        return anomalies_indices

    try:
        for idx, row in df_logs.iterrows():
            user = row.get('user')
            timestamp = row.get('timestamp')
            
            if user and timestamp:
                key = f"{REDIS_PROFILE_KEY_PREFIX}{user}"
                hour = str(timestamp.hour)
                
                # Lấy số lần đã xuất hiện
                count_bytes = redis_client.hget(key, hour)
                
                count = 0
                if count_bytes:
                    try:
                        count = int(count_bytes)
                    except:
                        count = 0
                
                # LOGIC PHÁT HIỆN: Dùng tham số min_threshold truyền vào
                if count < min_threshold:
                    anomalies_indices.append(idx)

    except Exception as e:
        logging.error(f"Error checking Redis profile: {e}")
        return []

    return anomalies_indices

# ==============================================================================
# REDIS CONFIGURATION HELPER
# ==============================================================================

def configure_redis_for_reliability(redis_client: Redis) -> bool:
    """
    Configure Redis for better reliability and handle MISCONF errors.
    
    Returns:
        bool: True if configuration was successful, False otherwise
    """
    try:
        # Strategy 1: Use AOF instead of RDB for better persistence
        try:
            redis_client.config_set("save", "")  # Disable RDB snapshots
            logging.info("✅ Redis: Disabled RDB snapshots")
            
            redis_client.config_set("appendonly", "yes")  # Enable AOF
            logging.info("✅ Redis: Enabled AOF persistence")
            
            return True
            
        except Exception as config_error:
            logging.warning(f"⚠️ Could not configure Redis persistence: {config_error}")
            
            # Strategy 2: Fallback - disable the error check
            try:
                redis_client.config_set("stop-writes-on-bgsave-error", "no")
                logging.warning("⚠️ Redis: Disabled RDB error checking (fallback)")
                return True
                
            except Exception as fallback_error:
                logging.warning(f"⚠️ Redis: Could not modify config: {fallback_error}")
                return False
                
    except Exception as e:
        logging.error(f"❌ Redis configuration failed: {e}")
        return False

def handle_redis_misconf_error(error_msg: str) -> str:
    """
    Provide helpful error message and suggestions for MISCONF errors.
    
    Args:
        error_msg: The Redis error message
        
    Returns:
        str: Helpful suggestion message
    """
    if "MISCONF" in error_msg:
        return (
            "💡 Redis MISCONF Error Solutions:\n"
            "   1. Check disk space: df -h\n"
            "   2. Check Redis logs: tail -f /var/log/redis/redis-server.log\n"
            "   3. Fix permissions: sudo chown redis:redis /var/lib/redis\n"
            "   4. Disable RDB: redis-cli CONFIG SET save ''\n"
            "   5. Enable AOF: redis-cli CONFIG SET appendonly yes"
        )
    return "Check Redis server status and logs"