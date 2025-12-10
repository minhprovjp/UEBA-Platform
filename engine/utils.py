import pandas as pd
import os
import sqlglot
import sqlglot.expressions as exp
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


# ==============================================================================
#   CÁC HÀM HỖ TRỢ FEATURE ENGINEERING VÀ FEEDBACK
# ==============================================================================
# Các hàm trong mục này hỗ trợ việc trích xuất đặc trưng cho AI và xử lý feedback.

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

        
def update_config_file(new_configs: dict):
    """
    Đọc file config.py, tìm và thay thế các giá trị mặc định, và ghi đè lại file.

    Args:
        new_configs (dict): Một dictionary chứa các giá trị mới cần cập nhật.

    Returns:
        tuple: (bool, str) - (Thành công/Thất bại, Thông báo)
    """
    config_path = 'config.py' # Đường dẫn đến file config.py trong cùng thư mục
    try:
        # Đọc tất cả các dòng của file vào một danh sách
        with open(config_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        new_lines = []
        # Lặp qua từng dòng để xử lý
        for line in lines:
            # Dùng regex để tìm các dòng gán giá trị mặc định
            # Ví dụ: LATE_NIGHT_START_TIME_DEFAULT = time(0, 0)
            match = re.match(r'^([A-Z_]+_DEFAULT)\s*=\s*(.*)', line)
            
            # Nếu dòng hiện tại là một dòng gán giá trị mặc định
            if match:
                var_name = match.group(1) # Lấy tên biến, ví dụ: LATE_NIGHT_START_TIME_DEFAULT
                
                # Nếu biến này có trong danh sách cần cập nhật
                if var_name in new_configs:
                    new_value = new_configs[var_name]
                    
                    # Định dạng lại giá trị mới thành một chuỗi Python hợp lệ
                    if isinstance(new_value, str):
                        # Chuỗi phải được đặt trong dấu ngoặc kép
                        new_line = f'{var_name} = r"{new_value}"\n' if '\\' in new_value else f'{var_name} = "{new_value}"\n'
                    elif isinstance(new_value, dt_time):
                        # Đối tượng time cần được tái tạo bằng time(...)
                        new_line = f"{var_name} = dt_time({new_value.hour}, {new_value.minute}, {new_value.second})\n"
                    elif isinstance(new_value, list):
                        # str(my_list) sẽ tạo ra một chuỗi như "['item1', 'item2']" trên một dòng.
                        new_line = f"{var_name} = {str(new_value)}\n"
                    else:
                        # Các kiểu dữ liệu khác (int, float)
                        new_line = f'{var_name} = {new_value}\n'
                    
                    # Thêm dòng mới đã được định dạng vào danh sách `new_lines`
                    new_lines.append(new_line)
                    print(f"Đang cập nhật {var_name}...")
                else:
                    # Nếu biến này không cần cập nhật, giữ nguyên dòng cũ
                    new_lines.append(line)
            else:
                # Giữ nguyên các dòng không phải là dòng gán giá trị (ví dụ: comment, import,...)
                new_lines.append(line)

        # Ghi đè lại toàn bộ file config.py với nội dung mới
        # Chế độ 'w' (write) sẽ tự động xóa nội dung cũ trước khi ghi
        with open(config_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
            
        return True, "Lưu cấu hình mặc định thành công!"

    except Exception as e:
        # Bắt lỗi và in ra để dễ dàng gỡ lỗi
        import traceback
        traceback.print_exc()
        return False, f"Lỗi khi lưu cấu hình: {e}"


def save_logs_to_parquet(records: list, source_dbms: str) -> int:
    if not records:
        return 0
    try:
        df = pd.DataFrame(records)
        if 'source_dbms' not in df.columns:
            df['source_dbms'] = source_dbms
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

        os.makedirs(STAGING_DATA_DIR, exist_ok=True)  # <-- thêm dòng này

        filename = f"{source_dbms}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
        file_path = os.path.join(STAGING_DATA_DIR, filename)
        df.to_parquet(file_path, engine='pyarrow', index=False)
        logging.info(f"Đã lưu {len(df)} bản ghi từ '{source_dbms}' vào file: {filename}")
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
                            <th>Anomaly Type</th>
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

# ==============================================================================
# 2. NHÓM INSIDER THREATS (Mối đe dọa nội bộ)
# Bao gồm: Service Account, Admin Privilege Escalation, Sensitive Access, Late Night, Ghost Account
# ==============================================================================
def check_insider_threats(df, rule_config):
    """Nhóm 2: Insider Threat"""
    anomalies = {}
    service_accounts = rule_config.get('service_accounts', {})
    signatures = rule_config.get('signatures', {})
    settings = rule_config.get('settings', {})
    hr_users = set(signatures.get('hr_authorized_users', []))
    
    # Rule 4. Service Account Misuse
    idx_service = []
    for user, config in service_accounts.items():
        user_logs = df[df['user'] == user]
        if user_logs.empty: continue
        invalid_hour = user_logs[~user_logs['timestamp'].dt.hour.isin(config.get('allowed_hours', []))]
        idx_service.extend(invalid_hour.index.tolist())  
        invalid_ip = user_logs[~user_logs['client_ip'].isin(config.get('allowed_ips', []))]
        idx_service.extend(invalid_ip.index.tolist())      
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
    # Hàm check logic
    def is_violation(row):
        # Nếu user nằm trong whitelist thì bỏ qua
        if row['user'] in allowed_users: return False
        
        # Check nếu query chứa bảng nhạy cảm
        for tbl in sensitive_tables:
            if tbl in str(row['query']):
                return True # Vi phạm: User không được phép truy cập bảng này
        return False
    if sensitive_tables:
        sensitive_violation = df[df.apply(is_violation, axis=1)]
        idx_sensitive.extend(sensitive_violation.index.tolist())   
    if idx_sensitive: anomalies['Sensitive Table Access'] = list(set(idx_sensitive))

    # Rule 7. Late Night Query 
    # Logic: Truy cập ngoài giờ hành chính (22h - 5h sáng)
    idx_latenight = []
    try:
        s_str = settings.get('late_night_start', '22:00:00')
        e_str = settings.get('late_night_end', '05:00:00')
        start_time = dt_time.fromisoformat(s_str)
        end_time = dt_time.fromisoformat(e_str)
        def check_time(ts):
            t = ts.time()
            if start_time <= end_time: return start_time <= t <= end_time
            else: return start_time <= t or t <= end_time # Qua đêm (ví dụ 22h -> 5h)
        late_night_logs = df[df['timestamp'].apply(check_time)]
        # Có thể lọc bớt các user chạy job đêm nếu cần
        idx_latenight.extend(late_night_logs.index.tolist())
    except:
        pass 
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
    
    return anomalies

# ==============================================================================
# 3. NHÓM TECHNICAL ATTACKS (Tấn công kỹ thuật)
# Bao gồm: SQLi, DoS, High CPU Usage, Scan Efficiency, Config Change, Entropy, Client Mismatch
# ==============================================================================
def check_technical_attacks(df, rule_config):
    """Nhóm 3: Technical Attacks"""
    anomalies = {}
    thresholds = rule_config.get('thresholds', {})
    signatures = rule_config.get('signatures', {})
    
    # Rule 9. SQL Injection
    idx_sqli = []
    sqli_kws = signatures.get('sqli_keywords', [])
    if sqli_kws:
        pattern_sqli = "|".join(re.escape(k) for k in sqli_kws)
        sqli_logs = df[df['query'].str.contains(pattern_sqli, case=False, na=False)]
        idx_sqli.extend(sqli_logs.index.tolist())
    if idx_sqli: anomalies['SQL Injection'] = list(set(idx_sqli))
    
    # Rule 10. DoS / Resource Exhaustion
    idx_dos = []
    max_time = thresholds.get('execution_time_limit_ms', 5000)
    idx_dos.extend(df[df['execution_time_ms'] > max_time].index.tolist())
    if idx_dos: anomalies['DoS / Resource Exhaustion'] = list(set(idx_dos))
    
    # Rule 11. High CPU Usage 
    idx_cpu = []
    max_cpu_time = thresholds.get('cpu_time_limit_ms', 1000) # 1s CPU
    if 'cpu_time_ms' in df.columns:
        high_cpu = df[df['cpu_time_ms'] > max_cpu_time]
        idx_cpu.extend(high_cpu.index.tolist())
    if idx_cpu: anomalies['High CPU Usage'] = list(set(idx_cpu))
    
    # Rule 12. Scan Efficiency
    idx_scan = []
    min_eff = thresholds.get('scan_efficiency_min', 0.01)
    min_rows = thresholds.get('scan_efficiency_min_rows', 1000)
    inefficient = df[
        (df['rows_examined'] > min_rows) & 
        (df['rows_returned'] < (df['rows_examined'] * min_eff))
    ]
    idx_scan.extend(inefficient.index.tolist())
    if idx_scan: anomalies['Scan Efficiency'] = list(set(idx_scan))
    
    # Rule 13. Config Change
    idx_config = []
    config_change = df[
        df['query'].str.contains("SET GLOBAL|general_log", regex=True, case=False, na=False)
    ]
    idx_config.extend(config_change.index.tolist())
    if idx_config: anomalies['Config Change'] = list(set(idx_config))
    
    # Rule 14. High Entropy Queries
    idx_entropy = []
    max_entropy = thresholds.get('max_query_entropy', 4.8)
    if 'query_entropy' in df.columns:
        high_entropy = df[df['query_entropy'] > max_entropy]
        idx_entropy.extend(high_entropy.index.tolist())
    if idx_entropy: anomalies['High Entropy Query'] = list(set(idx_entropy))

    # Rule 15. Client/OS Mismatch
    # Phát hiện tool tấn công trong blacklist (sqlmap, nmap...)
    idx_client = []
    whitelist = signatures.get('allowed_programs', [])
    if 'program_name' in df.columns and not whitelist:
        pattern_bad = "|".join(re.escape(p) for p in whitelist)
        bad_clients = df[df['program_name'].str.contains(pattern_bad, case=False, na=False)]
        idx_client.extend(bad_clients.index.tolist())
    if idx_client: anomalies['Client Mismatch'] = list(set(idx_client))

    return anomalies

# ==============================================================================
# 4. NHÓM DATA DESTRUCTION (Phá hoại dữ liệu)
# Bao gồm: Mass Delete, Old Data, Large Dump
# ==============================================================================
def check_data_destruction(df, rule_config):
    """Nhóm 4: Data Destruction"""
    anomalies = {}
    thresholds = rule_config.get('thresholds', {})
    signatures = rule_config.get('signatures', {})
    
    # Rule 16. Mass Deletion (Xóa hàng loạt)
    idx_delete = []
    limit_rows = thresholds.get('mass_deletion_rows', 500)
    mass_delete = df[
        (df['event_name'].isin(['statement/sql/delete', 'statement/sql/drop'])) &
        (df['rows_affected'] > limit_rows)
    ]
    idx_delete.extend(mass_delete.index.tolist())
    if idx_delete: anomalies['Mass Deletion'] = list(set(idx_delete))
    
    # Rule 17. Old Data Modification (Sửa dữ liệu cũ)
    idx_old_data = []
    old_data_access = df[df['query'].str.contains("2019|2020|2021|2022|2023|2024", regex=True, na=False)]
    idx_old_data.extend(old_data_access.index.tolist())
    if idx_old_data: anomalies['Old Data Modification'] = list(set(idx_old_data))

    # Rule 18. Large Data Dump 
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
    
    return anomalies

# ==============================================================================
# 5. RULE RIÊNG: MULTI-TABLE ACCESS 
# ==============================================================================
def check_multi_table_anomalies(df, rule_config):
    """
    Rule 19: Multi-table Access
    """
    anomalies = []
    thresholds = rule_config.get('thresholds', {})
    
    # Lấy tham số (Mặc định 5 phút, 3 bảng)
    window_min = thresholds.get('multi_table_window_minutes', 5)
    min_tables = thresholds.get('multi_table_min_count', 3)
    
    # Hàm tách tên bảng từ câu lệnh SQL
    def extract_table_name(q):
        if not isinstance(q, str): return None
        # Regex tìm từ sau FROM hoặc JOIN
        match = re.search(r'\b(?:FROM|JOIN)\s+[`\'"]?([a-zA-Z0-9_.]+)[`\'"]?', q, re.IGNORECASE)
        if match:
            # Loại bỏ ký tự quote thừa nếu có (ví dụ: `sales_db`.`orders` -> sales_db.orders)
            return match.group(1).replace('`', '').replace("'", "").replace('"', "")
        return None

    # Chỉ xét các câu lệnh SELECT
    df_select = df[df['query'].str.contains('SELECT', case=False, na=False)].copy()
    if df_select.empty: return []

    # Tạo cột tên bảng tạm thời
    df_select['extracted_table'] = df_select['query'].apply(extract_table_name)
    # Lọc bỏ các giá trị None hoặc rỗng
    df_select = df_select[df_select['extracted_table'].notna() & (df_select['extracted_table'] != '')]

    # Group theo User và Khung giờ
    # Sort trước để Grouper chạy đúng
    df_select = df_select.sort_values('timestamp')
    grouped = df_select.groupby(['user', pd.Grouper(key='timestamp', freq=f'{window_min}Min')], observed=False)

    for (user, time_window), group in grouped:
        unique_tables = group['extracted_table'].nunique()
        if unique_tables > min_tables:
            # --- ĐIỂM SỬA QUAN TRỌNG: Trả về toàn bộ index của nhóm này ---
            anomalies.extend(group.index.tolist())

    return list(set(anomalies))

# ==============================================================================
# 5. RULE RIÊNG: BEHAVIORAL PROFILE
# ==============================================================================
def check_unusual_user_activity_time(row, profiles):
    """
    Rule 20: Unusual User Time
    """
    user = row.get('user')
    if user not in profiles: return None
    
    hour = row['timestamp'].hour
    p = profiles[user]
    # Giả sử profile lưu 'active_start' và 'active_end'
    # Nếu giờ nằm ngoài khoảng [start - 1, end + 1] thì báo
    if hour < (p['active_start'] - 1) or hour > (p['active_end'] + 1):
        return f"User {user} active at {hour}h (Profile: {p['active_start']}-{p['active_end']})"
    return None

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