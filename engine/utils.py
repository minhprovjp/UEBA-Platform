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

# ==============================================================================
# I. CÁC HÀM HỖ TRỢ PHÂN TÍCH THEO LUẬT (RULE-BASED ANALYSIS)
# ==============================================================================
# Các hàm trong mục này cung cấp logic cốt lõi cho các luật phát hiện bất thường.

def is_late_night_query(timestamp_obj, start_time_rule, end_time_rule):
    """
    Kiểm tra xem một truy vấn có được thực hiện vào khung giờ "đêm khuya" không.
    Hàm này xử lý được cả trường hợp khung giờ vượt qua nửa đêm (ví dụ: 22:00 - 05:00).
    """
    # Lấy ra phần thời gian (giờ:phút:giây) từ đối tượng datetime đầy đủ.
    query_time = timestamp_obj.time()
    
    # Nếu khung giờ nằm trọn trong một ngày (ví dụ: 01:00 - 05:00).
    if start_time_rule <= end_time_rule:
        return start_time_rule <= query_time < end_time_rule
    # Nếu khung giờ vượt qua nửa đêm (ví dụ: 22:00 - 05:00 sáng hôm sau).
    else:
        # Điều kiện là: thời gian lớn hơn giờ bắt đầu (22:00) HOẶC nhỏ hơn giờ kết thúc (05:00).
        return query_time >= start_time_rule or query_time < end_time_rule

def is_potential_large_dump(row, large_tables_list, threshold=1000):
    """
    Phát hiện hành vi dump dữ liệu lớn
    """
    # DÙNG .at ĐỂ LẤY GIÁ TRỊ SCALAR — AN TOÀN 100%
    try:
        rows_returned = int(row.at['rows_returned']) if pd.notna(row.at['rows_returned']) else 0
    except:
        rows_returned = 0

    try:
        query = str(row.at['query']).lower() if pd.notna(row.at['query']) else ""
    except:
        query = ""

    # Rule 1: Rõ ràng dump lớn
    if rows_returned > threshold:
        return True

    # Rule 2: INTO OUTFILE / DUMPFILE
    if "into outfile" in query or "into dumpfile" in query:
        return True

    # Rule 3: SELECT * + bảng lớn + không có WHERE
    if "select *" in query:
        has_where = "where" in query
        accesses_large = any(table.lower() in query for table in large_tables_list)
        if not has_where and accesses_large:
            return True

    return False

def is_sensitive_table_accessed(accessed_tables_list, sensitive_tables_list):
    """
    Kiểm tra xem danh sách các bảng bị truy cập có chứa bất kỳ bảng nhạy cảm nào không.
    """
    # Trả về False nếu đầu vào không phải là một danh sách.
    if not isinstance(accessed_tables_list, list): 
        return False, []
        
    accessed_sensitive = [] # Danh sách để lưu các bảng nhạy cảm cụ thể đã bị truy cập.
    sensitive_tables_lower = [st.lower() for st in sensitive_tables_list] # Chuẩn hóa tên bảng nhạy cảm về chữ thường.
    
    # Lặp qua các bảng đã bị truy cập.
    for table in accessed_tables_list:
        table_lower = table.lower()
        # Lấy tên bảng (bỏ phần database nếu có)
        table_name_only = table_lower.split('.')[-1]
        
        # Kiểm tra cả tên đầy đủ và tên bảng đơn giản
        for sensitive in sensitive_tables_lower:
            sensitive_name_only = sensitive.split('.')[-1]
            # Match nếu:
            # 1. Tên đầy đủ khớp (mydb.users == mydb.users)
            # 2. Tên bảng khớp (users == users hoặc mydb.users ends with users)
            if table_lower == sensitive or table_name_only == sensitive_name_only:
                accessed_sensitive.append(table)
                break
            
    # Trả về một tuple: (True/False, danh_sách_bảng_nhạy_cảm_bị_truy_cập).
    return bool(accessed_sensitive), accessed_sensitive

# --- RULE MỚI (Giờ đã khả thi) ---
def is_data_sabotage(row, threshold=100):
    """
    Phát hiện các lệnh DELETE/UPDATE ảnh hưởng đến nhiều hàng.
    """
    query_lower = str(row.get('query', '')).lower()
    rows_affected = row.get('rows_affected', 0)
    
    if rows_affected > threshold and ('delete' in query_lower or 'update' in query_lower):
        # Kiểm tra thêm để loại bỏ các lệnh an toàn (ví dụ: không có WHERE)
        if 'where' not in query_lower:
            return True, "No WHERE clause"
        else:
            return True, f"High row count ({rows_affected})"
            
    return False, None

# --- RULE MỚI (Giờ đã khả thi) ---
def is_dos_attack(row, time_threshold_ms=15000): # 15 giây
    """
    Phát hiện các truy vấn chạy quá chậm (Tấn công DoS)
    """
    exec_time = row.get('execution_time_ms', 0)
    
    if exec_time > time_threshold_ms:
        return True, f"Query took {exec_time:.0f} ms"
    return False, None

def analyze_sensitive_access(row, sensitive_tables_list, allowed_users_list,
                             safe_start, safe_end, safe_days):

    accessed_tables = row.get('accessed_tables', [])
    user = row.get('user')
    timestamp = row.get('timestamp')

    if accessed_tables is None:
        accessed_tables = []
    
    # Nếu accessed_tables là chuỗi (do đọc từ CSV/DB lên), cần eval lại
    if isinstance(accessed_tables, str):
        try:
            import ast
            accessed_tables = ast.literal_eval(accessed_tables)
        except:
            accessed_tables = []

    is_sensitive_hit, specific_sensitive_tables = is_sensitive_table_accessed(accessed_tables, sensitive_tables_list)

    # Nếu không truy cập bảng nhạy cảm, bỏ qua
    if not is_sensitive_hit:
        return None

    # Kiểm tra các điều kiện
    user_is_allowed = (not pd.isna(user) and user in allowed_users_list)
    is_outside_safe_hours = not (safe_start <= timestamp.hour < safe_end and timestamp.weekday() in safe_days)

    # LỖ HỔNG LÀ Ở ĐÂY. Logic cũ của bạn là `if user_is_allowed: return None`.
    # Logic ĐÚNG là:

    # CHỈ COI LÀ HỢP LỆ (return None) NẾU
    # user được phép VÀ truy cập TRONG giờ an toàn.
    if user_is_allowed and not is_outside_safe_hours:
        return None

    # Tất cả các trường hợp khác đều là bất thường.
    # Xây dựng lý do:
    anomaly_reasons = []
    if not user_is_allowed:
        anomaly_reasons.append(f"User '{user if not pd.isna(user) else 'N/A'}' không có trong danh sách được phép.")
    if is_outside_safe_hours:
        anomaly_reasons.append("Truy cập ngoài giờ làm việc an toàn.")

    return {
        "reason": " ".join(anomaly_reasons) + f" [Tables: {', '.join(specific_sensitive_tables)}]",
        "accessed_sensitive_tables_list": specific_sensitive_tables
    }


def check_unusual_user_activity_time(row, user_profiles_dict):
    """Kiểm tra xem hoạt động của người dùng có nằm ngoài giờ hoạt động bình thường của họ không."""
    user = row['user']
    timestamp = row['timestamp']
    
    # Bỏ qua nếu không có thông tin user hoặc user chưa có hồ sơ hoạt động.
    if pd.isna(user) or user not in user_profiles_dict: 
        return None 
    
    # Lấy hồ sơ hoạt động của user.
    profile = user_profiles_dict[user]
    current_hour = timestamp.hour
    
    # Kiểm tra xem giờ hiện tại có nằm ngoài khoảng thời gian hoạt động bình thường không.
    if 'active_start' in profile and 'active_end' in profile:
        if not (profile['active_start'] <= current_hour < profile['active_end'] + 1):
            return f"Ngoài giờ Hoạt Động thường lệ của user ({profile['active_start']:02d}:00 - {profile['active_end']+1:02d}:00)"
            
    # Nếu nằm trong giờ bình thường, trả về None.
    return None

# --- RULE MỚI: PHÁT HIỆN HÀM NGHI VẤN (SQLi) ---
def is_suspicious_function_used(query: str):
    """
    Kiểm tra query có chứa các hàm đáng ngờ thường dùng trong SQLi/Exfiltration.
    Trả về (bool, str) - (Có đáng ngờ không, Tên hàm đáng ngờ)
    """
    if pd.isna(query):
        return False, None
    query_lower = str(query).lower()
    for func in SUSPICIOUS_FUNCTIONS:
        if f"{func}(" in query_lower:
            return True, func
    return False, None

# --- RULE MỚI: PHÁT HIỆN THAY ĐỔI QUYỀN (DCL/DDL) ---
def is_privilege_change(query: str):
    """
    Kiểm tra query có phải là lệnh thay đổi quyền hoặc user không.
    Trả về (bool, str) - (Có thay đổi không, Lệnh)
    """
    if pd.isna(query):
        return False, None
    query_lower = str(query).lower().strip()
    for cmd in PRIVILEGE_COMMANDS:
        if query_lower.startswith(cmd):
            return True, cmd
    return False, None

# ==============================================================================
# II. CÁC HÀM HỖ TRỢ FEATURE ENGINEERING VÀ FEEDBACK
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
# ACTIVE RESPONSE AUDIT LOGGER
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
                    {item['target_str']}
                </td>
                <td style="font-size: 13px; white-space: nowrap;">
                    {item['time_range']}
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