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


def is_potential_large_dump(row, large_tables_list):
    """
    Kiểm tra xem một truy vấn có khả năng là một hành vi kết xuất (dump) dữ liệu lớn không.
    Nó tìm kiếm các mẫu truy vấn nguy hiểm.
    """
    # Chuyển câu query về chữ thường để việc so sánh chuỗi không phân biệt hoa/thường.
    query = str(row['query']).lower()
    
    # Lặp qua danh sách các bảng được coi là lớn.
    for table in large_tables_list:
        table_lower = table.lower()
        # Kiểm tra mẫu "SELECT *" từ một bảng lớn mà không có điều kiện lọc `WHERE` hoặc `LIMIT`.
        if (f"from {table_lower}" in query or f"from `{table_lower}`" in query) and "select *" in query:
            if "where" not in query and "limit" not in query:
                return True # Nếu tìm thấy, trả về True ngay lập tức.
    
    # Kiểm tra mẫu lệnh xuất dữ liệu thẳng ra file.
    if "select into outfile" in query:
        return True
        
    # Nếu không có mẫu nào khớp, trả về False.
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
        if table.lower() in sensitive_tables_lower:
            accessed_sensitive.append(table)
            
    # Trả về một tuple: (True/False, danh_sách_bảng_nhạy_cảm_bị_truy_cập).
    return bool(accessed_sensitive), accessed_sensitive


# def analyze_sensitive_access(row, sensitive_tables_list, allowed_users_list,
#                              safe_start, safe_end, safe_days):
#     """
#     Phân tích một truy cập vào bảng nhạy cảm để tìm các vi phạm chính sách.
#     Một truy cập bị coi là vi phạm nếu nó truy cập bảng nhạy cảm VÀ không thỏa mãn
#     điều kiện về người dùng HOẶC thời gian.
#     """
#     # Lấy các thông tin cần thiết từ dòng dữ liệu.
#     accessed_tables = row['accessed_tables']
#     user = row['user']
#     timestamp = row['timestamp']
#
#     # Kiểm tra xem có bảng nhạy cảm nào bị truy cập không.
#     is_sensitive_hit, specific_sensitive_tables = is_sensitive_table_accessed(accessed_tables, sensitive_tables_list)
#     # Nếu không, không cần phân tích thêm, trả về None.
#     if not is_sensitive_hit:
#         return None
#
#     # Kiểm tra các điều kiện chính sách.
#     user_is_allowed = (not pd.isna(user) and user in allowed_users_list)
#     is_outside_safe_hours = not (safe_start <= timestamp.hour < safe_end and timestamp.weekday() in safe_days)
#
#     # Thu thập các lý do vi phạm.
#     anomaly_reasons = []
#     if not user_is_allowed:
#         anomaly_reasons.append(f"User '{user if not pd.isna(user) else 'N/A'}' không được phép.")
#     if is_outside_safe_hours:
#         anomaly_reasons.append("Truy cập ngoài giờ làm việc an toàn.")
#
#     # Nếu có bất kỳ lý do vi phạm nào, trả về một dictionary chứa thông tin chi tiết.
#     if anomaly_reasons:
#         return {
#             "violation_reason": ", ".join(anomaly_reasons),
#             "accessed_sensitive_tables_list": specific_sensitive_tables
#         }
#
#     # Nếu không có vi phạm nào, trả về None.
#     return None
def analyze_sensitive_access(row, sensitive_tables_list, allowed_users_list,
                             safe_start, safe_end, safe_days):
    """
    Phân tích truy cập bảng nhạy cảm với logic đã được sửa đổi.
    LOGIC MỚI: Nếu một user được phép, hàm sẽ trả về None ngay lập tức
    """
    # Bước 1: Trích xuất thông tin cần thiết từ dòng dữ liệu
    accessed_tables = row.get('accessed_tables', [])
    user = row.get('user')
    timestamp = row.get('timestamp')

    # Bước 2: Kiểm tra xem có truy cập vào bảng nhạy cảm không
    is_sensitive_hit, specific_sensitive_tables = is_sensitive_table_accessed(accessed_tables, sensitive_tables_list)
    # Nếu không, không phải là bất thường -> kết thúc sớm
    if not is_sensitive_hit:
        return None

        # --- THAY ĐỔI LOGIC CHÍNH NẰM Ở ĐÂY ---
    # Bước 3: Kiểm tra xem user có nằm trong danh sách được phép không
    user_is_allowed = (not pd.isna(user) and user in allowed_users_list)

    # Nếu user đã được phép, coi như đây là hành động hợp lệ và không cần kiểm tra thêm.
    if user_is_allowed:
        return None

    # Bước 4: Nếu user KHÔNG được phép, tiếp tục kiểm tra các điều kiện khác
    # Chỉ những user không thuộc danh sách "allowed" mới bị xét đến các quy tắc phụ.
    is_outside_safe_hours = not (safe_start <= timestamp.hour < safe_end and timestamp.weekday() in safe_days)

    # Xây dựng lý do bất thường
    anomaly_reasons = []
    # Lý do chính luôn là vì user không được phép
    anomaly_reasons.append(f"User '{user if not pd.isna(user) else 'N/A'}' không có trong danh sách được phép.")

    # Thêm lý do phụ về thời gian nếu có
    if is_outside_safe_hours:
        anomaly_reasons.append("Truy cập ngoài giờ làm việc an toàn..")

    # Vì user chắc chắn không được phép ở bước này, `anomaly_reasons` sẽ không bao giờ rỗng.
    # Trả về kết quả bất thường.
    return {
        "violation_reason": " ".join(anomaly_reasons),
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
