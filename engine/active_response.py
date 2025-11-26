# active_response.py

import subprocess
import os
import sys
# from utils import log_active_response_action
from typing import List, Tuple, Dict, Any


def _execute_mysql_query(db_config: Dict[str, Any],
                         sql_command: str,
                         fetch_results: bool = False) -> Tuple[bool, str, List[str]]:
    """
    Hàm helper nội bộ để thực thi một lệnh SQL qua mysql client.

    Args:
        db_config: Thông tin đăng nhập admin (host, port, user, password).
        sql_command: Lệnh SQL để thực thi.
        fetch_results: True nếu đây là lệnh SELECT và cần parse output.
                       False nếu đây là lệnh (ALTER, KILL)

    Returns:
        Tuple (bool: thành công, str: thông điệp lỗi/thành công, List[str]: kết quả (nếu fetch))
    """
    # 1. Chuẩn bị môi trường
    env = os.environ.copy()
    env["MYSQL_PWD"] = db_config.get("mysql_password")

    host = db_config.get("mysql_host")
    port = db_config.get("mysql_port")
    admin_user = db_config.get("mysql_user")

    if not all([host, port, admin_user, env["MYSQL_PWD"]]):
        return False, "Cấu hình MySQL Admin bị thiếu trong .env/config.py.", []

    # 2. Xây dựng lệnh command line
    command = [
        "mysql",  # Yêu cầu mysql client phải có trong PATH
        "-h", host,
        "-P", port,
        "-u", admin_user,
    ]

    if fetch_results:
        command.extend(["-ssN", "-e", sql_command])  # -ssN = silent, skip-column-names
    else:
        command.extend(["-e", sql_command])

    try:
        # 3. Thực thi lệnh
        process = subprocess.run(
            command,
            env=env,
            capture_output=True,
            text=True,
            check=True,  # Tự ném lỗi nếu return code != 0
            encoding='utf-8'
        )

        # 4. Xử lý kết quả
        if fetch_results:
            # Parse output: stdout là một chuỗi, mỗi dòng là 1 kết quả
            results = process.stdout.strip().splitlines()
            return True, f"Thực thi SELECT thành công, tìm thấy {len(results)} kết quả.", results
        else:
            # Cho lệnh ALTER, KILL
            return True, "Thực thi lệnh (ALTER/KILL) thành công.", []

    except subprocess.CalledProcessError as e:
        # Lỗi từ mysql client (ví dụ: Lỗi 1396, sai quyền)
        error_msg = e.stderr.strip()
        return False, error_msg, []
    except FileNotFoundError:
        error_msg = "Lệnh 'mysql' không được tìm thấy. Đảm bảo MySQL Client đã được cài đặt và nằm trong PATH."
        return False, error_msg, []
    except Exception as e:
        return False, f"Lỗi không xác định: {str(e)}", []


def execute_lock_and_kill_strategy(user_name: str, db_config: Dict[str, Any], reason: str) -> List[Tuple[str, str]]:
    """
    Hàm chính thực thi chiến lược "Lock & Kill"

    Args:
        user_name: Tên user vi phạm (ví dụ: 'dev_user').
        db_config: Thông tin đăng nhập admin.
        reason: Lý do tổng quan (ví dụ: 'Vượt ngưỡng 20 vi phạm').

    Returns:
        Một danh sách các thông báo (message)
    """

    messages = []

    # === HÀNH ĐỘNG 1: LOCK  ===
    # (Truy vấn mysql.user và khóa tất cả)

    messages.append(("info", f"Bắt đầu Hành động 1 (LOCK) cho user '{user_name}'..."))

    # 1.1. Truy vấn để tìm tất cả host của user
    sql_find_hosts = f"SELECT host FROM mysql.user WHERE user = '{user_name}';"

    success_find, msg_find, hosts_to_lock = _execute_mysql_query(
        db_config, sql_find_hosts, fetch_results=True
    )

    if not success_find:
        messages.append(("error", f"Lỗi nghiêm trọng khi tìm host: {msg_find}"))
        return messages  # Không thể tiếp tục nếu không tìm được host

    if not hosts_to_lock:
        messages.append(
            ("warning", f"Không tìm thấy tài khoản nào trong mysql.user cho '{user_name}'. Không thể LOCK."))

    else:
        messages.append(("info", f"Tìm thấy {len(hosts_to_lock)} tài khoản: {hosts_to_lock}. Bắt đầu khóa..."))

        # 1.2. Lặp và khóa từng host
        for host in hosts_to_lock:
            sql_lock = f"ALTER USER '{user_name}'@'{host}' ACCOUNT LOCK;"
            reason_lock = f"{reason} | Khóa tài khoản @{host}"

            success_lock, msg_lock, _ = _execute_mysql_query(db_config, sql_lock, fetch_results=False)

            if success_lock:
                messages.append(("success", f"Đã LOCK thành công: '{user_name}'@'{host}'"))
                # log_active_response_action("LOCK_ACCOUNT", f"'{user_name}'@'{host}'", reason_lock)
            else:
                messages.append(("error", f"Lỗi khi LOCK '{user_name}'@'{host}': {msg_lock}"))
                # log_active_response_action("LOCK_FAILED", f"'{user_name}'@'{host}'", msg_lock)

    # === HÀNH ĐỘNG 2: KILL  ===
    # (Truy vấn PROCESSLIST và KILL)

    messages.append(("info", f"Bắt đầu Hành động 2 (KILL) cho user '{user_name}'..."))

    # 2.1. Truy vấn PROCESSLIST để tìm các session ID đang hoạt động
    sql_find_sessions = f"SELECT ID FROM INFORMATION_SCHEMA.PROCESSLIST WHERE USER = '{user_name}';"

    success_find_sess, msg_find_sess, session_ids_to_kill = _execute_mysql_query(
        db_config, sql_find_sessions, fetch_results=True
    )

    if not success_find_sess:
        messages.append(("error", f"Lỗi nghiêm trọng khi tìm session: {msg_find_sess}"))
        return messages  # Dừng lại nếu không thể truy vấn PROCESSLIST

    if not session_ids_to_kill:
        messages.append(("info", f"Không tìm thấy session nào đang hoạt động cho '{user_name}'. SKIP KILL."))

    else:
        messages.append(("warning",
                         f"Tìm thấy {len(session_ids_to_kill)} session đang hoạt động: {session_ids_to_kill}. Bắt đầu KILL..."))

        # 2.2. Lặp và KILL từng session
        for session_id in session_ids_to_kill:
            sql_kill = f"KILL CONNECTION {session_id};"
            reason_kill = f"{reason} | Ngắt session ID {session_id}"

            success_kill, msg_kill, _ = _execute_mysql_query(db_config, sql_kill, fetch_results=False)

            if success_kill:
                messages.append(("success", f"Đã KILL thành công: Session ID {session_id}"))
                # log_active_response_action("KILL_SESSION", f"Session {session_id}", reason_kill)
            else:
                messages.append(("error", f"Lỗi khi KILL Session ID {session_id}: {msg_kill}"))
                # log_active_response_action("KILL_FAILED", f"Session {session_id}", msg_kill)

    return messages