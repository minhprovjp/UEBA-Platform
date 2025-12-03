# email_alert.py
"""
================================================================================
MODULE GỬI CẢNH BÁO QUA EMAIL
================================================================================
Module này chứa một hàm duy nhất `send_email_alert` chịu trách nhiệm cho
việc kết nối đến một máy chủ SMTP và gửi email thông báo khi phát hiện các
hành vi bất thường.
"""

import smtplib  # Thư viện chính của Python để làm việc với giao thức SMTP
from email.mime.text import MIMEText  # Dùng để tạo phần thân (body) của email dưới dạng văn bản thuần
from email.mime.multipart import MIMEMultipart  # Dùng để tạo một email có thể chứa nhiều phần (ví dụ: text, file đính kèm)
from email.utils import formataddr  # Dùng để định dạng địa chỉ email kèm theo tên hiển thị (ví dụ: "Tên Người Gửi <email@example.com>")


# ==============================================================================
# HÀM GỬI EMAIL CHÍNH
# ==============================================================================

def send_email_alert(subject,
                     # message,
                     text_content,  # Nội dung dạng chữ thường
                     to_recipients,
                     smtp_server,
                     smtp_port,
                     sender_email,
                     sender_password,
                     html_content=None,  # Nội dung HTML
                     bcc_recipients=None,  # Thêm tham số BCC, mặc định là None nếu không được cung cấp
                     use_tls: bool = True  # Mặc định sử dụng kết nối bảo mật TLS
                     ):
    """
    Hàm này thực hiện việc kết nối đến server SMTP và gửi đi một email cảnh báo.

    Args:
        subject (str): Tiêu đề của email.
        message (str): Nội dung văn bản của email.
        to_recipients (list): Danh sách các địa chỉ email người nhận chính (hiển thị trong trường 'To').
        smtp_server (str): Địa chỉ của máy chủ SMTP (ví dụ: 'smtp.gmail.com').
        smtp_port (int): Cổng của máy chủ SMTP (ví dụ: 587).
        sender_email (str): Địa chỉ email sẽ được dùng để gửi đi.
        sender_password (str): Mật khẩu ứng dụng (App Password) của email người gửi.
        bcc_recipients (list, optional): Danh sách người nhận ẩn danh (BCC). Mặc định là None.
        use_tls (bool, optional): Có sử dụng mã hóa TLS hay không. Mặc định là True.

    Returns:
        bool: True nếu gửi thành công.
        str: Một chuỗi chứa thông báo lỗi nếu thất bại.
    """

    # --- Bước 1: Kiểm tra các điều kiện đầu vào ---
    # Đảm bảo các thông tin đăng nhập bắt buộc đã được cung cấp.
    if not sender_email or not sender_password:
        return "Email và password của người gửi là bắt buộc"
    # Đảm bảo có ít nhất một người nhận trong trường 'To' hoặc 'BCC'.
    if not to_recipients and not bcc_recipients:
        return "Cần ít nhất một người nhận (TO hoặc BCC)"

    # --- Bước 2: Tạo đối tượng email ---
    try:
        # Tạo một đối tượng email `MIMEMultipart`, cho phép có nhiều phần khác nhau.
        msg = MIMEMultipart('alternative')
        # Thiết lập thông tin người gửi, sử dụng `formataddr`
        msg['From'] = formataddr(("ueba_system-noreply", sender_email))
        # Chỉ thêm trường 'To' vào header của email nếu có người nhận chính.
        if to_recipients:
            msg['To'] = ', '.join(
                to_recipients)

        msg['Subject'] = subject

        # 1. Attach phần Text thuần (Fallback)
        part1 = MIMEText(text_content, 'plain')
        msg.attach(part1)
        # 2. Attach phần HTML
        if html_content:
            part2 = MIMEText(html_content, 'html')
            msg.attach(part2)
        # 3: Gửi email qua SMTP ---

        # Kết hợp cả danh sách người nhận 'To' và 'BCC' lại với nhau.
        # Đây là danh sách đầy đủ mà hàm `send_message` sẽ gửi đến.
        all_recipients = to_recipients.copy()  # Bắt đầu với danh sách 'To'
        if bcc_recipients:
            all_recipients.extend(bcc_recipients)  # Nối thêm danh sách 'BCC'

        # Sử dụng câu lệnh `with` để tạo kết nối SMTP.
        # `with` đảm bảo rằng kết nối sẽ được tự động đóng (`server.quit()`) ngay cả khi có lỗi xảy ra.
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            # Nếu `use_tls` là True, nâng cấp kết nối lên chuẩn bảo mật TLS (Transport Layer Security).
            # Đây là bước cần thiết cho hầu hết các server email hiện đại như Gmail.
            if use_tls:
                server.starttls()
            server.login(sender_email, sender_password)  # Đăng nhập vào server SMTP bằng thông tin đã cung cấp.

            # Gửi email. `to_addrs` chứa tất cả người nhận (cả To và BCC).
            # Header của email (msg['To']) sẽ chỉ hiển thị những người nhận chính.
            server.send_message(msg, to_addrs=all_recipients)

        return True

    # --- Bước 4: Xử lý các ngoại lệ (lỗi) có thể xảy ra ---
    except smtplib.SMTPAuthenticationError:
        # Lỗi này xảy ra khi username/password không đúng hoặc cần tạo lại App Password.
        return "Lỗi xác thực: Sai email/ mật khẩu hoặc cần tạo lại mật khẩu ứng dụng."
    except smtplib.SMTPException as e:
        # Bắt các lỗi chung khác liên quan đến SMTP (ví dụ: không kết nối được server).
        return f"Lỗi kết nối SMTP: {str(e)}"
    except Exception as e:
        # Bắt tất cả các lỗi không lường trước khác.
        return f"Lỗi không xác định: {str(e)}"