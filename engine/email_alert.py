"""
================================================================================
MODULE GỬI CẢNH BÁO QUA EMAIL (SMTP)
================================================================================

Module này cung cấp hàm `send_email_alert` để kết nối an toàn đến một máy chủ
SMTP, hỗ trợ mã hóa TLS/SSL, và gửi các email thông báo với cấu hình người
nhận linh hoạt (TO, BCC).

Ghi chú: Để sử dụng với Gmail, bạn cần bật Xác minh 2 bước và tạo
"Mật khẩu ứng dụng" (App Password) thay vì dùng mật khẩu tài khoản chính.
"""

# Import thư viện chuẩn của Python
import smtplib
import ssl 
import datetime # Dùng để lấy thời gian hiện tại
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr

# Import các loại lỗi cụ thể để xử lý ngoại lệ (Exception Handling)
from smtplib import SMTPAuthenticationError, SMTPException

# Cấu hình Mặc định (Có thể đưa vào file cấu hình riêng nếu cần)
DEFAULT_SENDER_NAME = "UBA MONITORING SYSTEM"

# ==============================================================================
# HÀM GỬI EMAIL CHÍNH (HỖ TRỢ HTML)
# ==============================================================================

def send_email_alert(
    subject: str,
    message_plain: str,     # Nội dung văn bản thuần
    message_html: str,      # Nội dung HTML
    to_recipients: list,
    smtp_server: str,
    smtp_port: int,
    sender_email: str,
    sender_password: str,
    bcc_recipients: list = None,
    use_tls: bool = True
) -> [bool, str]:
    """
    Kết nối đến máy chủ SMTP và gửi một email cảnh báo hỗ trợ định dạng HTML.

    Args:
        subject (str): Tiêu đề của email.
        message_plain (str): Nội dung văn bản thuần (fallback cho HTML).
        message_html (str): Nội dung HTML đẹp mắt của email.
        to_recipients (list): Danh sách các địa chỉ email người nhận chính.
        smtp_server (str): Địa chỉ máy chủ SMTP.
        smtp_port (int): Cổng máy chủ SMTP.
        sender_email (str): Địa chỉ email được dùng để gửi đi.
        sender_password (str): Mật khẩu ứng dụng (App Password) của email người gửi.
        bcc_recipients (list, optional): Danh sách người nhận ẩn danh (BCC).
        use_tls (bool, optional): Có sử dụng mã hóa TLS (STARTTLS) không.

    Returns:
        [bool, str]: True nếu gửi thành công, hoặc một chuỗi chứa thông báo lỗi.
    """

    # --- Bước 1: Xác thực Đầu vào Cơ bản ---
    if not sender_email or not sender_password:
        return False, "Thiếu thông tin: Email và password (App Password) của người gửi là bắt buộc."
        
    all_recipients = (to_recipients or []) + (bcc_recipients or [])
    if not all_recipients:
        return False, "Thiếu người nhận: Cần ít nhất một người nhận trong trường 'TO' hoặc 'BCC'."

    # --- Bước 2: Tạo Đối tượng Email Message ---
    try:
        # Tạo đối tượng email MIMEMultipart('alternative') cho HTML/Plaintext
        msg = MIMEMultipart('alternative') 
        
        msg['From'] = formataddr((DEFAULT_SENDER_NAME, sender_email))
        if to_recipients:
            msg['To'] = ', '.join(to_recipients)
        msg['Subject'] = subject
        
        # Đính kèm nội dung văn bản thuần (Plain text) làm fallback đầu tiên
        msg.attach(MIMEText(message_plain, 'plain'))
        
        # Đính kèm nội dung HTML làm lựa chọn thứ hai
        msg.attach(MIMEText(message_html, 'html')) 

    except Exception as e:
        return False, f"Lỗi nội bộ khi tạo email: {e}"

    # --- Bước 3: Thiết lập và Gửi Email qua SMTP ---
    try:
        with smtplib.SMTP(smtp_server, smtp_port, timeout=10) as server:
            if use_tls:
                server.starttls()
            
            server.login(sender_email, sender_password)
            server.send_message(msg, from_addr=sender_email, to_addrs=all_recipients)

        return True, "Email đã được gửi thành công."

    except SMTPAuthenticationError:
        return False, "Lỗi xác thực SMTP: Sai Email / Mật khẩu ứng dụng (App Password) hoặc cần tạo lại."
        
    except SMTPException as e:
        return False, f"Lỗi giao thức SMTP: Không kết nối được hoặc máy chủ từ chối: {e}"
        
    except Exception as e:
        return False, f"Lỗi không xác định: {e}"

# ==============================================================================
# VÍ DỤ SỬ DỤNG (Chỉ chạy khi module này được thực thi trực tiếp)
# ==============================================================================

if __name__ == '__main__':
    
    # LẤY THỜI GIAN PHÁT HIỆN LỖI (HIỆN TẠI)
    current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # -----------------------------------------------------------------
    # 🌟 ĐIỂM ĐẦU VÀO TỪ HỆ THỐNG PHÁT HIỆN LỖI (ENGINE OUTPUT) 🌟
    # 
    # BẠN CẦN THAY THẾ DÒNG DƯỚI ĐÂY BẰNG BIẾN CHỨA ĐIỂM SỐ THỰC TẾ
    ENGINE_RISK_SCORE = 65 # Giả định: Điểm số rủi ro từ UBA Engine (0-100)
    # -----------------------------------------------------------------

    # --- LOGIC XÁC ĐỊNH MỨC ĐỘ ƯU TIÊN (Dựa trên điểm số) ---
    if ENGINE_RISK_SCORE >= 90:
        SEVERITY = "KHẨN CẤP (CRITICAL)"
        SEVERITY_COLOR = "#DC3545" # Đỏ đậm
    elif ENGINE_RISK_SCORE >= 60:
        SEVERITY = "CAO (HIGH)"
        SEVERITY_COLOR = "#FFC107" # Cam
    elif ENGINE_RISK_SCORE >= 30:
        SEVERITY = "TRUNG BÌNH (MEDIUM)"
        SEVERITY_COLOR = "#007BFF" # Xanh dương
    else:
        SEVERITY = "THẤP (LOW)"
        SEVERITY_COLOR = "#28A745" # Xanh lá
    # ---------------------------------------------------------

    # Định nghĩa các biến thông tin chi tiết (cũng nên lấy từ Engine)
    EVENT_TYPE = "Truy Cập Tài Nguyên Bị Hạn Chế"
    ACTOR = "User A (ID: 1024)"
    RESOURCE = "CSDL Khách Hàng Tuyệt Mật"
    DETAILS = f"Đã thực hiện 15 lần truy cập thất bại trong 5 phút. Điểm Rủi Ro: {ENGINE_RISK_SCORE}/100."
    TIMESTAMP = f"{current_time_str} (Local Time)" 
    
    # --- A. NỘI DUNG VĂN BẢN THUẦN (PLAIN TEXT) ---
    MESSAGE_PLAIN = f"""
[CẢNH BÁO HỆ THỐNG UBA - TÌNH TRẠNG {SEVERITY}]

LOẠI CẢNH BÁO: {EVENT_TYPE}
THỜI GIAN PHÁT HIỆN: {TIMESTAMP}

THÔNG TIN CHI TIẾT LỖI:
- Đối Tượng: {ACTOR}
- Mục Tiêu: {RESOURCE}
- Mô Tả Hành Vi: {DETAILS}

HÀNH ĐỘNG ĐỀ XUẤT: Lập tức kiểm tra log, tạm thời vô hiệu hóa User A.
Vui lòng xử lý cảnh báo này ngay lập tức.
"""

    # --- B. NỘI DUNG HTML CHUYÊN NGHIỆP ---
    MESSAGE_HTML = f"""
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
        
        <div style="background-color: #f7f7f7; padding: 20px; border-radius: 5px;">
            <h2 style="color: {SEVERITY_COLOR}; margin-top: 0;">🚨 CẢNH BÁO HỆ THỐNG UBA</h2>
            
            <table cellpadding="10" cellspacing="0" width="100%" style="border: 1px solid #ddd; border-collapse: collapse; background-color: #ffffff;">
                <tr>
                    <td width="30%" style="background-color: #eee; border: 1px solid #ddd;"><b>MỨC ĐỘ ƯU TIÊN</b></td>
                    <td style="color: {SEVERITY_COLOR}; font-weight: bold; border: 1px solid #ddd;">{SEVERITY.upper()}</td>
                </tr>
                <tr>
                    <td style="background-color: #eee; border: 1px solid #ddd;"><b>THỜI GIAN PHÁT HIỆN</b></td>
                    <td style="border: 1px solid #ddd;">{TIMESTAMP}</td>
                </tr>
            </table>
            
            <h3 style="color: #0056b3; margin-top: 25px;">CHI TIẾT LỖI BẤT THƯỜNG</h3>
            <ul style="list-style-type: none; padding-left: 0;">
                <li style="margin-bottom: 10px;"><b>LOẠI CẢNH BÁO:</b> <span style="color: #555;">{EVENT_TYPE.upper()}</span></li>
                <li style="margin-bottom: 10px;"><b>ĐỐI TƯỢNG GÂY LỖI:</b> <span style="color: #555;">{ACTOR.upper()}</span></li>
                <li style="margin-bottom: 10px;"><b>MỤC TIÊU BỊ ẢNH HƯỞNG:</b> <span style="color: #555;">{RESOURCE}</span></li>
                <li style="margin-bottom: 10px;"><b>MÔ TẢ HÀNH VI:</b> <span style="color: #555;">{DETAILS}</span></li>
            </ul>
            
            <div style="padding: 15px; background-color: #ffe0b2; border-left: 5px solid {SEVERITY_COLOR}; margin-top: 20px;">
                <p style="font-weight: bold; color: #ff9800; margin: 0;">🎯 HÀNH ĐỘNG ĐỀ XUẤT:</p>
                <ol style="margin-top: 5px; padding-left: 20px;">
                    <li><b>LẬP TỨC KIỂM TRA</b> nhật ký (logs) của {ACTOR}.</li>
                    <li>Tạm thời vô hiệu hóa tài khoản (Suspend Account) nếu hành vi tiếp diễn.</li>
                    <li>Phân tích lịch sử truy cập.</li>
                </ol>
            </div>
            
            <p style="margin-top: 20px; font-size: 0.9em; color: #777;"><i>Đây là email tự động. Vui lòng không trả lời email này.</i></p>
        </div>
    </body>
    </html>
    """
    
    # THÔNG TIN CẤU HÌNH (Sử dụng thông tin đã cung cấp)
    TEST_CONFIG = {
        'subject': f"🚨 {SEVERITY}: CẢNH BÁO BẤT THƯỜNG - {EVENT_TYPE}",
        'message_plain': MESSAGE_PLAIN.strip(),   
        'message_html': MESSAGE_HTML.strip(),     
        
        'to_recipients': ['ngoclmds170220@fpt.edu.vn'], 
        'bcc_recipients': [], 
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'sender_email': 'myngoclhoang7577@gmail.com', 
        'sender_password': 'xnxb kqhs poik mxfk' 
    }
    
    print("--- BẮT ĐẦU GỬI EMAIL THỬ NGHIỆM ---")
    
    # Kiểm tra cấu hình trước khi chạy
    if 'YOUR_APP_PASSWORD_HERE' in TEST_CONFIG['sender_password']:
        print("Lỗi: Vui lòng thay 'YOUR_APP_PASSWORD_HERE' bằng Mật khẩu ứng dụng thực tế.")
    else:
        # GỌI HÀM VỚI CÁC THAM SỐ MỚI (message_plain và message_html)
        success, result_message = send_email_alert(
            subject=TEST_CONFIG['subject'],
            message_plain=TEST_CONFIG['message_plain'],
            message_html=TEST_CONFIG['message_html'],
            to_recipients=TEST_CONFIG['to_recipients'],
            smtp_server=TEST_CONFIG['smtp_server'],
            smtp_port=TEST_CONFIG['smtp_port'],
            sender_email=TEST_CONFIG['sender_email'],
            sender_password=TEST_CONFIG['sender_password'],
            bcc_recipients=TEST_CONFIG.get('bcc_recipients'),
            use_tls=True
        )

        if success:
            print(f"✅ Gửi email THÀNH CÔNG: {result_message}")
            print(f"Người nhận TO: {', '.join(TEST_CONFIG['to_recipients'])}")
            print(f"Người nhận BCC: {', '.join(TEST_CONFIG['bcc_recipients'] or ['Không có'])}")
        else:
            print(f"❌ Gửi email THẤT BẠI: {result_message}")