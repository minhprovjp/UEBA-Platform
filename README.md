# UBA Platform - Hệ thống Phân tích Hành vi Người dùng

Đây là một nền tảng giám sát an ninh được thiết kế để phân tích log từ các hệ quản trị cơ sở dữ liệu (MySQL, PostgreSQL) nhằm phát hiện các hành vi bất thường.

Hệ thống bao gồm hai thành phần chính:
-   **Analysis Engine:** Một dịch vụ chạy nền, liên tục phân tích log và lưu kết quả vào CSDL.
-   **Backend API:** Một API server (FastAPI) để truy vấn và điều khiển engine.

## Yêu cầu Môi trường (Prerequisites)

Trước khi bắt đầu, hãy đảm bảo bạn đã cài đặt các phần mềm sau trên máy của mình:

-   [Git](https://git-scm.com/downloads)
-   [Python](https://www.python.org/downloads/) (phiên bản 3.11+)
-   [PostgreSQL](https://www.postgresql.org/download/) (tùy chọn, nếu bạn muốn phân tích log PostgreSQL)
-   [Ollama](https://ollama.com/) (tùy chọn, nếu bạn muốn sử dụng tính năng phân tích của LLM)
-   [Nodejs](https://nodejs.org/en/download)

## Hướng dẫn Cài đặt (Setup Instructions)

Hãy thực hiện các bước sau theo thứ tự để cài đặt môi trường phát triển.

### 1. Lấy Code về máy (Clone)

Mở terminal hoặc Git Bash và chạy lệnh sau:
```bash
git clone https://github.com/minhprovjp/UBA-Platform.git
cd UBA-Platform
```

### 2. Tạo và Kích hoạt Môi trường ảo Python

Việc này cực kỳ quan trọng để đảm bảo các thư viện không xung đột với hệ thống.

```bash
# Tạo một thư mục môi trường ảo tên là .venv
python -m venv .venv

# Kích hoạt môi trường ảo
# Trên Windows (Git Bash hoặc PowerShell):
.venv/Scripts/activate

# Trên macOS / Linux:
source .venv/bin/activate
```
*(Sau khi kích hoạt, bạn sẽ thấy `(.venv)` ở đầu dòng lệnh.)*

### 3. Cài đặt các Thư viện cần thiết

Sử dụng file `requirements.txt` đã được cung cấp để cài đặt đúng phiên bản của tất cả các thư viện.
```bash
pip install -r requirements.txt
```

### 4. Cấu hình Dự án

Các file cấu hình chứa thông tin nhạy cảm và đường dẫn đặc thù cho máy của bạn sẽ không được đưa lên Git. Bạn cần tự tạo chúng từ các file template.

1.  **Tạo `config.py`:**
    *   Copy file `config.py.template` và đổi tên thành `config.py`.
    *   Mở file `config.py` và điền vào các đường dẫn chính xác trên máy của bạn (ví dụ: `SOURCE_MYSQL_LOG_PATH`, `SOURCE_POSTGRES_LOG_DIR`).

2.  **Tạo `engine_config.json`:**
    *   Copy file `engine_config.json.template` và đổi tên thành `engine_config.json`.
    *   Bạn có thể giữ nguyên các giá trị mặc định ban đầu.

### 5. Khởi tạo Cơ sở dữ liệu

Cơ sở dữ liệu SQLite sẽ được tự động tạo ra. Bạn chỉ cần chạy Engine một lần.

## Hướng dẫn Chạy Ứng dụng

Bạn cần mở **hai terminal riêng biệt** (đã kích hoạt môi trường ảo `.venv`) để chạy cả hai thành phần.

### Terminal 1: Chạy Analysis Engine

Tiến trình này sẽ chạy liên tục trong nền để phân tích log.

Chạy 2 cái terminal.
```bash
python engine/perf_log_publisher.py

python engine/realtime_engine.py
```
Hãy theo dõi output để đảm bảo nó khởi động thành công và bắt đầu các chu kỳ phân tích.

### Terminal 2: Chạy Backend API (FastAPI)

Tiến trình này phục vụ API để bạn có thể tương tác.
```bash
uvicorn backend_api.main_api:app --reload
```

### Terminal 3: Chạy Frontend

Tiến trình này để hiển thị giao diện bạn có thể tương tác.
```bash
cd uba-frontend
npm run dev
```


### Kiểm tra Hoạt động

1.  Mở trình duyệt và truy cập: **[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)**
2.  Bạn sẽ thấy giao diện tài liệu Swagger UI.
3.  Hãy thử dùng các endpoint trong mục "Anomalies" và "Engine Control" để kiểm tra.
4. Truy cập vào URL **[http://127.0.0.1:5173/docs](http://127.0.0.1:5173/docs)** và kiểm tra frontend.


Setup frontend

npm create vite@latest uba-frontend -- --template react-ts
npm install axios 
npm install chart.js react-chartjs-2



npm install axios recharts lucide-react
npm install @tanstack/react-query react-day-picker date-fns
npm install react-router-dom recharts sonner
npx shadcn@latest add table
npx shadcn@latest add button
npx shadcn@latest add dialog
npx shadcn@latest add input

Nếu bạn đang dùng Docker Redis: docker run -p 6379:6379 redis:7