// src/config.ts

// Hàm helper để đọc API URL từ các nguồn khác nhau theo thứ tự ưu tiên
function getApiUrl(): string {
  // Ưu tiên 1: Đọc giá trị người dùng đã lưu trong localStorage (cho phép tùy chỉnh trên UI)
  const storedApiUrl = localStorage.getItem('user_api_url');
  if (storedApiUrl) {
    return storedApiUrl;
  }

  // Ưu tiên 2: Đọc giá trị từ biến môi trường .env (dùng khi build hoặc cấu hình mặc định)
  const envApiUrl = import.meta.env.VITE_API_BASE_URL;
  if (envApiUrl) {
    return envApiUrl;
  }

  // Ưu tiên 3: Giá trị mặc định cuối cùng nếu không có gì được thiết lập
  return 'http://localhost:8000';
}

// Hàm helper để LƯU URL mới vào localStorage
function setApiUrl(url: string): void {
  localStorage.setItem('user_api_url', url);
}

// Xuất ra cả hai hàm và giá trị hiện tại để các component khác có thể sử dụng
export const API_URL = getApiUrl();
export const updateUserApiUrl = setApiUrl;