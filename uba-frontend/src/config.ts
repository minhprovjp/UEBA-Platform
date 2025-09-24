// src/config.ts

// Hàm helper để đọc API URL từ các nguồn khác nhau
function getApiUrl(): string {
    // Ưu tiên 1: Đọc giá trị người dùng đã lưu trong localStorage
    const storedApiUrl = localStorage.getItem('user_api_url');
    if (storedApiUrl) {
      return storedApiUrl;
    }
  
    // Ưu tiên 2: Đọc giá trị từ biến môi trường .env (dùng khi build)
    const envApiUrl = import.meta.env.VITE_API_BASE_URL;
    if (envApiUrl) {
      return envApiUrl;
    }
  
    // Ưu tiên 3: Giá trị mặc định cuối cùng
    return 'http://127.0.0.1:8000';
  }
  
  // Hàm helper để LƯU URL mới vào localStorage
  function setApiUrl(url: string): void {
    localStorage.setItem('user_api_url', url);
  }
  
  // Xuất ra cả hai hàm và giá trị hiện tại
  export const API_URL = getApiUrl();
  export const updateUserApiUrl = setApiUrl;