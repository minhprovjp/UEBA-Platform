// uba_frontend/src/api/client.js
import axios from 'axios';

const baseURL = (import.meta.env.VITE_API_URL || 'http://localhost:8001').replace(/\/$/, '');

export const apiClient = axios.create({
  baseURL,
  headers: {
    'Content-Type': 'application/json',
    'x-api-key': import.meta.env.VITE_API_KEY || '',
  },
  timeout: 120000,
});

// [NEW] Interceptor: Tự động chèn Token vào Header trước khi gửi request
apiClient.interceptors.request.use((config) => {
  const token = localStorage.getItem('uba_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// [NEW] Interceptor: Nếu gặp lỗi 401 (Unauthorized), tự động đá ra Login
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response && error.response.status === 401) {
      localStorage.removeItem('uba_token');
      window.location.href = '/'; // Reload về trang chủ (sẽ bị chặn lại ở Login)
    }
    return Promise.reject(error);
  }
);

export const cleanParams = (obj = {}) =>
  Object.fromEntries(Object.entries(obj).filter(([, v]) => v !== undefined && v !== null && v !== ''));
