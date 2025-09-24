// src/stores/anomalyStore.ts
import { create } from 'zustand';
import type { Anomaly } from '../interfaces/Anomaly';
import axios from 'axios';
import { API_URL } from '../config';

// Định nghĩa cấu trúc state mới, bao gồm các biến cho phân trang
interface AnomalyState {
  anomalies: Anomaly[];
  recentAnomalies: Anomaly[]; // <-- THÊM STATE MỚI
  loading: boolean;
  error: string | null;
  
  currentPage: number;
  itemsPerPage: number;
  totalItems: number;
  
  fetchAnomalies: (page: number, type: string) => Promise<void>;
  fetchRecentAnomalies: () => Promise<void>; // <-- THÊM ACTION MỚI
  setCurrentPage: (page: number, type: string) => void;
}

export const useAnomalyStore = create<AnomalyState>((set, get) => ({
  anomalies: [],
  recentAnomalies: [], // <-- KHỞI TẠO STATE MỚI
  loading: false,
  error: null,
  
  currentPage: 1,
  itemsPerPage: 50,
  totalItems: 0,

  // Hàm fetch được nâng cấp để nhận cả `page` và `type`
  fetchAnomalies: async (page: number, type: string) => {
    set({ loading: true, error: null });
    const { itemsPerPage } = get();
    const skip = (page - 1) * itemsPerPage;
    
    try {
      // Gọi đến API với đầy đủ các tham số
      const response = await axios.get(`${API_URL}/api/anomalies/?skip=${skip}&limit=${itemsPerPage}&type=${type}`);
      set({
        anomalies: response.data.items,
        totalItems: response.data.total_items,
        currentPage: page,
        loading: false,
      });
    } catch (error) {
      set({ error: 'Failed to fetch paginated anomalies', loading: false });
    }
  },

  // HÀM MỚI ĐỂ FETCH 10 BẤT THƯỜNG GẦN ĐÂY NHẤT
  fetchRecentAnomalies: async () => {
    // Không cần set loading/error ở đây để tránh ảnh hưởng đến các phần khác của UI
    try {
      // Gọi API, không cần type, chỉ cần limit=10
      const response = await axios.get(`${API_URL}/api/anomalies/?limit=10`);
      set({ recentAnomalies: response.data.items });
    } catch (err) {
      console.error("Failed to fetch recent anomalies", err);
      // Có thể set một biến lỗi riêng nếu bạn muốn hiển thị lỗi này
    }
  },
  
  // setCurrentPage giờ đây cũng cần biết `type` để fetch lại cho đúng
  setCurrentPage: (page: number, type: string) => {
    const { totalItems, itemsPerPage, fetchAnomalies } = get();
    const totalPages = Math.ceil(totalItems / itemsPerPage);
    if (page > 0 && page <= totalPages) {
      fetchAnomalies(page, type);
    }
  },
}));