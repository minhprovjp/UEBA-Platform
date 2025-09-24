// src/stores/useStatStore.ts
import { create } from 'zustand';
import axios from 'axios';
import { API_URL } from '../config';

// Định nghĩa cấu trúc dữ liệu thống kê từ API
interface Stats {
  anomaly_counts: Record<string, number>;
  top_users: Record<string, number>;
}

// Định nghĩa cấu trúc state và các hàm hành động của store
interface StatState {
  stats: Stats | null;
  loading: boolean;
  error: string | null;
  fetchStats: () => Promise<void>;
}

export const useStatStore = create<StatState>((set) => ({
  stats: null,
  loading: false,
  error: null,
  
  // Hàm để gọi API và cập nhật state
  fetchStats: async () => {
    set({ loading: true, error: null });
    try {
      const response = await axios.get<Stats>(`${API_URL}/api/stats/summary`);
      set({ stats: response.data, loading: false });
    } catch (error) {
      set({ error: 'Failed to fetch statistics', loading: false });
      console.error("Statistics fetch error:", error);
    }
  },
}));