// src/api/queries.js
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { apiClient } from './client';
import { toast } from 'sonner';

// --- QUERIES (Lệnh GET) ---

// Hook để lấy TẤT CẢ logs (có phân trang và bộ lọc)
export const useLogs = (filters) => {
  return useQuery({
    queryKey: ['logs', filters], // Key sẽ thay đổi khi filter thay đổi
    queryFn: async () => {
      // params sẽ tự động loại bỏ các giá trị null/undefined
      const { data } = await apiClient.get('/api/logs/', { 
        params: {
          skip: filters.pageIndex * filters.pageSize,
          limit: filters.pageSize,
          search: filters.search, // Cần backend hỗ trợ
          user: filters.user,     // Cần backend hỗ trợ
          date_from: filters.date_from, // Cần backend hỗ trợ
          date_to: filters.date_to,   // Cần backend hỗ trợ
        } 
      });
      // API của bạn không trả về totalCount, nên ta sẽ tự đoán
      // Nếu số log trả về < pageSize, nghĩa là đã hết
      const hasMore = data.length === filters.pageSize;
      return { logs: data, hasMore };
    },
  });
};

// Hook để lấy TẤT CẢ anomalies (có phân trang và bộ lọc)
export const useAnomalies = (filters) => {
  return useQuery({
    queryKey: ['anomalies', filters],
    queryFn: async () => {
      const { data } = await apiClient.get('/api/anomalies/', { 
        params: {
          skip: filters.pageIndex * filters.pageSize,
          limit: filters.pageSize,
          search: filters.search,
          user: filters.user,
          anomaly_type: filters.anomaly_type,
          date_from: filters.date_from,
          date_to: filters.date_to,
        } 
      });
      const hasMore = data.length === filters.pageSize;
      return { anomalies: data, hasMore };
    },
  });
};

// Hook để lấy Thống kê (cho Dashboard & AnomalyTriage)
export const useAnomalyStats = () => {
  return useQuery({
    queryKey: ['anomalyStats'],
    queryFn: async () => {
      // Lấy 10000 bản ghi để tính toán (Đây là cách "hack" vì backend ko có endpoint stats)
      const { data } = await apiClient.get('/api/anomalies/', { params: { limit: 10000 } });
      
      // Xử lý dữ liệu
      const totalAnomalies = data.length;
      const countsPerType = data.reduce((acc, anomaly) => {
        const type = anomaly.anomaly_type || 'Unknown';
        acc[type] = (acc[type] || 0) + 1;
        return acc;
      }, {});

      const criticalAlerts = (countsPerType['sensitive'] || 0) + (countsPerType['sqli'] || 0);

      // Xử lý dữ liệu cho biểu đồ (nhóm theo giờ)
      const chartData = data
        .map(a => ({ ...a, hour: new Date(a.timestamp).getHours() }))
        .reduce((acc, anomaly) => {
          const hourKey = `${String(anomaly.hour).padStart(2, '0')}:00`;
          const entry = acc.find(item => item.name === hourKey);
          if (entry) {
            entry.anomalies += 1;
          } else {
            acc.push({ name: hourKey, anomalies: 1 });
          }
          return acc;
        }, [])
        .sort((a, b) => a.name.localeCompare(b.name)); // Sắp xếp theo giờ

      return {
        totalAnomalies,
        countsPerType,
        criticalAlerts,
        chartData,
        rawAnomalies: data, // để lấy unique users
      };
    },
  });
};


// Hook để lấy Config
export const useConfig = () => {
  return useQuery({
    queryKey: ['config'],
    queryFn: async () => {
      const { data } = await apiClient.get('/api/engine/config');
      return data;
    },
  });
};

// --- MUTATIONS (Lệnh POST, PUT) ---

// Hook để gửi Feedback
export const useFeedbackMutation = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (payload) => apiClient.post('/api/feedback/', payload),
    onSuccess: (data) => {
      toast.success(data.data.message || "Đã gửi feedback thành công!");
      // Tải lại dữ liệu anomalies để cập nhật (nếu cần)
      queryClient.invalidateQueries(['anomalies']); 
    },
    onError: (error) => {
      toast.error("Gửi feedback thất bại: " + error.message);
    }
  });
};

// Hook để gọi LLM
export const useAnalyzeMutation = () => {
  return useMutation({
    mutationFn: (anomalyData) => apiClient.post('/api/llm/analyze-anomaly', anomalyData),
    onSuccess: () => {
      toast.success("Đã gửi yêu cầu phân tích AI!");
    },
    onError: (error) => {
      toast.error("Phân tích AI thất bại: " + error.message);
    }
  });
};

// Hook để cập nhật Config
export const useUpdateConfigMutation = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (configData) => apiClient.put('/api/engine/config', configData),
    onSuccess: (data) => {
      toast.success(data.data.message || "Lưu cấu hình thành công!");
      // Tải lại config
      queryClient.invalidateQueries(['config']);
    },
    onError: (error) => {
      toast.error("Lưu cấu hình thất bại: " + error.message);
    }
  });
};