// uba_frontend/src/api/queries.js
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { apiClient, cleanParams } from './client';
import { toast } from 'sonner';

// --- QUERIES (Lệnh GET) ---

// KPIs theo rule
export const useAnomalyKpis = () =>
  useQuery({
    queryKey: ['anomalyKpis'],
    queryFn: async () => {
      const { data } = await apiClient.get('/api/anomalies/kpis');
      return data; // {late_night, large_dump, multi_table, sensitive_access, profile_deviation, total}
    },
    staleTime: 30_000,
    refetchInterval: 2000,
  });

// Facets (users, types)
export const useAnomalyFacets = (behavior_group) =>
  useQuery({
    queryKey: ['anomalyFacets', behavior_group], // Thêm vào key để auto-refetch khi đổi nhóm
    queryFn: async () => {
      const { data } = await apiClient.get('/api/anomalies/facets', {
        params: cleanParams({ behavior_group }) // Gửi params
      });
      return data; 
    },
    staleTime: 60_000,
    refetchInterval: 2000,
  });

// Search hợp nhất (server-side)
export const useAnomalySearch = (filters) =>
  useQuery({
    queryKey: ['anomalySearch', filters],
    queryFn: async () => {
      const { data } = await apiClient.get('/api/anomalies/search', {
        params: cleanParams({
          skip: filters.pageIndex * filters.pageSize,
          limit: filters.pageSize,
          search: filters.search,
          user: filters.user,
          anomaly_type: filters.anomaly_type,
          behavior_group: filters.behavior_group,
          date_from: filters.date_from,
          date_to: filters.date_to,
        }),
      });
      return data; // {items, total}
    },
    keepPreviousData: true,
    staleTime: 0,           
    refetchInterval: 2000,
  });

// Hook để lấy TẤT CẢ logs (có phân trang và bộ lọc)
export const useLogs = (filters) => {
  return useQuery({
    queryKey: ['logs', filters], // Key sẽ thay đổi khi filter thay đổi
    queryFn: async () => {
      const params = cleanParams({
        skip: filters.pageIndex * filters.pageSize,
        limit: filters.pageSize,
        search: filters.search,
        user: filters.user,
        // Backend mong đợi YYYY-MM-DDTHH:MM:SS, nếu input type="datetime-local" trả về YYYY-MM-DDTHH:MM
        // thì vẫn ổn, nhưng quan trọng là không được gửi chuỗi rỗng.
        date_from: filters.date_from, 
        date_to: filters.date_to,
        // Nếu bạn muốn hỗ trợ lọc theo anomaly only ở server-side sau này:
        // is_anomaly: filters.is_anomaly 
      });
      // params sẽ tự động loại bỏ các giá trị null/undefined
      const { data } = await apiClient.get('/api/logs/', {params});
      // API của bạn không trả về totalCount, nên ta sẽ tự đoán
      // Nếu số log trả về < pageSize, nghĩa là đã hết
      const hasMore = data.length === filters.pageSize;
      return { logs: data, hasMore };
    },
  });
};

// Hook để lấy TẤT CẢ anomalies (có phân trang và bộ lọc)
export const useAnomalyStats = (timeRange = '24h') => // <--- Nhận tham số
  useQuery({
    queryKey: ['anomalyStats', timeRange], // <--- Thêm vào key để auto refetch
    queryFn: async () => {
      const { data } = await apiClient.get('/api/anomalies/stats', {
        params: { time_range: timeRange } // <--- Gửi params lên server
      });
      return data;
    },
    staleTime: 0,            // Dữ liệu luôn được coi là "cũ" ngay lập tức để chấp nhận cái mới
    refetchInterval: 2000,   // Tự động gọi lại API mỗi 5000ms (5 giây)
  });

export const useEventAnomalies = (filters) =>
  useQuery({
    queryKey: ['eventAnomalies', filters],
    queryFn: async () => {
      const { data } = await apiClient.get('/api/anomalies/events', {
        params: cleanParams({
          skip: filters.pageIndex * filters.pageSize,
          limit: filters.pageSize,
          search: filters.search,
          user: filters.user,
          anomaly_type: filters.anomaly_type,
          date_from: filters.date_from,
          date_to: filters.date_to,
        }),
      });
      return data; // UnifiedAnomaly[]
    },
    keepPreviousData: true,
    staleTime: 0,          
    refetchInterval: 2000,
  });

export const useAggregateAnomalies = (filters) =>
  useQuery({
    queryKey: ['aggregateAnomalies', filters],
    queryFn: async () => {
      const { data } = await apiClient.get('/api/aggregate-anomalies', {
        params: cleanParams({
          skip: filters.pageIndex * filters.pageSize,
          limit: filters.pageSize,
          search: filters.search,
          user: filters.user,
          anomaly_type: filters.anomaly_type,
          date_from: filters.date_from,
          date_to: filters.date_to,
        }),
      });
      return data; // UnifiedAnomaly[]
    },
    keepPreviousData: true,
    staleTime: 0,         
    refetchInterval: 2000,
  });

// (tuỳ chọn) dữ liệu để vẽ biểu đồ theo giờ từ Event anomalies
export const useEventAnomalyHistogram = () =>
  useQuery({
    queryKey: ['eventAnomalyHistogram'],
    queryFn: async () => {
      const { data } = await apiClient.get('/api/anomalies/events', { params: { skip: 0, limit: 100000 } });
      // sửa spread: ...a (không phải .a)
      const hours = Array.from({ length: 24 }, (_, i) => ({ name: `${i}:00`, count: 0 }));
      for (const a of data.map((a) => ({ ...a, hour: new Date(a.timestamp).getHours() }))) {
        hours[a.hour].count += 1;
      }
      return hours;
    },
    staleTime: 30_000,
    refetchInterval: 2000,
  });

export const useAnomalyTypeStats = () =>
  useQuery({
    queryKey: ['anomalyTypeStats'],
    queryFn: async () => {
      const { data } = await apiClient.get('/api/anomalies/type-stats');
      return data; // { by_type: {late_night, dump, multi_table, sensitive, user_time, ml}, total }
    },
    staleTime: 30_000,
    refetchInterval: 2000,
  });


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