// src/pages/AnomalyExplorerPage.tsx
import React, { useEffect } from 'react';
import AnomalyExplorer from '../components/AnomalyExplorer/AnomalyExplorerPage';
import { useAnomalyStore } from '../stores/anomalyStore';

export const AnomalyExplorerPage: React.FC = () => {
  // Store giờ đây quản lý việc gọi API, nên component "trang" này rất đơn giản
  const { fetchAnomalies } = useAnomalyStore();

  useEffect(() => {
    // Khi trang được tải lần đầu, gọi API để lấy dữ liệu cho tab mặc định
    fetchAnomalies(1, 'late_night'); 
  }, [fetchAnomalies]);

  return (
    <div>
      <h1>Anomaly Explorer</h1>
      <p>Điều tra chi tiết các hành vi bất thường được hệ thống phát hiện.</p>
      <AnomalyExplorer />
    </div>
  );
};