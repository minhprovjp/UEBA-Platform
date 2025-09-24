// src/pages/DashboardPage.tsx

import React, { useEffect } from 'react';
import { useStatStore } from '../stores/useStatStore';
import { useAnomalyStore } from '../stores/anomalyStore';
import StatCards from '../components/Dashboard/StatCards'; // Ta sẽ tạo component này
import Charts from '../components/Dashboard/Charts';
import RecentAnomalies from '../components/Dashboard/RecentAnomalies';
import './DashboardPage.css'; // Ta sẽ tạo file CSS này

const DashboardPage: React.FC = () => {
  // Lấy state và action từ các store tương ứng
  const { stats, loading: statsLoading, fetchStats } = useStatStore();
  const { recentAnomalies, fetchRecentAnomalies } = useAnomalyStore();

  // Sử dụng useEffect để gọi các API khi component được tải lần đầu
  useEffect(() => {
    fetchStats();
    fetchRecentAnomalies();
  }, [fetchStats, fetchRecentAnomalies]);

  // Xử lý trạng thái loading
  if (statsLoading || !stats) {
    return <div>Đang tải dữ liệu dashboard...</div>;
  }

  return (
    <div className="dashboard-page">
      <h1>Bảng Điều khiển Tổng quan</h1>
      
      {/* Truyền dữ liệu anomaly_counts vào StatCards */}
      <StatCards anomalyCounts={stats.anomaly_counts} />
      
      <div className="dashboard-columns">
        <div className="main-charts">
          {/* Truyền dữ liệu vào Charts */}
          <Charts 
            anomalyCounts={stats.anomaly_counts} 
            topUsers={stats.top_users} 
          />
        </div>
        <div className="recent-anomalies-panel">
          {/* RecentAnomalies tự quản lý dữ liệu của nó */}
          <RecentAnomalies />
        </div>
      </div>
    </div>
  );
};

export default DashboardPage;