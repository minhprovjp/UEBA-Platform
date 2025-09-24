// src/components/Dashboard/RecentAnomalies.tsx
import React, { useEffect } from 'react'; // Bỏ useMemo, thêm useEffect
import { useAnomalyStore } from '../../stores/anomalyStore'; // Import store
import './Dashboard.css';

// Component này không cần nhận props nữa
const RecentAnomalies: React.FC = () => {
  // Kết nối trực tiếp đến store để lấy state và action
  const { recentAnomalies, fetchRecentAnomalies } = useAnomalyStore();

  // Sử dụng useEffect để gọi API một lần duy nhất khi component được render
  useEffect(() => {
    fetchRecentAnomalies();
  }, [fetchRecentAnomalies]);

  // Nếu chưa có dữ liệu, có thể hiển thị một thông báo tải
  if (recentAnomalies.length === 0) {
    return (
      <div className="table-container">
        <h3>10 Bất thường Gần đây nhất</h3>
        <p>Đang tải hoặc không có dữ liệu...</p>
      </div>
    );
  }
  
  return (
    <div className="table-container">
      <h3>10 Bất thường Gần đây nhất</h3>
      <table className="anomaly-table">
        <thead><tr><th>Thời gian</th><th>User</th><th>Loại</th><th>Query Preview</th></tr></thead>
        <tbody>
          {/* Vòng lặp map giờ đây dùng thẳng state từ store */}
          {recentAnomalies.map(anom => (
            <tr key={anom.id}>
              <td>{new Date(anom.timestamp).toLocaleString('vi-VN')}</td>
              <td>{anom.user}</td>
              <td>{anom.anomaly_type}</td>
              <td><code>{anom.query.slice(0, 100)}...</code></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default RecentAnomalies;