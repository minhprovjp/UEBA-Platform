// src/components/Dashboard/AnomalyTable.tsx
import React, { useState, useEffect } from 'react';
import axios from 'axios';
import type { Anomaly } from '../../interfaces/Anomaly'; // Import "bản thiết kế"
import './Dashboard.css'; // Import file CSS chúng ta sẽ tạo ngay sau đây

// Định nghĩa URL của API backend FastAPI
const API_URL = 'http://127.0.0.1:8000';

const AnomalyTable: React.FC = () => {
  // Khai báo các state để quản lý dữ liệu, trạng thái tải, và lỗi
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // Sử dụng useEffect để thực hiện việc gọi API một lần khi component được tải
  useEffect(() => {
    const fetchAnomalies = async () => {
      try {
        setLoading(true);
        // Gọi đến endpoint /api/anomalies/
        const response = await axios.get(`${API_URL}/api/anomalies/`);
        setAnomalies(response.data); // Lưu dữ liệu trả về vào state
        setError(null);
      } catch (err) {
        setError("Không thể tải dữ liệu từ API. Hãy đảm bảo API server đang chạy.");
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    fetchAnomalies();
  }, []); // Mảng rỗng [] đảm bảo useEffect chỉ chạy một lần duy nhất

  // Xử lý các trạng thái giao diện
  if (loading) {
    return <p>Đang tải dữ liệu bất thường...</p>;
  }
  if (error) {
    return <p className="error-message">{error}</p>;
  }
  if (anomalies.length === 0) {
    return <p>Không tìm thấy bất thường nào.</p>;
  }

  // Nếu có dữ liệu, hiển thị bảng
  return (
    <div className="table-container">
      <h2>Danh sách Bất thường Mới nhất</h2>
      <table className="anomaly-table">
        <thead>
          <tr>
            <th>Thời gian</th>
            <th>User</th>
            <th>Loại Bất thường</th>
            <th>Điểm số</th>
            <th>Query</th>
            <th>IP Client</th>
          </tr>
        </thead>
        <tbody>
          {anomalies.map((anomaly) => (
            <tr key={anomaly.id}>
              <td>{new Date(anomaly.timestamp).toLocaleString('vi-VN')}</td>
              <td>{anomaly.user}</td>
              <td>{anomaly.anomaly_type}</td>
              <td>{anomaly.score?.toFixed(4) ?? 'N/A'}</td>
              <td className="query-cell"><code>{anomaly.query.slice(0, 150)}...</code></td>
              <td>{anomaly.client_ip}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default AnomalyTable;