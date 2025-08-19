// src/components/Dashboard/AnomalyTable.tsx
import React, { useState, useEffect } from 'react';
import axios from 'axios';
<<<<<<< Updated upstream
<<<<<<< Updated upstream
import type { Anomaly } from '../../interfaces/Anomaly'; // Import "bản thiết kế"
import './Dashboard.css'; // Import file CSS chúng ta sẽ tạo ngay sau đây

// Định nghĩa URL của API backend FastAPI
const API_URL = 'http://127.0.0.1:8000';

const AnomalyTable: React.FC = () => {
  // Khai báo các state để quản lý dữ liệu, trạng thái tải, và lỗi
=======
=======
>>>>>>> Stashed changes
import type { Anomaly } from '../../interfaces/Anomaly';
import './Dashboard.css';

const API_URL = 'http://127.0.0.1:8000';

const AnomalyTable: React.FC = () => {
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

<<<<<<< Updated upstream
<<<<<<< Updated upstream
  // Sử dụng useEffect để thực hiện việc gọi API một lần khi component được tải
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
  useEffect(() => {
    const fetchAnomalies = async () => {
      try {
        setLoading(true);
<<<<<<< Updated upstream
<<<<<<< Updated upstream
        // Gọi đến endpoint /api/anomalies/
        const response = await axios.get(`${API_URL}/api/anomalies/`);
        setAnomalies(response.data); // Lưu dữ liệu trả về vào state
=======
        const response = await axios.get(`${API_URL}/api/anomalies/`);
        setAnomalies(response.data);
>>>>>>> Stashed changes
=======
        const response = await axios.get(`${API_URL}/api/anomalies/`);
        setAnomalies(response.data);
>>>>>>> Stashed changes
        setError(null);
      } catch (err) {
        setError("Không thể tải dữ liệu từ API. Hãy đảm bảo API server đang chạy.");
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    fetchAnomalies();
<<<<<<< Updated upstream
<<<<<<< Updated upstream
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
=======
=======
>>>>>>> Stashed changes
  }, []);

  if (loading) {
    return (
      <div className="table-container">
        <div className="loading-container">
          <div className="loading-text">🔄 Đang tải dữ liệu bất thường...</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="table-container">
        <p className="error-message">{error}</p>
      </div>
    );
  }

  if (anomalies.length === 0) {
    return (
      <div className="table-container">
        <div className="empty-state">
          <div className="empty-icon">📭</div>
          <h3>Không tìm thấy bất thường nào</h3>
          <p>Hệ thống hiện tại không phát hiện bất thường nào.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="table-container">
      <div className="table-header">
        <h2>📋 Danh sách Bất thường Mới nhất</h2>
        <div className="table-stats">
          <span className="stat-badge">Tổng: {anomalies.length}</span>
        </div>
      </div>
      <div className="table-wrapper">
        <table className="anomaly-table">
          <thead>
            <tr>
              <th>⏰ Thời gian</th>
              <th>👤 User</th>
              <th>🚨 Loại Bất thường</th>
              <th>📊 Điểm số</th>
              <th>🔍 Query</th>
              <th>🌐 IP Client</th>
            </tr>
          </thead>
          <tbody>
            {anomalies.slice(0, 10).map((anomaly) => (
              <tr key={anomaly.id}>
                <td className="timestamp-cell">
                  {new Date(anomaly.timestamp).toLocaleString('vi-VN')}
                </td>
                <td className="user-cell">
                  <span className="user-badge">{anomaly.user}</span>
                </td>
                <td className="type-cell">
                  <span className={`type-badge type-${anomaly.anomaly_type}`}>
                    {anomaly.anomaly_type}
                  </span>
                </td>
                <td className="score-cell">
                  {anomaly.score ? (
                    <span className={`score-badge score-${anomaly.score > 0.7 ? 'high' : anomaly.score > 0.4 ? 'medium' : 'low'}`}>
                      {anomaly.score.toFixed(4)}
                    </span>
                  ) : (
                    <span className="score-badge score-na">N/A</span>
                  )}
                </td>
                <td className="query-cell">
                  <code>{anomaly.query.slice(0, 150)}...</code>
                </td>
                <td className="ip-cell">
                  <span className="ip-badge">{anomaly.client_ip || 'N/A'}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
    </div>
  );
};

export default AnomalyTable;