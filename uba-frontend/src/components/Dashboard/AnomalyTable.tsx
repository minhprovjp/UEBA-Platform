// src/components/Dashboard/AnomalyTable.tsx
import React, { useState, useEffect } from 'react';
import axios from 'axios';
import type { Anomaly } from '../../interfaces/Anomaly';
import './Dashboard.css';
import { API_URL } from '../../config';

const AnomalyTable: React.FC = () => {
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchAnomalies = async () => {
      try {
        setLoading(true);
        const response = await axios.get(`${API_URL}/api/anomalies/`);
        setAnomalies(response.data);
        setError(null);
      } catch (err) {
        setError("Không thể tải dữ liệu từ API. Hãy đảm bảo API server đang chạy.");
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    fetchAnomalies();
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
    <>
      <div className="table-header">
        <h2>📋 Danh sách Bất thường Mới nhất</h2>
        <div className="table-stats">
          <span className="stat-badge">Tổng: {anomalies.length}</span>
        </div>
      </div>
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
    </>
  );
};

export default AnomalyTable;