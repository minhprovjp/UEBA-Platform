// src/pages/EngineControlPage.tsx (Nội dung đầy đủ và chính xác)

import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { API_URL } from '../config';
import './EngineControlPage.css'; // Giả sử bạn đã có file CSS này

interface EngineStatus {
  is_running: boolean;
  status: string;
  last_run_finish_time_utc: string | null;
}

const EngineControlPage: React.FC = () => {
  const [status, setStatus] = useState<EngineStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchStatus = async () => {
    try {
      const response = await axios.get<EngineStatus>(`${API_URL}/api/engine/status`);
      setStatus(response.data);
      setError(null); 
    } catch (err) {
      setError("Không thể lấy trạng thái. API server có đang chạy không hoặc endpoint không tồn tại?");
      setStatus(null); 
    } finally {
      if (loading) {
        setLoading(false);
      }
    }
  };

  useEffect(() => {
    fetchStatus();
    const intervalId = setInterval(fetchStatus, 5000);
    return () => clearInterval(intervalId);
  }, []); 

  if (loading) {
    return <div className="page-container"><p>Đang tải trạng thái...</p></div>;
  }

  return (
    <div className="page-container">
      <h1>Giám sát Trạng thái Engine</h1>
      <p>Theo dõi trạng thái của tiến trình phân tích log chạy nền.</p>
      <div className="info-box">
        ℹ️ **Lưu ý:** Việc khởi động và dừng Engine giờ đây được quản lý trực tiếp trên server (bằng cách chạy `python engine/engine_runner.py`), không còn điều khiển qua giao diện.
      </div>

      {error && <p className="error-message">{error}</p>}
      
      {status ? (
        <div className="status-container">
          <div className="status-item">
            <span className="status-label">Trạng thái hiện tại:</span>
            <span className={`status-badge ${status.is_running ? 'running' : 'stopped'}`}>
              {status.is_running ? 'Đang chạy' : 'Đã dừng'}
            </span>
          </div>
          <div className="status-item">
            <span className="status-label">Hành động:</span>
            <span className="status-value">{status.status}</span>
          </div>
          <div className="status-item">
            <span className="status-label">Lần chạy cuối (UTC):</span>
            <span className="status-value">
              {status.last_run_finish_time_utc 
                ? new Date(status.last_run_finish_time_utc).toLocaleString('vi-VN') 
                : 'Chưa chạy lần nào'}
            </span>
          </div>
        </div>
      ) : (
        !error && <p>Không có thông tin trạng thái.</p>
      )}

    </div>
  );
};

export default EngineControlPage;