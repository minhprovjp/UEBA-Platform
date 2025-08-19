// src/components/EngineControl/EngineControlPage.tsx
import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './EngineControl.css'; // File CSS riêng cho trang này

const API_URL = 'http://127.0.0.1:8000';

interface EngineStatus {
  is_running: boolean;
  status: string;
  last_run_finish_time_utc: string | null;
}

const EngineControlPage: React.FC = () => {
  const [status, setStatus] = useState<EngineStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Hàm để lấy trạng thái từ API
  const fetchStatus = async () => {
    try {
      const response = await axios.get<EngineStatus>(`${API_URL}/api/engine/status`);
      setStatus(response.data);
    } catch (err) {
      setError("Không thể kết nối đến API để lấy trạng thái Engine.");
    } finally {
      setLoading(false);
    }
  };

  // Tự động làm mới trạng thái mỗi 5 giây
  useEffect(() => {
    fetchStatus(); // Lấy trạng thái ngay khi tải trang
    const intervalId = setInterval(fetchStatus, 5000); // Lặp lại sau mỗi 5 giây
    return () => clearInterval(intervalId); // Dọn dẹp khi component bị hủy
  }, []);

  const handleStart = async () => {
    try {
      await axios.post(`${API_URL}/api/engine/start`);
      alert("Đã gửi yêu cầu khởi động Engine!");
      setTimeout(fetchStatus, 1000); // Cập nhật trạng thái sau 1 giây
    } catch (err) {
      alert("Lỗi khi khởi động Engine.");
    }
  };

  const handleStop = async () => {
    try {
      await axios.post(`${API_URL}/api/engine/stop`);
      alert("Đã gửi yêu cầu dừng Engine!");
      setTimeout(fetchStatus, 1000); // Cập nhật trạng thái sau 1 giây
    } catch (err) {
      alert("Lỗi khi dừng Engine.");
    }
  };
  
  return (
    <div className="engine-control-page">
      <h1>Bảng Điều khiển Engine</h1>
      {loading && <p>Đang tải trạng thái...</p>}
      {error && <p className="error-message">{error}</p>}
      
      {status && (
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
            <span className="status-label">Lần chạy cuối hoàn thành (UTC):</span>
            <span className="status-value">
              {status.last_run_finish_time_utc 
                ? new Date(status.last_run_finish_time_utc).toLocaleString('vi-VN') 
                : 'Chưa chạy lần nào'}
            </span>
          </div>
        </div>
      )}

      <div className="actions-container">
        <button onClick={handleStart} disabled={status?.is_running}>
          Khởi động Engine
        </button>
        <button onClick={handleStop} disabled={!status?.is_running} className="stop-button">
          Dừng Engine
        </button>
      </div>
    </div>
  );
};

export default EngineControlPage;