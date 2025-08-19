// src/components/Dashboard/DashboardPage.tsx
import React, { useState, useEffect } from 'react';
import axios from 'axios';
import type { Anomaly } from '../../interfaces/Anomaly';
import StatCards from './StatCards';
import Charts from './Charts';
import AnomalyTable from './AnomalyTable';
<<<<<<< Updated upstream
<<<<<<< Updated upstream
=======
import AnomalyDetailModal from './AnomalyDetailModal';
import { LoadingSpinner } from '../UI';
import './Dashboard.css';
>>>>>>> Stashed changes
=======
import AnomalyDetailModal from './AnomalyDetailModal';
import { LoadingSpinner } from '../UI';
import './Dashboard.css';
>>>>>>> Stashed changes

const API_URL = 'http://127.0.0.1:8000';

const DashboardPage: React.FC = () => {
<<<<<<< Updated upstream
<<<<<<< Updated upstream
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // Component này sẽ chịu trách nhiệm gọi API một lần duy nhất
  useEffect(() => {
    const fetchAnomalies = async () => {
      try {
        setLoading(true);
        const response = await axios.get(`${API_URL}/api/anomalies/`);
        setAnomalies(response.data);
        setError(null);
      } catch (err) {
        setError("Lỗi kết nối API.");
      } finally {
        setLoading(false);
      }
    };
    fetchAnomalies();
  }, []);

  if (loading) return <h3>Đang tải dữ liệu dashboard...</h3>;
  if (error) return <p className="error-message">{error}</p>;

  return (
    <div>
      <h1>Dashboard Tổng Quan</h1>
      <StatCards anomalies={anomalies} />
      <Charts anomalies={anomalies} />
      <AnomalyTable /> {/* Bảng này tự gọi API riêng, chúng ta sẽ sửa sau */}
=======
=======
>>>>>>> Stashed changes
    const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
    const [loading, setLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);
    const [selectedAnomaly, setSelectedAnomaly] = useState<Anomaly | null>(null);
    const [isModalOpen, setIsModalOpen] = useState<boolean>(false);

    const fetchAnomalies = async () => {
        try {
            const response = await axios.get(`${API_URL}/api/anomalies/`);
            setAnomalies(response.data);
            setError(null);
        } catch (err) {
            setError("Lỗi kết nối API.");
        } finally {
            setLoading(false);
        }
    };

  // Component này sẽ chịu trách nhiệm gọi API một lần duy nhất
  useEffect(() => {
    // Chạy ngay lần đầu
    fetchAnomalies();
    
    // Thiết lập một interval để gọi lại `fetchAnomalies` mỗi 10 giây
    const intervalId = setInterval(() => {
      console.log("Refreshing anomaly data...");
      fetchAnomalies();
    }, 10000); // 10000 milliseconds = 10 giây

    // Hàm dọn dẹp: sẽ được gọi khi component bị hủy (ví dụ: chuyển trang)
    // để ngăn chặn việc gọi API vô tận
    return () => clearInterval(intervalId);
  }, []); // Mảng rỗng đảm bảo useEffect chỉ chạy một lần để thiết lập interval

  if (loading) {
    return (
      <div className="dashboard-page">
        <div className="loading-container">
          <LoadingSpinner 
            size="large" 
            color="primary" 
            text="Đang tải dữ liệu dashboard..." 
          />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="dashboard-page">
        <div className="dashboard-header">
          <h1>Dashboard Tổng Quan</h1>
        </div>
        <p className="error-message">{error}</p>
      </div>
    );
  }

  return (
    <div className="dashboard-page">
      <div className="dashboard-header">
        <h1>Dashboard Tổng Quan</h1>
      </div>
      <StatCards anomalies={anomalies} />
      <Charts anomalies={anomalies} />
      <div className="table-container">
        <AnomalyTable />
      </div>
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
    </div>
  );
};

export default DashboardPage;