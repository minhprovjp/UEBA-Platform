// src/components/Dashboard/DashboardPage.tsx
import React, { useState, useEffect } from 'react';
import axios from 'axios';
import type { Anomaly } from '../../interfaces/Anomaly';
import StatCards from './StatCards';
import Charts from './Charts';
import AnomalyTable from './AnomalyTable';

const API_URL = 'http://127.0.0.1:8000';

const DashboardPage: React.FC = () => {
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
    </div>
  );
};

export default DashboardPage;