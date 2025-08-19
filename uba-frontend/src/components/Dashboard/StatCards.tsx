// src/components/Dashboard/StatCards.tsx
import React from 'react';
import type { Anomaly } from '../../interfaces/Anomaly';
import './Dashboard.css';

interface StatCardsProps {
  anomalies: Anomaly[];
}

const StatCards: React.FC<StatCardsProps> = ({ anomalies }) => {
  // Tính toán số lượng cho từng loại bất thường
  const anomalyCounts = anomalies.reduce((acc, anomaly) => {
    acc[anomaly.anomaly_type] = (acc[anomaly.anomaly_type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  const stats = [
    { title: 'Truy vấn Giờ Khuya', count: anomalyCounts['late_night'] || 0 },
    { title: 'Kết xuất Dữ liệu Lớn', count: anomalyCounts['dump'] || 0 },
    { title: 'Truy cập Nhiều Bảng', count: anomalyCounts['multi_table'] || 0 },
    { title: 'Truy cập Bảng Nhạy cảm', count: anomalyCounts['sensitive'] || 0 },
    { title: 'HĐ User Bất thường', count: anomalyCounts['user_time'] || 0 },
    { title: 'Độ phức tạp (AI)', count: anomalyCounts['complexity'] || 0 },
  ];

  return (
    <div className="stats-container">
      {stats.map((stat, index) => (
        <div className="stat-card" key={index}>
          <h3>{stat.count}</h3>
          <p>{stat.title}</p>
        </div>
      ))}
    </div>
  );
};

export default StatCards;