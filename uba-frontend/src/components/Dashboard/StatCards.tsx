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
<<<<<<< Updated upstream
<<<<<<< Updated upstream
    { title: 'Truy vấn Giờ Khuya', count: anomalyCounts['late_night'] || 0 },
    { title: 'Kết xuất Dữ liệu Lớn', count: anomalyCounts['dump'] || 0 },
    { title: 'Truy cập Nhiều Bảng', count: anomalyCounts['multi_table'] || 0 },
    { title: 'Truy cập Bảng Nhạy cảm', count: anomalyCounts['sensitive'] || 0 },
    { title: 'HĐ User Bất thường', count: anomalyCounts['user_time'] || 0 },
    { title: 'Độ phức tạp (AI)', count: anomalyCounts['complexity'] || 0 },
=======
=======
>>>>>>> Stashed changes
    { 
      title: 'Truy vấn Giờ Khuya', 
      count: anomalyCounts['late_night'] || 0,
      icon: '🌙',
      color: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
    },
    { 
      title: 'Kết xuất Dữ liệu Lớn', 
      count: anomalyCounts['dump'] || 0,
      icon: '📊',
      color: 'linear-gradient(135deg, #f093fb 0%, #f5576c 100%)'
    },
    { 
      title: 'Truy cập Nhiều Bảng', 
      count: anomalyCounts['multi_table'] || 0,
      icon: '🔗',
      color: 'linear-gradient(135deg, #4facfe 0%, #00f2fe 100%)'
    },
    { 
      title: 'Truy cập Bảng Nhạy cảm', 
      count: anomalyCounts['sensitive'] || 0,
      icon: '⚠️',
      color: 'linear-gradient(135deg, #fa709a 0%, #fee140 100%)'
    },
    { 
      title: 'HĐ User Bất thường', 
      count: anomalyCounts['user_time'] || 0,
      icon: '⏰',
      color: 'linear-gradient(135deg, #a8edea 0%, #fed6e3 100%)'
    },
    { 
      title: 'Độ phức tạp (AI)', 
      count: anomalyCounts['complexity'] || 0,
      icon: '🤖',
      color: 'linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%)'
    },
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
  ];

  return (
    <div className="stats-container">
      {stats.map((stat, index) => (
        <div className="stat-card" key={index}>
<<<<<<< Updated upstream
<<<<<<< Updated upstream
          <h3>{stat.count}</h3>
          <p>{stat.title}</p>
=======
=======
>>>>>>> Stashed changes
          <div className="stat-icon" style={{ background: stat.color }}>
            {stat.icon}
          </div>
          <div className="stat-content">
            <h3>{stat.count.toLocaleString()}</h3>
            <p>{stat.title}</p>
          </div>
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
        </div>
      ))}
    </div>
  );
};

export default StatCards;