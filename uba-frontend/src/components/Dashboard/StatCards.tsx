// src/components/Dashboard/StatCards.tsx
import React from 'react';
import './Dashboard.css'; // Tái sử dụng file CSS chung

interface StatCardsProps {
  anomalyCounts: Record<string, number>;
}

// Ánh xạ từ key hệ thống sang tên hiển thị và icon
const ANOMALY_TYPE_DISPLAY_MAP: Record<string, { name: string; icon: string }> = {
  'late_night': { name: 'Giờ Khuya', icon: '🕒' },
  'dump': { name: 'Kết xuất Lớn', icon: '💾' },
  'multi_table': { name: 'Nhiều Bảng', icon: '🔗' },
  'sensitive': { name: 'Bảng Nhạy cảm', icon: '🛡️' },
  'user_time': { name: 'HĐ Bất thường', icon: '👤' },
  'complexity': { name: 'Phức tạp (AI)', icon: '🤖' },
};

const StatCards: React.FC<StatCardsProps> = ({ anomalyCounts }) => {
  return (
    <div className="stat-cards-container">
      {Object.entries(ANOMALY_TYPE_DISPLAY_MAP).map(([key, display]) => (
        <div className="stat-card" key={key}>
          <div className="stat-card-icon">{display.icon}</div>
          <div className="stat-card-info">
            <div className="stat-card-title">{display.name}</div>
            <div className="stat-card-value">{anomalyCounts[key] || 0}</div>
          </div>
        </div>
      ))}
    </div>
  );
};

export default StatCards;