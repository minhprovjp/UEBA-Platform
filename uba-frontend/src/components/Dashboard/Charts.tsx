// src/components/Dashboard/Charts.tsx
import React from 'react';
import { Bar, Pie } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement,
} from 'chart.js';  
import type { Anomaly } from '../../interfaces/Anomaly';
import './Dashboard.css';

// Đăng ký các thành phần cần thiết cho Chart.js
ChartJS.register(
  CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend, ArcElement
);

interface ChartsProps {
  anomalies: Anomaly[];
}

const Charts: React.FC<ChartsProps> = ({ anomalies }) => {
  // --- Dữ liệu cho Biểu đồ Cột ---
  const anomalyCounts = anomalies.reduce((acc, anomaly) => {
    acc[anomaly.anomaly_type] = (acc[anomaly.anomaly_type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  const barChartData = {
    labels: Object.keys(anomalyCounts),
    datasets: [{
      label: 'Số lượng bất thường',
      data: Object.values(anomalyCounts),
      backgroundColor: 'rgba(77, 171, 247, 0.6)',
      borderColor: 'rgba(77, 171, 247, 1)',
      borderWidth: 1,
    }],
  };

  // --- Dữ liệu cho Biểu đồ Tròn ---
  const userCounts = anomalies.reduce((acc, anomaly) => {
    if(anomaly.user) {
      acc[anomaly.user] = (acc[anomaly.user] || 0) + 1;
    }
    return acc;
  }, {} as Record<string, number>);
  
  const topUsers = Object.entries(userCounts)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 5);

  const pieChartData = {
    labels: topUsers.map(([user]) => user),
    datasets: [{
      data: topUsers.map(([, count]) => count),
      backgroundColor: ['#4DABF7', '#74C0FC', '#A5D8FF', '#E7F5FF', '#364FC7'],
    }],
  };

  return (
    <div className="charts-container">
      <div className="chart-wrapper">
        <h3>Số lượng bất thường theo loại</h3>
        <Bar data={barChartData} />
      </div>
      <div className="chart-wrapper">
        <h3>Top 5 Users có nhiều bất thường nhất</h3>
        <Pie data={pieChartData} />
      </div>
    </div>
  );
};

export default Charts;