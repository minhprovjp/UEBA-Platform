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
      backgroundColor: [
        'rgba(99, 102, 241, 0.8)',
        'rgba(139, 92, 246, 0.8)',
        'rgba(236, 72, 153, 0.8)',
        'rgba(16, 185, 129, 0.8)',
        'rgba(245, 158, 11, 0.8)',
        'rgba(239, 68, 68, 0.8)'
      ],
      borderColor: [
        'rgba(99, 102, 241, 1)',
        'rgba(139, 92, 246, 1)',
        'rgba(236, 72, 153, 1)',
        'rgba(16, 185, 129, 1)',
        'rgba(245, 158, 11, 1)',
        'rgba(239, 68, 68, 1)'
      ],
      borderWidth: 2,
      borderRadius: 8,
      borderSkipped: false,
    }],
  };

  const barChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
      tooltip: {
        backgroundColor: 'rgba(30, 30, 46, 0.95)',
        titleColor: '#ffffff',
        bodyColor: '#b8b8d1',
        borderColor: 'rgba(255, 255, 255, 0.1)',
        borderWidth: 1,
        cornerRadius: 12,
        displayColors: true,
        titleFont: {
          size: 14,
          weight: 'bold'
        },
        bodyFont: {
          size: 13
        }
      }
    },
    scales: {
      x: {
        grid: {
          color: 'rgba(255, 255, 255, 0.1)',
          drawBorder: false,
        },
        ticks: {
          color: '#b8b8d1',
          font: {
            size: 12,
            weight: 'bold'
          }
        }
      },
      y: {
        grid: {
          color: 'rgba(255, 255, 255, 0.1)',
          drawBorder: false,
        },
        ticks: {
          color: '#b8b8d1',
          font: {
            size: 12,
            weight: 'bold'
          },
          callback: function(value: any) {
            return value.toLocaleString();
          }
        }
      }
    },
    elements: {
      bar: {
        borderRadius: 8,
      }
    }
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
      backgroundColor: [
        'rgba(99, 102, 241, 0.8)',
        'rgba(139, 92, 246, 0.8)',
        'rgba(236, 72, 153, 0.8)',
        'rgba(16, 185, 129, 0.8)',
        'rgba(245, 158, 11, 0.8)'
      ],
      borderColor: [
        'rgba(99, 102, 241, 1)',
        'rgba(139, 92, 246, 1)',
        'rgba(236, 72, 153, 1)',
        'rgba(16, 185, 129, 1)',
        'rgba(245, 158, 11, 1)'
      ],
      borderWidth: 2,
      hoverOffset: 8,
    }],
  };

  const pieChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'bottom' as const,
        labels: {
          color: '#b8b8d1',
          font: {
            size: 12,
            weight: 'bold'
          },
          padding: 20,
          usePointStyle: true,
          pointStyle: 'circle'
        }
      },
      tooltip: {
        backgroundColor: 'rgba(30, 30, 46, 0.95)',
        titleColor: '#ffffff',
        bodyColor: '#b8b8d1',
        borderColor: 'rgba(255, 255, 255, 0.1)',
        borderWidth: 1,
        cornerRadius: 12,
        displayColors: true,
        titleFont: {
          size: 14,
          weight: '600'
        },
        bodyFont: {
          size: 13
        }
      }
    }
  };

  return (
    <div className="charts-container">
      <div className="chart-wrapper">
        <h3>📊 Số lượng bất thường theo loại</h3>
        <div style={{ height: '400px', position: 'relative' }}>
          <Bar data={barChartData} options={barChartOptions as any} />
        </div>
      </div>
      <div className="chart-wrapper">
        <h3>👥 Top 5 Users có nhiều bất thường nhất</h3>
        <div style={{ height: '400px', position: 'relative' }}>
          <Pie data={pieChartData} options={pieChartOptions as any} />
        </div>
      </div>
    </div>
  );
};

export default Charts;