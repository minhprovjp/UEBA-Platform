// src/components/Dashboard/Charts.tsx
import React, { useMemo } from 'react';
import { Bar, Pie } from 'react-chartjs-2';
import {
  Chart as ChartJS, CategoryScale, LinearScale, BarElement,
  Title, Tooltip, Legend, ArcElement
} from 'chart.js';
import './Dashboard.css';

// Đăng ký các thành phần cần thiết cho Chart.js
ChartJS.register(
  CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend, ArcElement
);

// Component nhận props là các đối tượng đã được tính toán sẵn
interface ChartsProps {
  anomalyCounts: Record<string, number>;
  topUsers: Record<string, number>;
}

const Charts: React.FC<ChartsProps> = ({ anomalyCounts, topUsers }) => {
  
  // Sử dụng useMemo để tránh việc tính toán lại dữ liệu biểu đồ không cần thiết
  const { barChartData, pieChartData } = useMemo(() => {
    const anomalyLabels: Record<string, string> = {
      'late_night': 'Giờ Khuya',
      'dump': 'Kết xuất Lớn',
      'multi_table': 'Nhiều Bảng',
      'sensitive': 'Bảng Nhạy cảm',
      'user_time': 'HĐ Bất thường',
      'complexity': 'Phức tạp (AI)',
    };

    const barData = {
      labels: Object.values(anomalyLabels), // Lấy tên tiếng Việt để làm nhãn
      datasets: [{
        label: 'Số lượng bất thường',
        // Lấy dữ liệu theo đúng thứ tự của các nhãn
        data: Object.keys(anomalyLabels).map(key => anomalyCounts[key] || 0),
        backgroundColor: 'rgba(75, 192, 192, 0.6)',
        borderColor: 'rgba(75, 192, 192, 1)',
        borderWidth: 1,
      }],
    };

    const pieData = {
      labels: Object.keys(topUsers), // Tên user
      datasets: [{
        data: Object.values(topUsers), // Số lượng bất thường tương ứng
        backgroundColor: [
          '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF'
        ],
      }],
    };

    return { barChartData: barData, pieChartData: pieData };
  }, [anomalyCounts, topUsers]); // Chỉ chạy lại khi props thay đổi

  return (
    <div className="charts-container">
      <div className="chart-wrapper">
        <h3>Số lượng bất thường theo loại</h3>
        <Bar data={barChartData} options={{ maintainAspectRatio: false, responsive: true, plugins: { title: { display: true, text: 'Số lượng bất thường theo loại' } } }} />
      </div>
      <div className="chart-wrapper">
        <h3>Top 5 Users có nhiều bất thường nhất</h3>
        <Pie data={pieChartData} options={{ maintainAspectRatio: false, responsive: true, plugins: { title: { display: true, text: 'Top 5 Users có nhiều bất thường nhất' } } }} />
      </div>
    </div>
  );
};

export default Charts;