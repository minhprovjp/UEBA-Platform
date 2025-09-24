// src/components/AnomalyExplorer/AnomalyExplorerPage.tsx
import React, { useState, useMemo } from 'react';
import { useAnomalyStore } from '../../stores/anomalyStore';
import type { Anomaly } from '../../interfaces/Anomaly';
import AnomalyDetail from './AnomalyDetail';
import Pagination from '../UI/Pagination';
import './AnomalyExplorer.css';

const ANOMALY_TYPES: { [key: string]: string } = {
  'late_night': '🕒 Giờ Khuya',
  'dump': '💾 Kết xuất Lớn',
  'multi_table': '🔗 Nhiều Bảng',
  'sensitive': '🛡️ Bảng Nhạy cảm',
  'user_time': '👤 HĐ Bất thường',
  'complexity': '🤖 Phức tạp (AI)',
};

const AnomalyExplorer: React.FC = () => {
  // Lấy ra các state và hàm mới từ store
  const { anomalies, loading, error, currentPage, totalItems, itemsPerPage, fetchAnomalies } = useAnomalyStore();
  
  const [selectedType, setSelectedType] = useState<string>('late_night');
  const [selectedAnomaly, setSelectedAnomaly] = useState<Anomaly | null>(null);

  const handleTypeChange = (type: string) => {
    setSelectedType(type);
    setSelectedAnomaly(null);
    // Khi đổi loại, gọi API để lấy trang đầu tiên của loại mới
    fetchAnomalies(1, type); 
  };
  
  const handlePageChange = (page: number) => {
    // Khi chuyển trang, gọi API để lấy dữ liệu cho trang mới của loại hiện tại
    fetchAnomalies(page, selectedType);
    setSelectedAnomaly(null);
  };

  return (
    <div className="explorer-layout">
      {/* Sidebar phụ để chọn loại bất thường */}
      <nav className="explorer-sidebar">
        <h3>Loại Bất thường</h3>
        <ul>
          {Object.entries(ANOMALY_TYPES).map(([key, name]) => (
            <li key={key} className={selectedType === key ? 'active' : ''}>
              <button onClick={() => handleTypeChange(key)}>
                {name}
              </button>
            </li>
          ))}
        </ul>
      </nav>

      {/* Nội dung chính */}
      <main className="explorer-content">
        <header className="explorer-header">
          <h2>{ANOMALY_TYPES[selectedType]}</h2>
          <span>Tổng số: {totalItems} kết quả</span>
        </header>
        
        {loading && <p>Đang tải...</p>}
        {error && <p className="error-message">{error}</p>}

        {!loading && !error && (
            <>
                <div className="explorer-table-container">
                    {/* Bảng dữ liệu giờ đây sẽ chỉ hiển thị dữ liệu của trang hiện tại */}
                    <table className="anomaly-table">
                        <thead><tr><th>Thời gian</th><th>User</th><th>Điểm số</th><th>Query Preview</th></tr></thead>
                        <tbody>
                            {anomalies.map(anomaly => (
                                <tr key={anomaly.id} onClick={() => setSelectedAnomaly(anomaly)}
                                    className={selectedAnomaly?.id === anomaly.id ? 'selected-row' : ''}>
                                    <td>{new Date(anomaly.timestamp).toLocaleString('vi-VN')}</td>
                                    <td>{anomaly.user}</td>
                                    <td>{anomaly.score?.toFixed(4) ?? 'N/A'}</td>
                                    <td><code>{anomaly.query.slice(0, 100)}...</code></td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
                
                <Pagination 
                    currentPage={currentPage}
                    totalItems={totalItems}
                    itemsPerPage={itemsPerPage}
                    onPageChange={handlePageChange}
                />
                
                {selectedAnomaly && <AnomalyDetail key={selectedAnomaly.id} anomaly={selectedAnomaly} />}
            </>
        )}
      </main>
    </div>
  );
};

export default AnomalyExplorer;