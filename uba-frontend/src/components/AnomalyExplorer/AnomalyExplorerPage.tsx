// src/components/AnomalyExplorer/AnomalyExplorerPage.tsx

import React, { useState, useEffect } from 'react';
import axios from 'axios';
import type { Anomaly } from '../../interfaces/Anomaly';
import AnomalyDetail from './AnomalyDetail';
import { LoadingSpinner } from '../UI';
import './AnomalyExplorer.css';
import { API_URL } from '../../config';


// Định nghĩa các loại bất thường và tên hiển thị
const ANOMALY_TYPES: { [key: string]: string } = {
  'late_night': '🕒 Giờ Khuya',
  'dump': '💾 Kết xuất Lớn',
  'multi_table': '🔗 Nhiều Bảng',
  'sensitive': '🛡️ Bảng Nhạy cảm',
  'user_time': '👤 HĐ Bất thường',
  'complexity': '🤖 Phức tạp (AI)',
};

const AnomalyExplorerPage: React.FC = () => {
  const [allAnomalies, setAllAnomalies] = useState<Anomaly[]>([]);
  const [selectedType, setSelectedType] = useState<string>('late_night');
  const [selectedAnomaly, setSelectedAnomaly] = useState<Anomaly | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // Fetch anomalies from API
  const fetchAnomalies = async () => {
    try {
      setLoading(true);
      console.log('🔍 Fetching anomalies from API...');
      const response = await axios.get(`${API_URL}/api/anomalies/`);
      console.log('📊 API Response:', response.data);
      console.log('📊 Number of anomalies received:', response.data.length);
      
      // Ensure response.data is an array
      if (Array.isArray(response.data)) {
        setAllAnomalies(response.data);
        setError(null);
      } else {
        console.error('❌ API response is not an array:', response.data);
        setError("Dữ liệu API không đúng định dạng. Vui lòng kiểm tra backend.");
        setAllAnomalies([]);
      }
    } catch (err: any) {
      console.error('❌ Error fetching anomalies:', err);
      setError("Không thể tải dữ liệu từ API. Hãy đảm bảo API server đang chạy.");
      setAllAnomalies([]);
    } finally {
      setLoading(false);
    }
  };

  // Fetch data on component mount
  useEffect(() => {
    fetchAnomalies();
    
    // Refresh data every 30 seconds
    const intervalId = setInterval(fetchAnomalies, 30000);
    return () => clearInterval(intervalId);
  }, []);

  // Filter anomalies by selected type with better error handling
  const filteredAnomalies = React.useMemo(() => {
    if (!Array.isArray(allAnomalies)) {
      console.warn('⚠️ allAnomalies is not an array:', allAnomalies);
      return [];
    }
    
    const filtered = allAnomalies.filter(anomaly => {
      if (!anomaly || typeof anomaly.anomaly_type !== 'string') {
        console.warn('⚠️ Invalid anomaly object:', anomaly);
        return false;
      }
      return anomaly.anomaly_type === selectedType;
    });
    
    console.log(`🔍 Filtered anomalies for type "${selectedType}":`, filtered.length);
    return filtered;
  }, [allAnomalies, selectedType]);

  // Get available anomaly types from actual data
  const availableTypes = React.useMemo(() => {
    if (!Array.isArray(allAnomalies)) return [];
    
    const types = [...new Set(allAnomalies.map(a => a.anomaly_type).filter(Boolean))];
    console.log('📋 Available anomaly types:', types);
    return types;
  }, [allAnomalies]);

  // Update selected type if current selection is not available
  useEffect(() => {
    if (availableTypes.length > 0 && !availableTypes.includes(selectedType)) {
      console.log(`🔄 Selected type "${selectedType}" not available, switching to first available type`);
      setSelectedType(availableTypes[0]);
      setSelectedAnomaly(null);
    }
  }, [availableTypes, selectedType]);

  // Debug logging
  useEffect(() => {
    console.log('🔍 Debug Info:');
    console.log('  - Total anomalies:', allAnomalies.length);
    console.log('  - Selected type:', selectedType);
    console.log('  - Filtered anomalies:', filteredAnomalies.length);
    console.log('  - Available types:', availableTypes);
    console.log('  - All anomalies sample:', allAnomalies.slice(0, 3));
  }, [allAnomalies, selectedType, filteredAnomalies, availableTypes]);

  if (loading) {
    return (
      <div className="explorer-page">
        <div className="loading-container">
          <LoadingSpinner 
            size="large" 
            color="primary" 
            text="Đang tải dữ liệu bất thường..." 
          />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="explorer-page">
        <div className="explorer-header">
          <h1>🔍 Anomaly Explorer</h1>
          <p>Khám phá và phân tích các bất thường được phát hiện</p>
        </div>
        <p className="error-message">{error}</p>
        <button onClick={fetchAnomalies} className="retry-button">
          🔄 Thử lại
        </button>
      </div>
    );
  }

  return (
    <div className="explorer-page">
      <div className="explorer-header">
        <h1>🔍 Anomaly Explorer</h1>
        <p>Khám phá và phân tích các bất thường được phát hiện</p>
      </div>

      <div className="explorer-layout">
        {/* Sidebar phụ để chọn loại bất thường */}
        <nav className="explorer-sidebar">
          <h3>Loại Bất thường</h3>
          <ul>
            {availableTypes.length > 0 ? (
              availableTypes.map((typeKey) => {
                const displayName = ANOMALY_TYPES[typeKey] || typeKey;
                return (
                  <li key={typeKey} className={selectedType === typeKey ? 'active' : ''}>
                    <button onClick={() => {
                      setSelectedType(typeKey);
                      setSelectedAnomaly(null); // Reset lựa chọn chi tiết khi đổi loại
                    }}>
                      {displayName}
                    </button>
                  </li>
                );
              })
            ) : (
              <li>
                <span className="no-types-message">Không có dữ liệu</span>
              </li>
            )}
          </ul>
        </nav>

        {/* Nội dung chính */}
        <main className="explorer-content">
          <header className="explorer-content-header">
            <h2>{ANOMALY_TYPES[selectedType] || selectedType}</h2>
            <span className="anomaly-counter">
              Tìm thấy: {filteredAnomalies.length} kết quả
              {allAnomalies.length > 0 && (
                <span className="total-count"> (Tổng: {allAnomalies.length})</span>
              )}
            </span>
          </header>
          
          {/* Bảng dữ liệu */}
          <div className="explorer-table-container">
            {filteredAnomalies.length === 0 ? (
              <div className="empty-state">
                <div className="empty-icon">📭</div>
                <h3>Không tìm thấy bất thường nào</h3>
                <p>
                  {availableTypes.length === 0 
                    ? "Không có dữ liệu bất thường nào được tải từ API."
                    : `Không có bất thường nào thuộc loại "${ANOMALY_TYPES[selectedType] || selectedType}" được phát hiện.`
                  }
                </p>
                {availableTypes.length === 0 && (
                  <button onClick={fetchAnomalies} className="retry-button">
                    🔄 Tải lại dữ liệu
                  </button>
                )}
              </div>
            ) : (
              <table className="anomaly-table">
                <thead>
                  <tr>
                    <th>Thời gian</th>
                    <th>User</th>
                    <th>Điểm số</th>
                    <th>Query Preview</th>
                    <th>IP Client</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredAnomalies.map(anomaly => (
                    <tr 
                      key={anomaly.id} 
                      onClick={() => setSelectedAnomaly(anomaly)}
                      className={selectedAnomaly?.id === anomaly.id ? 'selected-row' : ''}
                    >
                      <td>{new Date(anomaly.timestamp).toLocaleString('vi-VN')}</td>
                      <td>
                        <span className="user-badge">{anomaly.user || 'N/A'}</span>
                      </td>
                      <td>
                        {anomaly.score !== null && anomaly.score !== undefined ? (
                          <span className={`score-badge score-${anomaly.score > 0.7 ? 'high' : anomaly.score > 0.4 ? 'medium' : 'low'}`}>
                            {anomaly.score.toFixed(4)}
                          </span>
                        ) : (
                          <span className="score-badge score-na">N/A</span>
                        )}
                      </td>
                      <td>
                        <code className="query-preview">
                          {anomaly.query ? (anomaly.query.length > 100 ? `${anomaly.query.slice(0, 100)}...` : anomaly.query) : 'N/A'}
                        </code>
                      </td>
                      <td>
                        <span className="ip-badge">{anomaly.client_ip || 'N/A'}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
          
          {/* Khu vực hiển thị chi tiết và các nút */}
          {selectedAnomaly && (
            <AnomalyDetail anomaly={selectedAnomaly} />
          )}
        </main>
      </div>
    </div>
  );
};

export default AnomalyExplorerPage;