// src/pages/ConfigurationPage.tsx
import React, { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import { API_URL, updateUserApiUrl } from '../config';
import './ConfigurationPage.css';

const ConfigurationPage: React.FC = () => {
  const [config, setConfig] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [apiUrl, setApiUrl] = useState<string>(API_URL);

  const fetchConfig = useCallback(async () => {
    try {
      const response = await axios.get(`${apiUrl}/api/engine/config`);
      setConfig(response.data);
    } catch (err) {
      setConfig(null); // Xóa config cũ nếu không thể tải
    } finally {
      setLoading(false);
    }
  }, [apiUrl]);

  useEffect(() => {
    fetchConfig();
  }, [fetchConfig]);

  const handleSave = async () => {
    if (!config) return;
    try {
      await axios.put(`${apiUrl}/api/engine/config`, config);
      alert("Lưu cấu hình thành công!");
    } catch (err) {
      alert("Lỗi khi lưu cấu hình.");
    }
  };

  const handleApiUrlSave = () => {
    updateUserApiUrl(apiUrl);
    alert("Đã lưu địa chỉ API! Thay đổi sẽ có hiệu lực sau khi làm mới trang.");
    window.location.reload();
  };
  
  const handleChange = (section: string, key: string, value: any) => {
    setConfig((prev: any) => ({ ...prev, [section]: { ...prev[section], [key]: value } }));
  };
  
  const handleTextAreaChange = (section: string, key: string, e: React.ChangeEvent<HTMLTextAreaElement>) => {
    handleChange(section, key, e.target.value.split('\n').map(item => item.trim()).filter(Boolean));
  };

  return (
    <div className="page-container">
      <h1>Cấu hình Hệ thống</h1>
      <div className="config-section">
        <h2>Kết nối API</h2>
        <label><span>Địa chỉ API Backend:</span><input type="text" value={apiUrl} onChange={(e) => setApiUrl(e.target.value)}/></label>
        <button onClick={handleApiUrlSave} className="save-button-small">Lưu & Áp dụng</button>
      </div>

      {loading && <p>Đang tải cấu hình engine...</p>}
      {config && (
        <>
          <div className="config-section">
            <h2>Cấu hình Engine</h2>
            <label><span>Khoảng thời gian nghỉ (giây):</span><input type="number" value={config.engine_sleep_interval_seconds || 60} onChange={(e) => setConfig({...config, engine_sleep_interval_seconds: parseInt(e.target.value)})}/></label>
          </div>
          <div className="config-section">
            <h2>Cấu hình Rules</h2>
            <label><span>Bảng Nhạy cảm (mỗi bảng một dòng):</span><textarea value={(config.analysis_params?.p_sensitive_tables || []).join('\n')} onChange={(e) => handleTextAreaChange('analysis_params', 'p_sensitive_tables', e)}/></label>
            <label><span>User được phép truy cập:</span><textarea value={(config.analysis_params?.p_allowed_users_sensitive || []).join('\n')} onChange={(e) => handleTextAreaChange('analysis_params', 'p_allowed_users_sensitive', e)}/></label>
          </div>
          <button onClick={handleSave} className="save-button">Lưu Cấu hình Engine</button>
        </>
      )}
    </div>
  );
};

export default ConfigurationPage;