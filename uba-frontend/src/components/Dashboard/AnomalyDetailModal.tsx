// src/components/Dashboard/AnomalyDetailModal.tsx
import React, { useState } from 'react';
import axios from 'axios';
import type { Anomaly } from '../../interfaces/Anomaly';
import './Dashboard.css';

const API_URL = 'http://127.0.0.1:8000';

interface AnomalyDetailModalProps {
  anomaly: Anomaly;
  onClose: () => void;
}

const AnomalyDetailModal: React.FC<AnomalyDetailModalProps> = ({ anomaly, onClose }) => {
  const [llmExplanation, setLlmExplanation] = useState<string | null>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [feedbackMessage, setFeedbackMessage] = useState<string | null>(null);

  const handleAnalyzeClick = async () => { /* ... (hàm này không đổi) ... */ };
  
  // === THÊM HÀM MỚI ĐỂ GỬI FEEDBACK ===
  const handleFeedbackSubmit = async (label: 0 | 1) => {
    try {
        setFeedbackMessage(null);
        const requestBody = {
            label: label,
            anomaly_data: anomaly,
        };
        const response = await axios.post(`${API_URL}/api/feedback/`, requestBody);
        setFeedbackMessage(response.data.message || "Gửi phản hồi thành công!");
    } catch (error) {
        setFeedbackMessage("Lỗi: Không thể gửi phản hồi.");
    }
  };

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        {/* ... (phần modal-header và modal-body không đổi) ... */}
        
        {/* === THÊM KHỐI FEEDBACK VÀO ĐÂY === */}
        <hr />
        <h3>Phản hồi của Chuyên gia</h3>
        <p>Phản hồi của bạn sẽ giúp hệ thống AI học và trở nên thông minh hơn.</p>
        <div className="feedback-buttons">
            <button onClick={() => handleFeedbackSubmit(1)} className="feedback-yes">
                Là bất thường ❌
            </button>
            <button onClick={() => handleFeedbackSubmit(0)} className="feedback-no">
                Là bình thường ✅
            </button>
        </div>
        {feedbackMessage && <p className="feedback-message">{feedbackMessage}</p>}
        
        <hr />
        
        <h3>Phân tích của AI (LLM)</h3>
        {/* ... (phần code nút phân tích AI và hiển thị kết quả không đổi) ... */}
      </div>
    </div>
  );
};

export default AnomalyDetailModal;