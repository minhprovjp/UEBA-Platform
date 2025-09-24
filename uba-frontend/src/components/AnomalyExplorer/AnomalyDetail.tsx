
// src/components/AnomalyExplorer/AnomalyDetail.tsx
import React, { useState } from 'react';
import axios from 'axios';
import type { Anomaly } from '@/interfaces/Anomaly';
import './AnomalyExplorer.css';
import { API_URL } from '@/config';

interface AnomalyDetailProps {
  anomaly: Anomaly;
}

const AnomalyDetail: React.FC<AnomalyDetailProps> = ({ anomaly }) => {
  const [llmExplanation, setLlmExplanation] = useState<string | null>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [feedbackMessage, setFeedbackMessage] = useState<string | null>(null);

  const handleAnalyzeClick = async () => {
    setIsAnalyzing(true);
    setLlmExplanation(null);
    try {
      const response = await axios.post(`${API_URL}/api/llm/analyze-anomaly`, {
        timestamp: anomaly.timestamp, user: anomaly.user, query: anomaly.query,
        anomaly_type: anomaly.anomaly_type, score: anomaly.score, reason: anomaly.reason,
      });
      setLlmExplanation(response.data.explanation);
    } catch (error) {
      setLlmExplanation("Lỗi: Không thể nhận phân tích từ AI.");
      console.error(error);
    } finally {
      setIsAnalyzing(false);
    }
  };

  const handleFeedbackSubmit = async (label: 0 | 1) => {
    setFeedbackMessage(null);
    try {
      const response = await axios.post(`${API_URL}/api/feedback/`, { label, anomaly_data: anomaly });
      setFeedbackMessage(response.data.message || `Đã ghi nhận phản hồi.`);
    } catch (error) {
      setFeedbackMessage("Lỗi: Không thể gửi phản hồi.");
      console.error(error);
    }
  };

  return (
    <div className="anomaly-detail">
      <h4>Chi tiết Bất thường #{anomaly.id}</h4>
      <div className="detail-metadata">
          <span><strong>IP:</strong> {anomaly.client_ip || 'N/A'}</span>
          <span><strong>Database:</strong> {anomaly.database || 'N/A'}</span>
      </div>
      <p><strong>Câu lệnh SQL:</strong></p>
      <pre><code>{anomaly.query}</code></pre>
      
      <div className="interaction-section">
        <div className="feedback-section">
          <h5>Phản hồi của Chuyên gia</h5>
          <div className="feedback-buttons">
            <button onClick={() => handleFeedbackSubmit(1)} className="feedback-yes">Là bất thường ❌</button>
            <button onClick={() => handleFeedbackSubmit(0)} className="feedback-no">Là bình thường ✅</button>
          </div>
          {feedbackMessage && <p className="feedback-message">{feedbackMessage}</p>}
        </div>
        
        <div className="llm-section">
          <h5>Phân tích Chuyên sâu với AI</h5>
          <button onClick={handleAnalyzeClick} disabled={isAnalyzing}>
            {isAnalyzing ? 'Đang phân tích...' : '🤖 Yêu cầu AI Phân tích'}
          </button>
          {llmExplanation && <div className="llm-explanation">{llmExplanation}</div>}
        </div>
      </div>
    </div>
  );
};

export default AnomalyDetail;