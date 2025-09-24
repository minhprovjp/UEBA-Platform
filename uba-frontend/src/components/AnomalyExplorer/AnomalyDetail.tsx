// src/components/AnomalyExplorer/AnomalyDetail.tsx

import React, { useState } from 'react';
import axios from 'axios';
import type { Anomaly } from '../../interfaces/Anomaly';
import './AnomalyExplorer.css';
import { API_URL } from '../../config';


interface AnomalyDetailProps {
  anomaly: Anomaly;
}

const AnomalyDetail: React.FC<AnomalyDetailProps> = ({ anomaly }) => {
  const [llmExplanation, setLlmExplanation] = useState<string | null>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [feedbackMessage, setFeedbackMessage] = useState<string | null>(null);

  // Hàm gọi API để LLM phân tích
  const handleAnalyzeClick = async () => {
    setIsAnalyzing(true);
    setLlmExplanation(null);
    try {
      // API endpoint này cần được định nghĩa trong main_api.py
      const response = await axios.post(`${API_URL}/api/llm/analyze-anomaly`, {
        // Gửi các trường cần thiết mà API yêu cầu
        timestamp: anomaly.timestamp,
        user: anomaly.user,
        query: anomaly.query,
        anomaly_type: anomaly.anomaly_type,
        score: anomaly.score,
        reason: anomaly.reason,
      });
      
      // Handle the complex LLM response structure
      let explanation = '';
      
      if (response.data.final_analysis) {
        // Use the final analysis from the dual-round system
        const analysis = response.data.final_analysis;
        explanation = `🤖 **AI Analysis Results**\n\n` +
          `📊 **Anomaly Type:** ${analysis.anomaly_type || 'N/A'}\n` +
          `⚠️ **Is Anomalous:** ${analysis.is_anomalous ? 'Yes' : 'No'}\n` +
          `🎯 **Confidence Score:** ${analysis.confidence_score || 'N/A'}\n` +
          `🚨 **Security Risk Level:** ${analysis.security_risk_level || 'N/A'}\n` +
          `⚡ **Performance Impact:** ${analysis.performance_impact || 'N/A'}\n\n` +
          `📝 **Summary:** ${analysis.summary || 'N/A'}\n\n` +
          `🔍 **Detailed Analysis:** ${analysis.detailed_analysis || 'N/A'}\n\n` +
          `💡 **Recommendation:** ${analysis.recommendation || 'N/A'}\n\n` +
          `🏷️ **Tags:** ${(analysis.tags || []).join(', ')}\n\n` +
          `🔄 **Providers Used:** ${(response.data.providers_used || []).join(' → ')}`;
      } else if (response.data.message && response.data.message.content) {
        // Fallback for simple response format
        explanation = response.data.message.content;
      } else {
        // Fallback for any other response format
        explanation = JSON.stringify(response.data, null, 2);
      }
      
      setLlmExplanation(explanation);
    } catch (error: any) {
      console.error('LLM Analysis Error:', error);
      if (error.response) {
        setLlmExplanation(`❌ **Error:** ${error.response.status} - ${error.response.data?.detail || 'Unknown error'}`);
      } else if (error.request) {
        setLlmExplanation("❌ **Error:** No response received from server. Please check if the backend is running.");
      } else {
        setLlmExplanation(`❌ **Error:** ${error.message || 'Unknown error occurred'}`);
      }
    } finally {
      setIsAnalyzing(false);
    }
  };

  // Hàm gọi API để gửi feedback
  const handleFeedbackSubmit = async (label: 0 | 1) => {
    setFeedbackMessage(null);
    try {
      const requestBody = {
        label: label,
        anomaly_data: anomaly,
      };
      const response = await axios.post(`${API_URL}/api/feedback/`, requestBody);
      setFeedbackMessage(response.data.message || `Đã ghi nhận phản hồi.`);
    } catch (error) {
      setFeedbackMessage("Lỗi: Không thể gửi phản hồi.");
      console.error(error);
    }
  };

  return (
    <div className="anomaly-detail">
      <hr />
      <h4>Chi tiết Bất thường #{anomaly.id}</h4>
      
      {/* Thông tin metadata */}
      <div className="detail-metadata">
          <span><strong>User:</strong> {anomaly.user}</span>
          <span><strong>IP:</strong> {anomaly.client_ip || 'N/A'}</span>
          <span><strong>Database:</strong> {anomaly.database || 'N/A'}</span>
      </div>
      
      {/* Câu lệnh Query */}
      <p><strong>Câu lệnh SQL:</strong></p>
      <pre><code>{anomaly.query}</code></pre>
      
      {/* Nút Phản hồi */}
      <div className="feedback-section">
        <h5>Phản hồi của Chuyên gia</h5>
        <p>Phản hồi của bạn sẽ giúp hệ thống AI học và trở nên thông minh hơn.</p>
        <div className="feedback-buttons">
          <button onClick={() => handleFeedbackSubmit(1)} className="feedback-yes">Là bất thường ❌</button>
          <button onClick={() => handleFeedbackSubmit(0)} className="feedback-no">Là bình thường ✅</button>
        </div>
        {feedbackMessage && <p className="feedback-message">{feedbackMessage}</p>}
      </div>
      
      {/* Nút Phân tích AI */}
      <div className="llm-section">
          <h5>Phân tích Chuyên sâu với AI</h5>
          <button onClick={handleAnalyzeClick} disabled={isAnalyzing}>
            {isAnalyzing ? 'Đang phân tích...' : '🤖 Yêu cầu AI Phân tích'}
          </button>
          {llmExplanation && (
            <div className="llm-explanation">
              {llmExplanation}
            </div>
          )}
      </div>
    </div>
  );
};

export default AnomalyDetail;