// src/components/Configuration/ConfigurationPage.tsx
import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { LoadingSpinner, Button } from '../UI';
import './Configuration.css';

const API_URL = 'http://127.0.0.1:8000';

const ConfigurationPage: React.FC = () => {
  const [config, setConfig] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const fetchConfig = async () => {
    try {
      setLoading(true);
      const response = await axios.get(`${API_URL}/api/engine/config`);
      setConfig(response.data);
    } catch (err) {
      setError("Không thể tải cấu hình từ API.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchConfig();
  }, []);

  const handleSave = async () => {
    if (!config) return;
    
    try {
      setSaving(true);
      setSuccessMessage(null);
      setError(null);
      
      await axios.put(`${API_URL}/api/engine/config`, config);
      setSuccessMessage("✅ Lưu cấu hình thành công! Engine sẽ áp dụng ở chu kỳ tiếp theo.");
      
      // Auto-hide success message after 5 seconds
      setTimeout(() => setSuccessMessage(null), 5000);
    } catch (err) {
      setError("❌ Lỗi khi lưu cấu hình. Vui lòng thử lại.");
    } finally {
      setSaving(false);
    }
  };
  
  const handleChange = (section: string, key: string, value: any) => {
    setConfig((prevConfig: any) => ({
      ...prevConfig,
      [section]: {
        ...prevConfig[section],
        [key]: value,
      },
    }));
  };
  
  const handleTextAreaChange = (section: string, key: string, event: React.ChangeEvent<HTMLTextAreaElement>) => {
    const list = event.target.value.split('\n').map(item => item.trim()).filter(Boolean);
    handleChange(section, key, list);
  };

  if (loading) {
    return (
      <div className="config-page">
        <div className="loading-container">
          <LoadingSpinner 
            size="large" 
            color="warning" 
            text="Đang tải cấu hình..." 
          />
        </div>
      </div>
    );
  }

  if (error && !config) {
    return (
      <div className="config-page">
        <div className="config-header">
          <h1>⚙️ Cấu hình Engine</h1>
          <p>Quản lý cài đặt và tham số của hệ thống phân tích</p>
        </div>
        <p className="error-message">{error}</p>
        <Button 
          variant="primary"
          size="large"
          onClick={fetchConfig}
          icon="🔄"
          iconPosition="left"
        >
          Thử lại
        </Button>
      </div>
    );
  }

  return (
    <div className="config-page">
      <div className="config-header">
        <h1>⚙️ Cấu hình Engine</h1>
        <p>Quản lý cài đặt và tham số của hệ thống phân tích</p>
      </div>
      
      {error && <p className="error-message">{error}</p>}
      {successMessage && <p className="success-message">{successMessage}</p>}
      
      <div className="config-section">
        <h2>Cấu hình Chung</h2>
        <label>
          <span>⏱️ Khoảng thời gian nghỉ (giây):</span>
          <input 
            type="number" 
            value={config?.engine_sleep_interval_seconds || 60}
            onChange={(e) => setConfig({...config, engine_sleep_interval_seconds: parseInt(e.target.value)})}
            min="10"
            max="3600"
          />
        </label>
      </div>

      <div className="config-section">
        <h2>Cấu hình Phân tích Rule-based</h2>
        {config?.analysis_params && (
          <>
            <label>
              <span>🚨 Bảng Nhạy cảm (mỗi bảng một dòng):</span>
              <textarea 
                value={config.analysis_params.p_sensitive_tables?.join('\n') || ''}
                onChange={(e) => handleTextAreaChange('analysis_params', 'p_sensitive_tables', e)}
                placeholder="users&#10;passwords&#10;credit_cards&#10;personal_info"
              />
            </label>
            <label>
              <span>✅ User được phép truy cập Bảng Nhạy cảm:</span>
              <textarea 
                value={config.analysis_params.p_allowed_users_sensitive?.join('\n') || ''}
                onChange={(e) => handleTextAreaChange('analysis_params', 'p_allowed_users_sensitive', e)}
                placeholder="admin&#10;security_team&#10;auditor"
              />
            </label>
            <label>
              <span>🌙 Giờ bắt đầu "Khuya" (0-23):</span>
              <input 
                type="number" 
                value={config.analysis_params.p_late_night_start || 22}
                onChange={(e) => handleChange('analysis_params', 'p_late_night_start', parseInt(e.target.value))}
                min="0"
                max="23"
              />
            </label>
            <label>
              <span>🌅 Giờ kết thúc "Khuya" (0-23):</span>
              <input 
                type="number" 
                value={config.analysis_params.p_late_night_end || 6}
                onChange={(e) => handleChange('analysis_params', 'p_late_night_end', parseInt(e.target.value))}
                min="0"
                max="23"
              />
            </label>
          </>
        )}
      </div>

      <div className="config-section">
        <h2>🔧 Cấu hình LLM - Ollama (Local)</h2>
        {config?.llm_config && (
          <>
            <label>
              <span>✅ Kích hoạt Ollama:</span>
              <input 
                type="checkbox" 
                checked={config.llm_config.enable_ollama || false}
                onChange={(e) => handleChange('llm_config', 'enable_ollama', e.target.checked)}
              />
            </label>
            <label>
              <span>🌐 Ollama Host:</span>
              <input 
                type="text" 
                value={config.llm_config.ollama_host || 'http://localhost:11434'}
                onChange={(e) => handleChange('llm_config', 'ollama_host', e.target.value)}
                placeholder="http://localhost:11434"
              />
            </label>
            <label>
              <span>🤖 Model:</span>
              <input 
                type="text" 
                value={config.llm_config.ollama_model || 'seneca:latest'}
                onChange={(e) => handleChange('llm_config', 'ollama_model', e.target.value)}
                placeholder="seneca:latest"
              />
            </label>
            <label>
              <span>⏱️ Timeout (giây):</span>
              <input 
                type="number" 
                value={config.llm_config.ollama_timeout || 3600}
                onChange={(e) => handleChange('llm_config', 'ollama_timeout', parseInt(e.target.value))}
                min="30"
                max="7200"
              />
            </label>
            <label>
              <span>🔄 Max Retries:</span>
              <input 
                type="number" 
                value={config.llm_config.ollama_max_retries || 3}
                onChange={(e) => handleChange('llm_config', 'ollama_max_retries', parseInt(e.target.value))}
                min="1"
                max="10"
              />
            </label>
            <label>
              <span>🌡️ Temperature:</span>
              <input 
                type="number" 
                value={config.llm_config.ollama_temperature || 0.7}
                onChange={(e) => handleChange('llm_config', 'ollama_temperature', parseFloat(e.target.value))}
                min="0"
                max="2"
                step="0.1"
              />
            </label>
            <label>
              <span>📝 Max Tokens:</span>
              <input 
                type="number" 
                value={config.llm_config.ollama_max_tokens || 4096}
                onChange={(e) => handleChange('llm_config', 'ollama_max_tokens', parseInt(e.target.value))}
                min="512"
                max="8192"
                step="512"
              />
            </label>
          </>
        )}
      </div>

      <div className="config-section">
        <h2>🌐 Cấu hình LLM - Third-Party AI APIs</h2>
        {config?.llm_config && (
          <>
            <div className="provider-selector">
              <label>
                <span>🤖 Chọn Provider:</span>
                <select 
                  value={config.llm_config.selected_provider || 'none'}
                  onChange={(e) => handleChange('llm_config', 'selected_provider', e.target.value)}
                >
                  <option value="none">Không sử dụng</option>
                  <option value="openai">OpenAI (GPT)</option>
                  <option value="anthropic">Anthropic (Claude)</option>
                  <option value="custom">Custom API</option>
                </select>
              </label>
            </div>

            {/* OpenAI Configuration */}
            {config.llm_config.selected_provider === 'openai' && (
              <div className="provider-config">
                <h3>🔑 OpenAI Configuration</h3>
                <label>
                  <span>API Key:</span>
                  <input 
                    type="password" 
                    value={config.llm_config.openai_api_key || ''}
                    onChange={(e) => handleChange('llm_config', 'openai_api_key', e.target.value)}
                    placeholder="sk-..."
                  />
                </label>
                <label>
                  <span>Model:</span>
                  <select 
                    value={config.llm_config.openai_model || 'gpt-3.5-turbo'}
                    onChange={(e) => handleChange('llm_config', 'openai_model', e.target.value)}
                  >
                    <option value="gpt-3.5-turbo">GPT-3.5 Turbo (Fast & Cost-effective)</option>
                    <option value="gpt-4">GPT-4 (High Quality)</option>
                    <option value="gpt-4-turbo">GPT-4 Turbo (Balanced)</option>
                  </select>
                </label>
                <label>
                  <span>Base URL:</span>
                  <input 
                    type="text" 
                    value={config.llm_config.openai_base_url || 'https://api.openai.com/v1'}
                    onChange={(e) => handleChange('llm_config', 'openai_base_url', e.target.value)}
                    placeholder="https://api.openai.com/v1"
                  />
                </label>
              </div>
            )}

            {/* Anthropic Configuration */}
            {config.llm_config.selected_provider === 'anthropic' && (
              <div className="provider-config">
                <h3>🔑 Anthropic Configuration</h3>
                <label>
                  <span>API Key:</span>
                  <input 
                    type="password" 
                    value={config.llm_config.anthropic_api_key || ''}
                    onChange={(e) => handleChange('llm_config', 'anthropic_api_key', e.target.value)}
                    placeholder="sk-ant-..."
                  />
                </label>
                <label>
                  <span>Model:</span>
                  <select 
                    value={config.llm_config.anthropic_model || 'claude-3-sonnet-20240229'}
                    onChange={(e) => handleChange('llm_config', 'anthropic_model', e.target.value)}
                  >
                    <option value="claude-3-haiku-20240307">Claude 3 Haiku (Fast)</option>
                    <option value="claude-3-sonnet-20240229">Claude 3 Sonnet (Balanced)</option>
                    <option value="claude-3-opus-20240229">Claude 3 Opus (High Quality)</option>
                  </select>
                </label>
              </div>
            )}

            {/* Custom API Configuration */}
            {config.llm_config.selected_provider === 'custom' && (
              <div className="provider-config">
                <h3>🔑 Custom API Configuration</h3>
                <label>
                  <span>API Key:</span>
                  <input 
                    type="password" 
                    value={config.llm_config.custom_api_key || ''}
                    onChange={(e) => handleChange('llm_config', 'custom_api_key', e.target.value)}
                    placeholder="Your API key..."
                  />
                </label>
                <label>
                  <span>API Endpoint:</span>
                  <input 
                    type="text" 
                    value={config.llm_config.custom_api_endpoint || ''}
                    onChange={(e) => handleChange('llm_config', 'custom_api_endpoint', e.target.value)}
                    placeholder="https://api.example.com/v1/chat/completions"
                  />
                </label>
                <label>
                  <span>Model Name:</span>
                  <input 
                    type="text" 
                    value={config.llm_config.custom_model_name || ''}
                    onChange={(e) => handleChange('llm_config', 'custom_model_name', e.target.value)}
                    placeholder="your-model-name"
                  />
                </label>
                <label>
                  <span>API Type:</span>
                  <select 
                    value={config.llm_config.custom_api_type || 'openai-compatible'}
                    onChange={(e) => handleChange('llm_config', 'custom_api_type', e.target.value)}
                  >
                    <option value="openai-compatible">OpenAI Compatible</option>
                    <option value="anthropic-compatible">Anthropic Compatible</option>
                    <option value="custom">Custom Format</option>
                  </select>
                </label>
              </div>
            )}

            {/* Common Settings for All Third-Party Providers */}
            {config.llm_config.selected_provider !== 'none' && (
              <div className="common-settings">
                <h3>⚙️ Common Settings</h3>
                <label>
                  <span>🌡️ Temperature:</span>
                  <input 
                    type="number" 
                    value={config.llm_config.api_temperature || 0.7}
                    onChange={(e) => handleChange('llm_config', 'api_temperature', parseFloat(e.target.value))}
                    min="0"
                    max="2"
                    step="0.1"
                  />
                </label>
                <label>
                  <span>📝 Max Tokens:</span>
                  <input 
                    type="number" 
                    value={config.llm_config.api_max_tokens || 4096}
                    onChange={(e) => handleChange('llm_config', 'api_max_tokens', parseInt(e.target.value))}
                    min="512"
                    max="8192"
                    step="512"
                  />
                </label>
                <label>
                  <span>⏱️ Timeout (giây):</span>
                  <input 
                    type="number" 
                    value={config.llm_config.api_timeout || 60}
                    onChange={(e) => handleChange('llm_config', 'api_timeout', parseInt(e.target.value))}
                    min="30"
                    max="300"
                  />
                </label>
              </div>
            )}
          </>
        )}
      </div>

      <div className="config-section">
        <h2>⚙️ Cấu hình Fallback & Priority</h2>
        {config?.llm_config && (
          <>
            <label>
              <span>🔄 Kích hoạt Fallback:</span>
              <input 
                type="checkbox" 
                checked={config.llm_config.enable_fallback || false}
                onChange={(e) => handleChange('llm_config', 'enable_fallback', e.target.checked)}
              />
            </label>
            <label>
              <span>📊 Số lần Fallback tối đa:</span>
              <input 
                type="number" 
                value={config.llm_config.max_fallback_attempts || 3}
                onChange={(e) => handleChange('llm_config', 'max_fallback_attempts', parseInt(e.target.value))}
                min="1"
                max="5"
              />
            </label>
            <label>
              <span>🎯 Thứ tự ưu tiên Provider:</span>
              <select 
                value={config.llm_config.provider_priority || 'ollama,api'}
                onChange={(e) => handleChange('llm_config', 'provider_priority', e.target.value)}
              >
                <option value="ollama,api">Ollama → Third-Party API</option>
                <option value="api,ollama">Third-Party API → Ollama</option>
                <option value="ollama">Chỉ Ollama (Không fallback)</option>
                <option value="api">Chỉ Third-Party API (Không fallback)</option>
              </select>
            </label>
            <div className="priority-info">
              <p>💡 <strong>Fallback Logic:</strong></p>
              <ul>
                <li><strong>Ollama → API:</strong> Ưu tiên Ollama, nếu lỗi thì dùng API</li>
                <li><strong>API → Ollama:</strong> Ưu tiên API, nếu lỗi thì dùng Ollama</li>
                <li><strong>Chỉ Ollama:</strong> Không sử dụng fallback</li>
                <li><strong>Chỉ API:</strong> Không sử dụng fallback</li>
              </ul>
            </div>
          </>
        )}
      </div>
      
      <Button 
        variant="primary"
        size="large"
        onClick={handleSave} 
        loading={saving}
        icon="💾"
        iconPosition="left"
        className="save-button"
      >
        {saving ? 'Đang lưu...' : 'Lưu Cấu hình'}
      </Button>
    </div>
  );
};

export default ConfigurationPage;