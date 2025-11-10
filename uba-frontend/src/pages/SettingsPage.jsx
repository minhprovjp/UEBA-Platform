// src/pages/SettingsPage.jsx
import React, { useState, useEffect } from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Toaster } from "sonner"; // Import thư viện thông báo
import { useConfig, useUpdateConfigMutation } from '@/api/queries'; // Import hooks mới

export default function SettingsPage() {
  // Dùng state riêng để chỉnh sửa, không sửa trực tiếp data của useQuery
  const [localConfig, setLocalConfig] = useState(null);
  
  // 1. Tải Cấu hình
  const { data: config, isLoading } = useConfig();

  // 2. Lấy hook để Lưu Cấu hình
  const updateConfigMutation = useUpdateConfigMutation();

  // 3. Đồng bộ data từ server vào state local khi tải xong
  useEffect(() => {
    if (config) {
      setLocalConfig(config);
    }
  }, [config]);

  // 4. Lưu Cấu hình
  const handleSaveConfig = () => {
    updateConfigMutation.mutate(localConfig);
  };

  // 5. Hàm helper để cập nhật state
  const handleAnalysisParamChange = (key, value) => {
    setLocalConfig(prevConfig => ({
      ...prevConfig,
      analysis_params: {
        ...prevConfig.analysis_params,
        [key]: value
      }
    }));
  };
  
  const handleLLMParamChange = (key, value) => {
    setLocalConfig(prevConfig => ({
      ...prevConfig,
      llm_config: {
        ...prevConfig.llm_config,
        [key]: value
      }
    }));
  };

  if (isLoading || !localConfig) return <div>Đang tải cấu hình...</div>;

  const analysisParams = localConfig.analysis_params || {};
  const llmConfig = localConfig.llm_config || {};

  return (
    <>
      <Toaster position="top-right" theme="dark" />
      <div className="h-full flex flex-col">
        <header className="flex justify-between items-center mb-4">
          <div>
            <h2 className="text-2xl font-semibold">Engine Settings</h2>
            <p className="text-muted-foreground">Tinh chỉnh các luật và cài đặt của hệ thống phân tích.</p>
          </div>
          <Button 
            onClick={handleSaveConfig} 
            className="bg-primary-600 hover:bg-primary-700"
            disabled={updateConfigMutation.isPending} // Tắt nút khi đang lưu
          >
            {updateConfigMutation.isPending ? "Đang lưu..." : "Lưu Cấu hình"}
          </Button>
        </header>

        {/* Bố cục lưới (Giống "Systems Monitor") */}
        <div className="grid grid-cols-2 gap-6">
          {/* Cột 1: Các luật Phân tích */}
          <ConfigCard title="Analysis Rules (Luật 1, 3, 4)">
            <ConfigInput 
              label="Late Night Start (HH:MM)" 
              value={analysisParams.p_late_night_start_time || "00:00"}
              onChange={e => handleAnalysisParamChange('p_late_night_start_time', e.target.value)}
            />
            <ConfigInput 
              label="Late Night End (HH:MM)" 
              value={analysisParams.p_late_night_end_time || "05:00"}
              onChange={e => handleAnalysisParamChange('p_late_night_end_time', e.target.value)}
            />
            <ConfigInput 
              label="Time Window (minutes)" 
              type="number"
              value={analysisParams.p_time_window_minutes || 5}
              onChange={e => handleAnalysisParamChange('p_time_window_minutes', parseInt(e.target.value))}
            />
            <ConfigInput 
              label="Distinct Tables Threshold" 
              type="number"
              value={analysisParams.p_min_distinct_tables || 3}
              onChange={e => handleAnalysisParamChange('p_min_distinct_tables', parseInt(e.target.value))}
            />
            <ConfigInput 
              label="Sensitive Tables (cách nhau bởi dấu phẩy)" 
              value={(analysisParams.p_sensitive_tables || []).join(', ')}
              onChange={e => handleAnalysisParamChange('p_sensitive_tables', e.target.value.split(',').map(s => s.trim()))}
            />
            <ConfigInput 
              label="Allowed Users (cách nhau bởi dấu phẩy)" 
              value={(analysisParams.p_allowed_users_sensitive || []).join(', ')}
              onChange={e => handleAnalysisParamChange('p_allowed_users_sensitive', e.target.value.split(',').map(s => s.trim()))}
            />
          </ConfigCard>

          {/* Cột 2: Cấu hình LLM */}
          <ConfigCard title="LLM & AI Settings">
             <ConfigInput 
              label="Ollama Host" 
              value={llmConfig.ollama_host || "http://localhost:11434"}
              onChange={e => handleLLMParamChange('ollama_host', e.target.value)}
            />
             <ConfigInput 
              label="OpenAI API Key" 
              type="password"
              value={llmConfig.openai_api_key || ""}
              onChange={e => handleLLMParamChange('openai_api_key', e.target.value)}
            />
             <ConfigInput 
              label="Anthropic API Key" 
              type="password"
              value={llmConfig.anthropic_api_key || ""}
              onChange={e => handleLLMParamChange('anthropic_api_key', e.target.value)}
            />
          </ConfigCard>
        </div>
      </div>
    </>
  );
}

// (Các component ConfigCard và ConfigInput giữ nguyên như cũ)
const ConfigCard = ({ title, children }) => (
  <div className="bg-zinc-900 border border-border rounded-lg p-6">
    <h3 className="text-xl font-semibold mb-4 text-primary-500">{title}</h3>
    <div className="space-y-4">
      {children}
    </div>
  </div>
);

const ConfigInput = ({ label, value, onChange, type = "text" }) => (
  <div>
    <label className="text-sm font-medium text-muted-foreground">{label}</label>
    <Input 
      type={type} 
      value={value} 
      onChange={onChange} 
      className="bg-zinc-800 border-zinc-700 mt-1" 
    />
  </div>
);