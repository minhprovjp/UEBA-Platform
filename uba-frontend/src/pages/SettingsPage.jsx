// uba_frontend/src/pages/SettingsPage.jsx
import React, { useState, useEffect } from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Toaster, toast } from "sonner";
import { useConfig, useUpdateConfigMutation } from '@/api/queries';
import { Save, Settings, Shield, BrainCircuit, List, Clock } from 'lucide-react';

export default function SettingsPage() {
  const [localConfig, setLocalConfig] = useState(null);
  const [isDirty, setIsDirty] = useState(false);
  
  const { data: config, isLoading } = useConfig();
  const updateConfigMutation = useUpdateConfigMutation();

  useEffect(() => {
    if (config) {
      setLocalConfig(JSON.parse(JSON.stringify(config))); // Deep copy
      setIsDirty(false);
    }
  }, [config]);

  // --- Hàm cập nhật giá trị nested ---
  // Ví dụ: updateField('llm_config.ollama_host', 'http://...')
  const updateField = (path, value) => {
    setLocalConfig(prev => {
      const newData = { ...prev };
      const keys = path.split('.');
      let current = newData;
      for (let i = 0; i < keys.length - 1; i++) {
        if (!current[keys[i]]) current[keys[i]] = {};
        current = current[keys[i]];
      }
      current[keys[keys.length - 1]] = value;
      return newData;
    });
    setIsDirty(true);
  };

  // Hàm xử lý mảng (nhập dấu phẩy -> convert sang array)
  const updateArrayField = (path, stringValue) => {
    const arr = stringValue.split(',').map(s => s.trim()).filter(Boolean);
    updateField(path, arr);
  };

  const handleSave = () => {
    updateConfigMutation.mutate(localConfig, {
        onSuccess: () => setIsDirty(false)
    });
  };

  if (isLoading || !localConfig) return <div className="p-10 text-center text-zinc-500">Loading configuration...</div>;

  // Helpers để lấy value an toàn (tránh crash nếu key thiếu)
  const getVal = (path, fallback = "") => {
    return path.split('.').reduce((acc, key) => (acc && acc[key] !== undefined) ? acc[key] : undefined, localConfig) ?? fallback;
  };

  return (
    <div className="h-full flex flex-col gap-4 overflow-hidden pr-2">
      <Toaster position="top-right" theme="dark" />
      
      {/* HEADER */}
      <header className="shrink-0 flex justify-between items-center pb-4 border-b border-zinc-800">
        <div>
          <h2 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
            <Settings className="w-6 h-6 text-primary-500"/> Engine Configuration
          </h2>
          <p className="text-zinc-400 text-sm mt-1">
            Fine-tune detection rules, thresholds, and AI models.
          </p>
        </div>
        <Button 
            onClick={handleSave} 
            disabled={!isDirty || updateConfigMutation.isPending}
            className={`min-w-[120px] ${isDirty ? 'bg-primary-600 hover:bg-primary-700' : 'bg-zinc-800 text-zinc-500'}`}
        >
            <Save className="w-4 h-4 mr-2"/>
            {updateConfigMutation.isPending ? "Saving..." : (isDirty ? "Save Changes" : "Saved")}
        </Button>
      </header>

      {/* TABS CONTENT */}
      <div className="flex-1 min-h-0 overflow-hidden">
        <Tabs defaultValue="analysis" className="h-full flex flex-col">
            <TabsList className="bg-zinc-900 border border-zinc-800 w-full justify-start rounded-lg p-1 h-auto mb-4">
                <TabsTrigger value="analysis" className="data-[state=active]:bg-zinc-800"><Clock className="w-4 h-4 mr-2"/> Time & Behavior</TabsTrigger>
                <TabsTrigger value="thresholds" className="data-[state=active]:bg-zinc-800"><Shield className="w-4 h-4 mr-2"/> Security Thresholds</TabsTrigger>
                <TabsTrigger value="signatures" className="data-[state=active]:bg-zinc-800"><List className="w-4 h-4 mr-2"/> Signatures & Lists</TabsTrigger>
                <TabsTrigger value="llm" className="data-[state=active]:bg-zinc-800"><BrainCircuit className="w-4 h-4 mr-2"/> AI & LLM</TabsTrigger>
            </TabsList>

            <div className="flex-1 overflow-y-auto custom-scrollbar pr-2 pb-10">
                
                {/* === TAB 1: ANALYSIS PARAMS === */}
                <TabsContent value="analysis" className="space-y-4 mt-0">
                    <div className="grid grid-cols-2 gap-4">
                        <ConfigCard title="Analysis Parameters (Heuristics)" desc="Cấu hình khung thời gian và giới hạn phân tích hành vi.">
                            <div className="grid grid-cols-2 gap-4">
                                <FormItem label="Time Window (minutes)" desc="Cửa sổ gom nhóm log">
                                    <Input type="number" value={getVal('analysis_params.p_time_window_minutes')} 
                                           onChange={e => updateField('analysis_params.p_time_window_minutes', parseInt(e.target.value))}/>
                                </FormItem>
                                <FormItem label="Distinct Tables Threshold" desc="Số bảng tối thiểu để tính là Multi-table">
                                    <Input type="number" value={getVal('analysis_params.p_min_distinct_tables')} 
                                           onChange={e => updateField('analysis_params.p_min_distinct_tables', parseInt(e.target.value))}/>
                                </FormItem>
                                <FormItem label="Safe Hours Start" desc="Giờ bắt đầu làm việc (0-23)">
                                    <Input type="number" value={getVal('analysis_params.p_safe_hours_start')} 
                                           onChange={e => updateField('analysis_params.p_safe_hours_start', parseInt(e.target.value))}/>
                                </FormItem>
                                <FormItem label="Safe Hours End" desc="Giờ kết thúc làm việc (0-23)">
                                    <Input type="number" value={getVal('analysis_params.p_safe_hours_end')} 
                                           onChange={e => updateField('analysis_params.p_safe_hours_end', parseInt(e.target.value))}/>
                                </FormItem>
                            </div>
                        </ConfigCard>

                        <ConfigCard title="Late Night Definition" desc="Định nghĩa khung giờ khuya để phát hiện bất thường.">
                             <div className="grid grid-cols-2 gap-4">
                                <FormItem label="Start Time (HH:MM)">
                                    <Input value={getVal('analysis_params.p_late_night_start_time')} 
                                           onChange={e => updateField('analysis_params.p_late_night_start_time', e.target.value)}/>
                                </FormItem>
                                <FormItem label="End Time (HH:MM)">
                                    <Input value={getVal('analysis_params.p_late_night_end_time')} 
                                           onChange={e => updateField('analysis_params.p_late_night_end_time', e.target.value)}/>
                                </FormItem>
                             </div>
                             <div className="mt-4 p-3 bg-zinc-900/50 rounded border border-zinc-800 text-xs text-zinc-400">
                                Global Config:
                                <br/>Engine Sleep Interval: {getVal('engine_sleep_interval_seconds')}s
                             </div>
                        </ConfigCard>
                    </div>
                </TabsContent>

                {/* === TAB 2: SECURITY THRESHOLDS === */}
                <TabsContent value="thresholds" className="space-y-4 mt-0">
                    <ConfigCard title="Detection Thresholds" desc="Ngưỡng kích hoạt các rule bảo mật cứng (Hard Rules).">
                        <div className="grid grid-cols-3 gap-6">
                            <FormItem label="Mass Deletion (rows)" desc="Số dòng tối đa được xóa">
                                <Input type="number" value={getVal('security_rules.thresholds.mass_deletion_rows')} 
                                       onChange={e => updateField('security_rules.thresholds.mass_deletion_rows', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Max Execution Time (ms)" desc="Phát hiện DoS/Slow query">
                                <Input type="number" value={getVal('security_rules.thresholds.execution_time_limit_ms')} 
                                       onChange={e => updateField('security_rules.thresholds.execution_time_limit_ms', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Brute Force Attempts" desc="Số lần đăng nhập sai tối đa/phút">
                                <Input type="number" value={getVal('security_rules.thresholds.brute_force_attempts')} 
                                       onChange={e => updateField('security_rules.thresholds.brute_force_attempts', parseInt(e.target.value))}/>
                            </FormItem>
                            
                            <FormItem label="Concurrent IPs" desc="Số IP tối đa cho 1 user">
                                <Input type="number" value={getVal('security_rules.thresholds.concurrent_ips_limit')} 
                                       onChange={e => updateField('security_rules.thresholds.concurrent_ips_limit', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Max CPU Time (ms)" desc="Ngưỡng CPU cao">
                                <Input type="number" value={getVal('security_rules.thresholds.cpu_time_limit_ms')} 
                                       onChange={e => updateField('security_rules.thresholds.cpu_time_limit_ms', parseInt(e.target.value))}/>
                            </FormItem>
                             <FormItem label="Max Query Entropy" desc="Ngưỡng độ hỗn loạn chuỗi (4.0 - 6.0)">
                                <Input type="number" step="0.1" value={getVal('security_rules.thresholds.max_query_entropy')} 
                                       onChange={e => updateField('security_rules.thresholds.max_query_entropy', parseFloat(e.target.value))}/>
                            </FormItem>
                            
                            <FormItem label="Impossible Travel (km/h)" desc="Vận tốc di chuyển tối đa">
                                <Input type="number" value={getVal('security_rules.thresholds.impossible_travel_speed_kmh')} 
                                       onChange={e => updateField('security_rules.thresholds.impossible_travel_speed_kmh', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Scan Efficiency Min" desc="Tỷ lệ Rows Returned / Examined (0-1)">
                                <Input type="number" step="0.01" value={getVal('security_rules.thresholds.scan_efficiency_min')} 
                                       onChange={e => updateField('security_rules.thresholds.scan_efficiency_min', parseFloat(e.target.value))}/>
                            </FormItem>
                        </div>
                    </ConfigCard>
                </TabsContent>

                {/* === TAB 3: SIGNATURES & LISTS === */}
                <TabsContent value="signatures" className="space-y-4 mt-0">
                    <div className="grid grid-cols-2 gap-4">
                        <ConfigCard title="Sensitive Data Definition" desc="Định nghĩa các bảng và user nhạy cảm.">
                             <FormItem label="Sensitive Tables" desc="Các bảng chứa dữ liệu quan trọng (comma separated)">
                                <Textarea className="h-24 font-mono text-xs" 
                                    value={getVal('analysis_params.p_sensitive_tables', []).join(', ')} 
                                    onChange={e => updateArrayField('analysis_params.p_sensitive_tables', e.target.value)}/>
                            </FormItem>
                            <FormItem label="Allowed Users for Sensitive Data" desc="User được phép truy cập bảng nhạy cảm">
                                <Textarea className="h-24 font-mono text-xs" 
                                    value={getVal('analysis_params.p_allowed_users_sensitive', []).join(', ')} 
                                    onChange={e => updateArrayField('analysis_params.p_allowed_users_sensitive', e.target.value)}/>
                            </FormItem>
                        </ConfigCard>

                        <ConfigCard title="Attack Signatures" desc="Các từ khóa để phát hiện tấn công.">
                             <FormItem label="SQL Injection Keywords" desc="Danh sách đen các từ khóa SQLi">
                                <Textarea className="h-32 font-mono text-xs" 
                                    value={getVal('security_rules.signatures.sqli_keywords', []).join(', ')} 
                                    onChange={e => updateArrayField('security_rules.signatures.sqli_keywords', e.target.value)}/>
                            </FormItem>
                            <FormItem label="Admin/Privilege Keywords" desc="Từ khóa liên quan đến thay đổi quyền">
                                <Textarea className="h-20 font-mono text-xs" 
                                    value={getVal('security_rules.signatures.admin_keywords', []).join(', ')} 
                                    onChange={e => updateArrayField('security_rules.signatures.admin_keywords', e.target.value)}/>
                            </FormItem>
                        </ConfigCard>
                        
                         <ConfigCard title="Tooling Blacklist/Whitelist" desc="Kiểm soát client truy cập.">
                            <FormItem label="Disallowed Programs (Blacklist)" desc="Các tool bị cấm (sqlmap, nmap...)">
                                <Textarea className="h-20 font-mono text-xs" 
                                    value={getVal('security_rules.signatures.disallowed_programs', []).join(', ')} 
                                    onChange={e => updateArrayField('security_rules.signatures.disallowed_programs', e.target.value)}/>
                            </FormItem>
                        </ConfigCard>
                    </div>
                </TabsContent>

                {/* === TAB 4: AI & LLM === */}
                <TabsContent value="llm" className="space-y-4 mt-0">
                    <ConfigCard title="LLM Provider Settings" desc="Cấu hình kết nối tới các mô hình ngôn ngữ lớn.">
                        <div className="flex items-center space-x-2 mb-4">
                            <Switch id="ollama-mode" 
                                checked={getVal('llm_config.enable_ollama')}
                                onCheckedChange={c => updateField('llm_config.enable_ollama', c)}
                            />
                            <Label htmlFor="ollama-mode" className="text-white">Enable Local Ollama (Priority)</Label>
                        </div>
                        
                        <div className="grid grid-cols-2 gap-4">
                            <FormItem label="Ollama Host">
                                <Input value={getVal('llm_config.ollama_host')} 
                                       onChange={e => updateField('llm_config.ollama_host', e.target.value)}/>
                            </FormItem>
                             <FormItem label="Ollama Model">
                                <Input value={getVal('llm_config.ollama_model')} 
                                       onChange={e => updateField('llm_config.ollama_model', e.target.value)}/>
                            </FormItem>
                        </div>

                        <div className="border-t border-zinc-800 my-4 pt-4">
                            <h4 className="text-sm font-semibold text-zinc-400 mb-3">Cloud Providers (Fallback)</h4>
                            <div className="grid grid-cols-1 gap-4">
                                <FormItem label="OpenAI API Key">
                                    <Input type="password" placeholder="sk-..." 
                                           value={getVal('llm_config.openai_api_key')} 
                                           onChange={e => updateField('llm_config.openai_api_key', e.target.value)}/>
                                </FormItem>
                                <FormItem label="Anthropic API Key">
                                    <Input type="password" placeholder="sk-ant-..." 
                                           value={getVal('llm_config.anthropic_api_key')} 
                                           onChange={e => updateField('llm_config.anthropic_api_key', e.target.value)}/>
                                </FormItem>
                            </div>
                        </div>
                    </ConfigCard>
                    
                    <ConfigCard title="Analysis Strategy" desc="Cấu hình chiến lược phân tích của AI.">
                        <div className="flex items-center space-x-4">
                             <div className="flex items-center space-x-2">
                                <Switch id="two-round" 
                                    checked={getVal('llm_analysis_settings.enable_two_round_analysis')}
                                    onCheckedChange={c => updateField('llm_analysis_settings.enable_two_round_analysis', c)}
                                />
                                <Label htmlFor="two-round">Two-Round Analysis (Accuracy)</Label>
                            </div>
                            <div className="flex items-center space-x-2">
                                <Switch id="session-analysis" 
                                    checked={getVal('llm_analysis_settings.enable_session_analysis')}
                                    onCheckedChange={c => updateField('llm_analysis_settings.enable_session_analysis', c)}
                                />
                                <Label htmlFor="session-analysis">Session Context Awareness</Label>
                            </div>
                        </div>
                         <div className="mt-4 max-w-xs">
                             <FormItem label="Confidence Threshold (0.0 - 1.0)">
                                <Input type="number" step="0.1" max="1" min="0" 
                                       value={getVal('llm_analysis_settings.confidence_threshold')} 
                                       onChange={e => updateField('llm_analysis_settings.confidence_threshold', parseFloat(e.target.value))}/>
                            </FormItem>
                        </div>
                    </ConfigCard>
                </TabsContent>

            </div>
        </Tabs>
      </div>
    </div>
  );
}

// === Sub-components ===

const ConfigCard = ({ title, desc, children }) => (
  <Card className="bg-zinc-950/40 border-zinc-800">
    <CardHeader className="pb-3">
      <CardTitle className="text-base font-semibold text-zinc-100">{title}</CardTitle>
      {desc && <CardDescription className="text-xs">{desc}</CardDescription>}
    </CardHeader>
    <CardContent>
      {children}
    </CardContent>
  </Card>
);

const FormItem = ({ label, desc, children }) => (
  <div className="space-y-1.5">
    <Label className="text-xs font-medium text-zinc-400">{label}</Label>
    {children}
    {desc && <p className="text-[10px] text-zinc-600">{desc}</p>}
  </div>
);