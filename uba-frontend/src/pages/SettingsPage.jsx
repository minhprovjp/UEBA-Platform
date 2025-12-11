// uba_frontend/src/pages/SettingsPage.jsx
import React, { useState, useEffect } from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent, CardDescription, CardHeader, CardTitle, CardFooter } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Toaster, toast } from "sonner";
import { useConfig, useUpdateConfigMutation } from '@/api/queries';
import { apiClient } from '@/api/client';
import { Save, Settings, Shield, BrainCircuit, List, Clock, Key, LogOut, User, Activity } from 'lucide-react';

export default function SettingsPage() {
  const [localConfig, setLocalConfig] = useState(null);
  const [isDirty, setIsDirty] = useState(false);
  
  // State cho Change Password
  const [passForm, setPassForm] = useState({ currentPassword: '', newPassword: '', confirmPassword: '' });
  const [isPassLoading, setIsPassLoading] = useState(false);

  const { data: config, isLoading } = useConfig();
  const updateConfigMutation = useUpdateConfigMutation();

  useEffect(() => {
    if (config) {
      setLocalConfig(JSON.parse(JSON.stringify(config)));
      setIsDirty(false);
    }
  }, [config]);

  // Helper để update nested object an toàn
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

  // Helper để update mảng (từ string text area)
  const updateArrayField = (path, stringValue) => {
    const arr = stringValue.split(',').map(s => s.trim()).filter(Boolean);
    updateField(path, arr);
  };

  const handleSave = () => {
    updateConfigMutation.mutate(localConfig, {
        onSuccess: () => setIsDirty(false)
    });
  };

  // Logic Đổi Mật Khẩu
  const handleChangePassword = async (e) => {
    e.preventDefault();
    if (!passForm.currentPassword) { toast.error("Vui lòng nhập mật khẩu hiện tại."); return; }
    if (passForm.newPassword !== passForm.confirmPassword) { toast.error("Mật khẩu xác nhận không khớp!"); return; }
    if (passForm.newPassword.length < 6) { toast.error("Mật khẩu phải có ít nhất 6 ký tự."); return; }

    setIsPassLoading(true);
    try {
        await apiClient.post('/api/change-password', {
            current_password: passForm.currentPassword,
            new_password: passForm.newPassword
        });
        toast.success("Đổi mật khẩu thành công!");
        setPassForm({ currentPassword: '', newPassword: '', confirmPassword: '' });
    } catch (error) {
        toast.error(error.response?.data?.detail || "Đổi mật khẩu thất bại.");
    } finally {
        setIsPassLoading(false);
    }
  };

  const handleLogout = () => {
    if (confirm("Bạn có chắc chắn muốn đăng xuất?")) {
        localStorage.removeItem('uba_token');
        window.location.href = '/'; 
    }
  };

  if (isLoading || !localConfig) return <div className="p-10 text-center text-zinc-500">Loading configuration...</div>;

  // Helper lấy giá trị an toàn từ nested object
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
            <Settings className="w-6 h-6 text-primary-500"/> System Settings
          </h2>
          <p className="text-zinc-400 text-sm mt-1">
            Global configuration for UEBA Engine v2.4
          </p>
        </div>
        <div className="flex gap-2">
            <Button 
                onClick={handleSave} 
                disabled={!isDirty || updateConfigMutation.isPending}
                className={`min-w-[120px] transition-all ${isDirty ? 'bg-primary-600 hover:bg-primary-700' : 'bg-zinc-800 text-zinc-500'}`}
            >
                <Save className="w-4 h-4 mr-2"/>
                {updateConfigMutation.isPending ? "Saving..." : (isDirty ? "Save Changes" : "Saved")}
            </Button>
        </div>
      </header>

      {/* TABS CONTENT */}
      <div className="flex-1 min-h-0 overflow-hidden">
        <Tabs defaultValue="analysis" className="h-full flex flex-col">
            <TabsList className="bg-zinc-900 border border-zinc-800 w-full justify-start rounded-lg p-1 h-auto mb-4 shrink-0">
                <TabsTrigger value="analysis" className="data-[state=active]:bg-zinc-800"><Clock className="w-4 h-4 mr-2"/> Time & Behavior</TabsTrigger>
                <TabsTrigger value="thresholds" className="data-[state=active]:bg-zinc-800"><Shield className="w-4 h-4 mr-2"/> Security Thresholds</TabsTrigger>
                <TabsTrigger value="signatures" className="data-[state=active]:bg-zinc-800"><List className="w-4 h-4 mr-2"/> Signatures & Lists</TabsTrigger>
                <TabsTrigger value="llm" className="data-[state=active]:bg-zinc-800"><BrainCircuit className="w-4 h-4 mr-2"/> AI & LLM</TabsTrigger>
                <TabsTrigger value="account" className="data-[state=active]:bg-zinc-800 ml-auto text-zinc-300 data-[state=active]:text-white">
                    <User className="w-4 h-4 mr-2"/> Account
                </TabsTrigger>
            </TabsList>

            <div className="flex-1 overflow-y-auto custom-scrollbar pr-2 pb-10">
                
                {/* === TAB 1: ANALYSIS PARAMS === */}
                <TabsContent value="analysis" className="space-y-4 mt-0">
                    <div className="grid grid-cols-2 gap-4">
                        <ConfigCard title="Global Behavior Settings" desc="Cấu hình chu kỳ hoạt động của Engine.">
                            <FormItem label="Engine Sleep Interval (seconds)" desc="Thời gian nghỉ giữa các lần quét log">
                                <Input type="number" value={getVal('engine_sleep_interval_seconds')} 
                                       onChange={e => updateField('engine_sleep_interval_seconds', parseInt(e.target.value))}/>
                            </FormItem>
                            <div className="grid grid-cols-2 gap-4 mt-4">
                                <FormItem label="Multi-Table Window (min)" desc="Cửa sổ thời gian gom nhóm">
                                    <Input type="number" value={getVal('security_rules.thresholds.multi_table_window_minutes')} 
                                           onChange={e => updateField('security_rules.thresholds.multi_table_window_minutes', parseInt(e.target.value))}/>
                                </FormItem>
                                <FormItem label="Min Distinct Tables" desc="Ngưỡng số bảng để báo động">
                                    <Input type="number" value={getVal('security_rules.thresholds.multi_table_min_count')} 
                                           onChange={e => updateField('security_rules.thresholds.multi_table_min_count', parseInt(e.target.value))}/>
                                </FormItem>
                            </div>
                        </ConfigCard>

                        <ConfigCard title="Working Hours Definition" desc="Định nghĩa khung giờ làm việc và giờ khuya.">
                             <div className="grid grid-cols-2 gap-4">
                                <FormItem label="Safe Start Hour (0-23)">
                                    <Input type="number" value={getVal('analysis_params.p_safe_hours_start')} 
                                           onChange={e => updateField('analysis_params.p_safe_hours_start', parseInt(e.target.value))}/>
                                </FormItem>
                                <FormItem label="Safe End Hour (0-23)">
                                    <Input type="number" value={getVal('analysis_params.p_safe_hours_end')} 
                                           onChange={e => updateField('analysis_params.p_safe_hours_end', parseInt(e.target.value))}/>
                                </FormItem>
                                <FormItem label="Late Night Start (HH:MM)">
                                    <Input value={getVal('analysis_params.p_late_night_start_time')} 
                                           onChange={e => updateField('analysis_params.p_late_night_start_time', e.target.value)}/>
                                </FormItem>
                                <FormItem label="Late Night End (HH:MM)">
                                    <Input value={getVal('analysis_params.p_late_night_end_time')} 
                                           onChange={e => updateField('analysis_params.p_late_night_end_time', e.target.value)}/>
                                </FormItem>
                             </div>
                        </ConfigCard>
                    </div>
                </TabsContent>

                {/* === TAB 2: THRESHOLDS === */}
                <TabsContent value="thresholds" className="space-y-4 mt-0">
                    <ConfigCard title="Anomaly Detection Thresholds" desc="Các ngưỡng số liệu để kích hoạt cảnh báo bảo mật.">
                        <div className="grid grid-cols-3 gap-6">
                            <FormItem label="Mass Deletion Rows" desc="Ngưỡng xóa nhiều dòng">
                                <Input type="number" value={getVal('security_rules.thresholds.mass_deletion_rows')} 
                                       onChange={e => updateField('security_rules.thresholds.mass_deletion_rows', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Execution Time Limit (ms)" desc="Ngưỡng Slow Query / DoS">
                                <Input type="number" value={getVal('security_rules.thresholds.execution_time_limit_ms')} 
                                       onChange={e => updateField('security_rules.thresholds.execution_time_limit_ms', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Brute Force Attempts" desc="Số lần thử đăng nhập sai">
                                <Input type="number" value={getVal('security_rules.thresholds.brute_force_attempts')} 
                                       onChange={e => updateField('security_rules.thresholds.brute_force_attempts', parseInt(e.target.value))}/>
                            </FormItem>
                            
                            <FormItem label="Concurrent IPs Limit" desc="Số IP đồng thời tối đa">
                                <Input type="number" value={getVal('security_rules.thresholds.concurrent_ips_limit')} 
                                       onChange={e => updateField('security_rules.thresholds.concurrent_ips_limit', parseInt(e.target.value))}/>
                            </FormItem>
                             <FormItem label="Max Query Entropy" desc="Độ hỗn loạn chuỗi (SQLi/Obfuscation)">
                                <Input type="number" step="0.1" value={getVal('security_rules.thresholds.max_query_entropy')} 
                                       onChange={e => updateField('security_rules.thresholds.max_query_entropy', parseFloat(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Impossible Travel Speed (km/h)">
                                <Input type="number" value={getVal('security_rules.thresholds.impossible_travel_speed_kmh')} 
                                       onChange={e => updateField('security_rules.thresholds.impossible_travel_speed_kmh', parseInt(e.target.value))}/>
                            </FormItem>

                            <FormItem label="Min Scan Efficiency" desc="Tỷ lệ returned/examined thấp">
                                <Input type="number" step="0.01" value={getVal('security_rules.thresholds.scan_efficiency_min')} 
                                       onChange={e => updateField('security_rules.thresholds.scan_efficiency_min', parseFloat(e.target.value))}/>
                            </FormItem>
                             <FormItem label="Min Rows for Scan Check" desc="Số dòng tối thiểu để check efficiency">
                                <Input type="number" value={getVal('security_rules.thresholds.scan_efficiency_min_rows')} 
                                       onChange={e => updateField('security_rules.thresholds.scan_efficiency_min_rows', parseInt(e.target.value))}/>
                            </FormItem>
                        </div>
                    </ConfigCard>
                </TabsContent>

                {/* === TAB 3: SIGNATURES === */}
                <TabsContent value="signatures" className="space-y-4 mt-0">
                    <div className="grid grid-cols-2 gap-4">
                        <ConfigCard title="Sensitive Data Access" desc="Quản lý bảng nhạy cảm và user được phép truy cập.">
                             <FormItem label="Sensitive Tables List" desc="Danh sách bảng quan trọng (comma separated)">
                                <Textarea className="h-24 font-mono text-xs" 
                                    value={getVal('security_rules.signatures.sensitive_tables', []).join(', ')} 
                                    onChange={e => updateArrayField('security_rules.signatures.sensitive_tables', e.target.value)}/>
                            </FormItem>
                            <FormItem label="Allowed Users (Sensitive Data)" desc="User được phép truy cập bảng nhạy cảm">
                                <Textarea className="h-24 font-mono text-xs" 
                                    value={getVal('security_rules.settings.sensitive_allowed_users', []).join(', ')} 
                                    onChange={e => updateArrayField('security_rules.settings.sensitive_allowed_users', e.target.value)}/>
                            </FormItem>
                        </ConfigCard>

                        <ConfigCard title="Threat Signatures" desc="Danh sách từ khóa đen để phát hiện tấn công.">
                             <FormItem label="SQL Injection Keywords">
                                <Textarea className="h-24 font-mono text-xs" 
                                    value={getVal('security_rules.signatures.sqli_keywords', []).join(', ')} 
                                    onChange={e => updateArrayField('security_rules.signatures.sqli_keywords', e.target.value)}/>
                            </FormItem>
                            <FormItem label="Disallowed Programs (Blacklist Tools)">
                                <Textarea className="h-24 font-mono text-xs" 
                                    value={getVal('security_rules.signatures.disallowed_programs', []).join(', ')} 
                                    onChange={e => updateArrayField('security_rules.signatures.disallowed_programs', e.target.value)}/>
                            </FormItem>
                        </ConfigCard>
                    </div>
                </TabsContent>

                {/* === TAB 4: LLM === */}
                <TabsContent value="llm" className="space-y-4 mt-0">
                    <ConfigCard title="Ollama Configuration" desc="Kết nối tới Local LLM (DeepSeek/Llama).">
                        <div className="flex items-center space-x-2 mb-4">
                            <Switch id="ollama-mode" 
                                checked={getVal('llm_config.enable_ollama')}
                                onCheckedChange={c => updateField('llm_config.enable_ollama', c)}
                            />
                            <Label htmlFor="ollama-mode" className="text-white font-bold">Enable Ollama Provider</Label>
                        </div>
                        
                        <div className="grid grid-cols-2 gap-4">
                            <FormItem label="Ollama Host URL">
                                <Input value={getVal('llm_config.ollama_host')} 
                                       onChange={e => updateField('llm_config.ollama_host', e.target.value)}/>
                            </FormItem>
                             <FormItem label="Model Name (e.g., deepseek-r1:1.5b)">
                                <Input value={getVal('llm_config.ollama_model')} 
                                       onChange={e => updateField('llm_config.ollama_model', e.target.value)}/>
                            </FormItem>
                             <FormItem label="Timeout (seconds)">
                                <Input type="number" value={getVal('llm_config.ollama_timeout')} 
                                       onChange={e => updateField('llm_config.ollama_timeout', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Max Retries">
                                <Input type="number" value={getVal('llm_config.ollama_max_retries')} 
                                       onChange={e => updateField('llm_config.ollama_max_retries', parseInt(e.target.value))}/>
                            </FormItem>
                        </div>
                    </ConfigCard>
                    
                    <ConfigCard title="Analysis Strategy" desc="Chiến lược phân tích của AI.">
                        <div className="grid grid-cols-2 gap-4">
                             <div className="flex items-center space-x-2 border border-zinc-800 p-3 rounded bg-zinc-900/50">
                                <Switch id="two-round" 
                                    checked={getVal('llm_analysis_settings.enable_two_round_analysis')}
                                    onCheckedChange={c => updateField('llm_analysis_settings.enable_two_round_analysis', c)}
                                />
                                <Label htmlFor="two-round">Enable Two-Round Analysis (Review Mode)</Label>
                            </div>
                            <FormItem label="Confidence Threshold (0.0 - 1.0)">
                                <Input type="number" step="0.1" max="1" min="0" 
                                       value={getVal('llm_analysis_settings.confidence_threshold')} 
                                       onChange={e => updateField('llm_analysis_settings.confidence_threshold', parseFloat(e.target.value))}/>
                            </FormItem>
                        </div>
                    </ConfigCard>
                </TabsContent>

                {/* === TAB 5: ACCOUNT === */}
                <TabsContent value="account" className="max-w-xl mx-auto space-y-6 mt-6">
                    <Card className="bg-zinc-950/40 border-zinc-800">
                        <CardHeader>
                            <CardTitle className="text-lg flex items-center gap-2">
                                <Key className="w-5 h-5 text-primary-500"/> Change Password
                            </CardTitle>
                            <CardDescription>Update your login credentials securely.</CardDescription>
                        </CardHeader>
                        <form onSubmit={handleChangePassword}>
                            <CardContent className="space-y-4">
                                <div className="space-y-2">
                                    <Label>Current Password</Label>
                                    <Input type="password" required value={passForm.currentPassword}
                                        onChange={e => setPassForm({...passForm, currentPassword: e.target.value})}
                                        className="bg-zinc-900 border-zinc-700 focus:border-primary-500"/>
                                </div>
                                <div className="space-y-2">
                                    <Label>New Password</Label>
                                    <Input type="password" required value={passForm.newPassword}
                                        onChange={e => setPassForm({...passForm, newPassword: e.target.value})}
                                        className="bg-zinc-900 border-zinc-700 focus:border-primary-500"/>
                                </div>
                                <div className="space-y-2">
                                    <Label>Confirm New Password</Label>
                                    <Input type="password" required value={passForm.confirmPassword}
                                        onChange={e => setPassForm({...passForm, confirmPassword: e.target.value})}
                                        className="bg-zinc-900 border-zinc-700 focus:border-primary-500"/>
                                </div>
                            </CardContent>
                            <CardFooter className="flex justify-end border-t border-zinc-800/50 pt-4">
                                <Button type="submit" disabled={isPassLoading} className="bg-primary-600 hover:bg-primary-700">
                                    {isPassLoading ? "Verifying..." : "Update Password"}
                                </Button>
                            </CardFooter>
                        </form>
                    </Card>

                    <Card className="bg-red-950/10 border-red-900/30">
                        <CardHeader>
                            <CardTitle className="text-lg text-red-500 flex items-center gap-2">
                                <LogOut className="w-5 h-5"/> Session Control
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="flex justify-between items-center">
                            <div className="text-sm text-zinc-400">End your session securely.</div>
                            <Button variant="destructive" onClick={handleLogout} className="bg-red-600 hover:bg-red-700">Sign Out</Button>
                        </CardContent>
                    </Card>
                </TabsContent>

            </div>
        </Tabs>
      </div>
    </div>
  );
}

const ConfigCard = ({ title, desc, children }) => (
  <Card className="bg-zinc-950/40 border-zinc-800 h-full">
    <CardHeader className="pb-3 border-b border-zinc-800/50 mb-3">
      <CardTitle className="text-base font-semibold text-zinc-100 flex items-center gap-2">
        <Activity className="w-4 h-4 text-zinc-500"/> {title}
      </CardTitle>
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
    {desc && <p className="text-[10px] text-zinc-600 italic">{desc}</p>}
  </div>
);