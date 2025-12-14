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
import { 
    Save, Settings, Shield, BrainCircuit, List, Clock, Key, LogOut, User, Activity, Mail, Database
} from 'lucide-react';

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

  // Helper functions
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

  const updateArrayField = (path, stringValue) => {
    const arr = stringValue.split(',').map(s => s.trim()).filter(Boolean);
    updateField(path, arr);
  };

  const getVal = (path, fallback = "") => {
    return path.split('.').reduce((acc, key) => (acc && acc[key] !== undefined) ? acc[key] : undefined, localConfig) ?? fallback;
  };

  const handleSave = () => {
    updateConfigMutation.mutate(localConfig, {
        onSuccess: () => setIsDirty(false)
    });
  };

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

  if (isLoading || !localConfig) return <div className="p-10 text-center text-zinc-500">Loading settings...</div>;

  return (
    <div className="h-full flex flex-col gap-4 overflow-hidden pr-2">
      <Toaster position="top-right" theme="dark" />
      
      {/* HEADER */}
      <header className="shrink-0 flex justify-between items-center pb-4 border-b border-zinc-800">
        <div>
          <h2 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
            <Settings className="w-6 h-6 text-primary-500"/> System Configuration
          </h2>
          <p className="text-zinc-400 text-sm mt-1">
            Cấu hình lõi cho Engine UEBA (Email, Thresholds, AI, Signatures).
          </p>
        </div>
        <div className="flex gap-2">
            <Button 
                onClick={handleSave} 
                disabled={!isDirty || updateConfigMutation.isPending}
                className={`min-w-[120px] transition-all ${isDirty ? 'bg-primary-600 hover:bg-primary-700' : 'bg-zinc-800 text-zinc-500'}`}
            >
                <Save className="w-4 h-4 mr-2"/>
                {updateConfigMutation.isPending ? "Saving..." : (isDirty ? "Lưu Thay Đổi" : "Đã Lưu")}
            </Button>
        </div>
      </header>

      {/* TABS CONTENT */}
      <div className="flex-1 min-h-0 overflow-hidden">
        <Tabs defaultValue="alerts" className="h-full flex flex-col">
            <TabsList className="bg-zinc-900 border border-zinc-800 w-full justify-start rounded-lg p-1 h-auto mb-4 shrink-0 overflow-x-auto">
                <TabsTrigger value="alerts" className="data-[state=active]:bg-zinc-800"><Mail className="w-4 h-4 mr-2"/> Alerts & Email</TabsTrigger>
                <TabsTrigger value="thresholds" className="data-[state=active]:bg-zinc-800"><Shield className="w-4 h-4 mr-2"/> Security Thresholds</TabsTrigger>
                <TabsTrigger value="signatures" className="data-[state=active]:bg-zinc-800"><List className="w-4 h-4 mr-2"/> Signatures & Lists</TabsTrigger>
                <TabsTrigger value="analysis" className="data-[state=active]:bg-zinc-800"><Clock className="w-4 h-4 mr-2"/> Time & Behavior</TabsTrigger>
                <TabsTrigger value="llm" className="data-[state=active]:bg-zinc-800"><BrainCircuit className="w-4 h-4 mr-2"/> AI & LLM</TabsTrigger>
                <TabsTrigger value="account" className="data-[state=active]:bg-zinc-800 ml-auto text-zinc-300 data-[state=active]:text-white">
                    <User className="w-4 h-4 mr-2"/> Account
                </TabsTrigger>
            </TabsList>

            <div className="flex-1 overflow-y-auto custom-scrollbar pr-2 pb-10">
                
                {/* === TAB: EMAIL ALERTS === */}
                <TabsContent value="alerts" className="space-y-4 mt-0">
                    <ConfigCard title="Email Notification Settings" desc="Cấu hình máy chủ SMTP để gửi cảnh báo khi phát hiện bất thường.">
                        <div className="flex items-center space-x-2 mb-6 border border-zinc-800 p-3 rounded bg-zinc-900/50">
                            <Switch id="email-enable" 
                                checked={getVal('email_alert_config.enable_email_alerts', true)}
                                onCheckedChange={c => updateField('email_alert_config.enable_email_alerts', c)}
                            />
                            <Label htmlFor="email-enable" className="text-white font-bold cursor-pointer">Bật gửi cảnh báo qua Email</Label>
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <div className="space-y-4">
                                <h4 className="text-sm font-semibold text-primary-400 uppercase tracking-wider">Cấu hình SMTP Server</h4>
                                <FormItem label="SMTP Server Host" desc="Địa chỉ máy chủ gửi mail (VD: smtp.gmail.com)">
                                    <Input value={getVal('email_alert_config.smtp_server')} 
                                           onChange={e => updateField('email_alert_config.smtp_server', e.target.value)}/>
                                </FormItem>
                                <FormItem label="SMTP Port" desc="Cổng gửi mail (Thường là 587 cho TLS hoặc 465 cho SSL)">
                                    <Input type="number" value={getVal('email_alert_config.smtp_port')} 
                                           onChange={e => updateField('email_alert_config.smtp_port', parseInt(e.target.value))}/>
                                </FormItem>
                                <div className="border-t border-zinc-800 my-2 pt-2"></div>
                                <FormItem label="Email Người Gửi" desc="Địa chỉ email dùng để gửi cảnh báo đi">
                                    <Input value={getVal('email_alert_config.sender_email')} 
                                           onChange={e => updateField('email_alert_config.sender_email', e.target.value)}/>
                                </FormItem>
                                <FormItem label="Mật Khẩu Ứng Dụng" desc="Nếu dùng Gmail 2FA, hãy tạo App Password.">
                                    <Input type="password" value={getVal('email_alert_config.sender_password')} 
                                           onChange={e => updateField('email_alert_config.sender_password', e.target.value)}/>
                                </FormItem>
                            </div>

                            <div className="space-y-4">
                                <h4 className="text-sm font-semibold text-primary-400 uppercase tracking-wider">Danh Sách Người Nhận</h4>
                                <FormItem label="Người nhận chính (To)" desc="Email nhận cảnh báo trực tiếp (phân cách bằng dấu phẩy)">
                                    <Textarea className="h-32 font-mono text-xs" 
                                        value={getVal('email_alert_config.to_recipients', []).join(', ')} 
                                        onChange={e => updateArrayField('email_alert_config.to_recipients', e.target.value)}/>
                                </FormItem>
                                <FormItem label="Người nhận ẩn (BCC)" desc="Email nhận bản sao ẩn để giám sát (phân cách bằng dấu phẩy)">
                                    <Textarea className="h-32 font-mono text-xs" 
                                        value={getVal('email_alert_config.bcc_recipients', []).join(', ')} 
                                        onChange={e => updateArrayField('email_alert_config.bcc_recipients', e.target.value)}/>
                                </FormItem>
                            </div>
                        </div>
                    </ConfigCard>
                </TabsContent>

                {/* === TAB: THRESHOLDS (ĐÃ BỔ SUNG CÁC TRƯỜNG THIẾU) === */}
                <TabsContent value="thresholds" className="space-y-4 mt-0">
                    <ConfigCard title="Ngưỡng Phát Hiện Bất Thường" desc="Điều chỉnh độ nhạy của các Rule.">
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                            <FormItem label="Ngưỡng Xóa (Rows)" desc="Rule 25: Mass Deletion">
                                <Input type="number" value={getVal('security_rules.thresholds.mass_deletion_rows')} 
                                       onChange={e => updateField('security_rules.thresholds.mass_deletion_rows', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Thời gian chạy tối đa (ms)" desc="Rule 12: DoS / Slow Query">
                                <Input type="number" value={getVal('security_rules.thresholds.execution_time_limit_ms')} 
                                       onChange={e => updateField('security_rules.thresholds.execution_time_limit_ms', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="CPU Time tối đa (ms)" desc="Rule 13: High CPU Usage">
                                <Input type="number" value={getVal('security_rules.thresholds.cpu_time_limit_ms')} 
                                       onChange={e => updateField('security_rules.thresholds.cpu_time_limit_ms', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Lock Time tối đa (ms)" desc="Rule 20: Excessive Locking">
                                <Input type="number" value={getVal('security_rules.thresholds.lock_time_limit_ms')} 
                                       onChange={e => updateField('security_rules.thresholds.lock_time_limit_ms', parseInt(e.target.value))}/>
                            </FormItem>

                            <FormItem label="Số lần thử sai (Brute-force)" desc="Rule 2: Login Failure Limit">
                                <Input type="number" value={getVal('security_rules.thresholds.brute_force_attempts')} 
                                       onChange={e => updateField('security_rules.thresholds.brute_force_attempts', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Số Warning tối đa" desc="Rule 22: Warning Flooding">
                                <Input type="number" value={getVal('security_rules.thresholds.warning_count_threshold')} 
                                       onChange={e => updateField('security_rules.thresholds.warning_count_threshold', parseInt(e.target.value))}/>
                            </FormItem>
                            
                            <FormItem label="Số IP đồng thời" desc="Rule 1: Concurrent IPs">
                                <Input type="number" value={getVal('security_rules.thresholds.concurrent_ips_limit')} 
                                       onChange={e => updateField('security_rules.thresholds.concurrent_ips_limit', parseInt(e.target.value))}/>
                            </FormItem>
                             <FormItem label="Entropy tối đa" desc="Rule 16: SQL Injection / Obfuscation">
                                <Input type="number" step="0.1" value={getVal('security_rules.thresholds.max_query_entropy')} 
                                       onChange={e => updateField('security_rules.thresholds.max_query_entropy', parseFloat(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Tốc độ di chuyển (km/h)" desc="Rule 3: Impossible Travel">
                                <Input type="number" value={getVal('security_rules.thresholds.impossible_travel_speed_kmh')} 
                                       onChange={e => updateField('security_rules.thresholds.impossible_travel_speed_kmh', parseInt(e.target.value))}/>
                            </FormItem>

                            <FormItem label="Hiệu suất quét (Min Ratio)" desc="Rule 14: Scan Efficiency">
                                <Input type="number" step="0.01" value={getVal('security_rules.thresholds.scan_efficiency_min')} 
                                       onChange={e => updateField('security_rules.thresholds.scan_efficiency_min', parseFloat(e.target.value))}/>
                            </FormItem>
                             <FormItem label="Số dòng tối thiểu check quét" desc="Rule 14: Min Rows">
                                <Input type="number" value={getVal('security_rules.thresholds.scan_efficiency_min_rows')} 
                                       onChange={e => updateField('security_rules.thresholds.scan_efficiency_min_rows', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Min Profile Occurrences" desc="Rule 31: Behavior Profile (Redis)">
                                <Input type="number" value={getVal('security_rules.thresholds.min_occurrences_threshold')} 
                                    onChange={e => updateField('security_rules.thresholds.min_occurrences_threshold', parseInt(e.target.value))}/>
                            </FormItem>
                        </div>
                    </ConfigCard>
                </TabsContent>

                {/* === TAB: SIGNATURES (ĐÃ BỔ SUNG CÁC TRƯỜNG THIẾU) === */}
                <TabsContent value="signatures" className="space-y-4 mt-0">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {/* COL 1: ATTACK SIGNATURES */}
                        <div className="space-y-6">
                            <ConfigCard title="Dấu Hiệu Tấn Công (Attack Patterns)" desc="Các từ khóa định danh hành vi nguy hiểm.">
                                <div className="space-y-4">
                                    <FormItem label="Từ khóa SQL Injection" desc="Rule 11: Phát hiện tấn công tiêm nhiễm SQL">
                                        <Textarea className="h-32 font-mono text-xs" 
                                            value={getVal('security_rules.signatures.sqli_keywords', []).join(', ')} 
                                            onChange={e => updateArrayField('security_rules.signatures.sqli_keywords', e.target.value)}/>
                                    </FormItem>
                                    <FormItem label="Từ khóa Admin Privilege" desc="Rule 5: Phát hiện leo thang đặc quyền">
                                        <Textarea className="h-24 font-mono text-xs" 
                                            value={getVal('security_rules.signatures.admin_keywords', []).join(', ')} 
                                            onChange={e => updateArrayField('security_rules.signatures.admin_keywords', e.target.value)}/>
                                    </FormItem>
                                    <FormItem label="Công cụ bị cấm (Blacklist)" desc="Rule 17: Các tool/client không được phép kết nối">
                                        <Textarea className="h-24 font-mono text-xs" 
                                            value={getVal('security_rules.signatures.disallowed_programs', []).join(', ')} 
                                            onChange={e => updateArrayField('security_rules.signatures.disallowed_programs', e.target.value)}/>
                                    </FormItem>
                                    <FormItem label="Công cụ cho phép (Whitelist)" desc="Rule 17: Nếu cấu hình, chỉ các tool này được phép">
                                        <Textarea className="h-24 font-mono text-xs" 
                                            value={getVal('security_rules.signatures.allowed_programs', []).join(', ')} 
                                            onChange={e => updateArrayField('security_rules.signatures.allowed_programs', e.target.value)}/>
                                    </FormItem>
                                </div>
                            </ConfigCard>
                        </div>

                        {/* COL 2: SENSITIVE DATA & USERS */}
                        <div className="space-y-6">
                            <ConfigCard title="Bảo Vệ Dữ Liệu (Data Protection)" desc="Cấu hình bảo vệ dữ liệu nhạy cảm.">
                                <div className="space-y-4">
                                    <FormItem label="Danh sách Bảng Nhạy Cảm" desc="Rule 6: Bảng chứa dữ liệu mật (lương, thẻ...)">
                                        <Textarea className="h-24 font-mono text-xs" 
                                            value={getVal('security_rules.signatures.sensitive_tables', []).join(', ')} 
                                            onChange={e => updateArrayField('security_rules.signatures.sensitive_tables', e.target.value)}/>
                                    </FormItem>
                                    <FormItem label="User Được Phép Truy Cập" desc="Rule 6: Whitelist user xem bảng nhạy cảm">
                                        <Textarea className="h-24 font-mono text-xs" 
                                            value={getVal('security_rules.settings.sensitive_allowed_users', []).join(', ')} 
                                            onChange={e => updateArrayField('security_rules.settings.sensitive_allowed_users', e.target.value)}/>
                                    </FormItem>
                                    <div className="border-t border-zinc-800 pt-2"></div>
                                    <FormItem label="Bảng Dữ Liệu Lớn (Large Dump)" desc="Rule 27: Giám sát việc dump dữ liệu trái phép">
                                        <Textarea className="h-24 font-mono text-xs" 
                                            value={getVal('security_rules.signatures.large_dump_tables', []).join(', ')} 
                                            onChange={e => updateArrayField('security_rules.signatures.large_dump_tables', e.target.value)}/>
                                    </FormItem>
                                </div>
                            </ConfigCard>

                            <ConfigCard title="Người Dùng Đặc Biệt" desc="Các user có quyền hạn cao.">
                                <div className="space-y-4">
                                    <FormItem label="HR Authorized Users" desc="Rule 8: User được phép tạo tài khoản mới">
                                        <Textarea className="h-24 font-mono text-xs" 
                                            value={getVal('security_rules.signatures.hr_authorized_users', []).join(', ')} 
                                            onChange={e => updateArrayField('security_rules.signatures.hr_authorized_users', e.target.value)}/>
                                    </FormItem>
                                    <FormItem label="Restricted Connection Users" desc="Rule 10: User bị cấm kết nối Insecure (TCP/IP)">
                                        <Textarea className="h-24 font-mono text-xs" 
                                            value={getVal('security_rules.signatures.restricted_connection_users', []).join(', ')} 
                                            onChange={e => updateArrayField('security_rules.signatures.restricted_connection_users', e.target.value)}/>
                                    </FormItem>
                                </div>
                            </ConfigCard>
                        </div>
                    </div>
                </TabsContent>
                
                {/* === TAB: ANALYSIS PARAMS === */}
                <TabsContent value="analysis" className="space-y-4 mt-0">
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                        <ConfigCard title="Cấu Hình Chu Kỳ Engine" desc="Tham số điều khiển tần suất quét và phân tích.">
                            <FormItem label="Chu kỳ nghỉ (Sleep Interval)" desc="Thời gian nghỉ giữa các lần quét log (giây)">
                                <Input type="number" value={getVal('engine_sleep_interval_seconds')} 
                                       onChange={e => updateField('engine_sleep_interval_seconds', parseInt(e.target.value))}/>
                            </FormItem>
                            <div className="grid grid-cols-2 gap-4 mt-4">
                                <FormItem label="Cửa sổ gom nhóm (phút)" desc="Khoảng thời gian để đếm số bảng bị truy cập (Rule 30)">
                                    <Input type="number" value={getVal('security_rules.thresholds.multi_table_window_minutes')} 
                                           onChange={e => updateField('security_rules.thresholds.multi_table_window_minutes', parseInt(e.target.value))}/>
                                </FormItem>
                                <FormItem label="Số bảng tối thiểu" desc="Số lượng bảng khác nhau bị truy cập để kích hoạt cảnh báo Multi-table">
                                    <Input type="number" value={getVal('security_rules.thresholds.multi_table_min_count')} 
                                           onChange={e => updateField('security_rules.thresholds.multi_table_min_count', parseInt(e.target.value))}/>
                                </FormItem>
                            </div>
                        </ConfigCard>

                        <ConfigCard title="Định Nghĩa Giờ Làm Việc" desc="Thiết lập khung giờ chuẩn và giờ khuya.">
                             <div className="grid grid-cols-2 gap-4">
                                <FormItem label="Giờ bắt đầu làm việc" desc="Giờ an toàn bắt đầu (VD: 8)">
                                    <Input type="number" value={getVal('security_rules.settings.sensitive_safe_hours_start')} 
                                           onChange={e => updateField('security_rules.settings.sensitive_safe_hours_start', parseInt(e.target.value))}/>
                                </FormItem>
                                <FormItem label="Giờ kết thúc làm việc" desc="Giờ an toàn kết thúc (VD: 17)">
                                    <Input type="number" value={getVal('security_rules.settings.sensitive_safe_hours_end')} 
                                           onChange={e => updateField('security_rules.settings.sensitive_safe_hours_end', parseInt(e.target.value))}/>
                                </FormItem>
                                <FormItem label="Bắt đầu Giờ Khuya" desc="Mốc thời gian bắt đầu tính là đêm khuya (Rule 7)">
                                    <Input value={getVal('security_rules.settings.late_night_start')} 
                                           onChange={e => updateField('security_rules.settings.late_night_start', e.target.value)}/>
                                </FormItem>
                                <FormItem label="Kết thúc Giờ Khuya" desc="Mốc thời gian kết thúc đêm khuya">
                                    <Input value={getVal('security_rules.settings.late_night_end')} 
                                           onChange={e => updateField('security_rules.settings.late_night_end', e.target.value)}/>
                                </FormItem>
                             </div>
                        </ConfigCard>
                    </div>
                </TabsContent>

                {/* === TAB: AI & LLM === */}
                <TabsContent value="llm" className="space-y-4 mt-0">
                    <ConfigCard title="Cấu Hình AI (Ollama/LLM)" desc="Kết nối tới mô hình ngôn ngữ lớn.">
                        <div className="flex items-center space-x-2 mb-4 border border-zinc-800 p-2 rounded">
                            <Switch id="ollama-mode" 
                                checked={getVal('llm_config.enable_ollama')}
                                onCheckedChange={c => updateField('llm_config.enable_ollama', c)}
                            />
                            <Label htmlFor="ollama-mode" className="text-white font-bold cursor-pointer">Bật sử dụng Local LLM (Ollama)</Label>
                        </div>
                        
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            <FormItem label="Ollama Host URL" desc="Địa chỉ server chạy Ollama">
                                <Input value={getVal('llm_config.ollama_host')} 
                                       onChange={e => updateField('llm_config.ollama_host', e.target.value)}/>
                            </FormItem>
                             <FormItem label="Tên Model" desc="Tên model đã pull về (VD: deepseek-r1:1.5b)">
                                <Input value={getVal('llm_config.ollama_model')} 
                                       onChange={e => updateField('llm_config.ollama_model', e.target.value)}/>
                            </FormItem>
                             <FormItem label="Timeout (giây)" desc="Thời gian chờ tối đa cho 1 request">
                                <Input type="number" value={getVal('llm_config.ollama_timeout')} 
                                       onChange={e => updateField('llm_config.ollama_timeout', parseInt(e.target.value))}/>
                            </FormItem>
                            <FormItem label="Số lần thử lại (Max Retries)" desc="Thử lại nếu kết nối thất bại">
                                <Input type="number" value={getVal('llm_config.ollama_max_retries')} 
                                       onChange={e => updateField('llm_config.ollama_max_retries', parseInt(e.target.value))}/>
                            </FormItem>
                        </div>
                    </ConfigCard>
                    
                    <ConfigCard title="Chiến Lược Phân Tích" desc="Cách thức AI xử lý dữ liệu.">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                             <div className="flex items-center space-x-2 border border-zinc-800 p-3 rounded bg-zinc-900/50">
                                <Switch id="two-round" 
                                    checked={getVal('llm_analysis_settings.enable_two_round_analysis')}
                                    onCheckedChange={c => updateField('llm_analysis_settings.enable_two_round_analysis', c)}
                                />
                                <Label htmlFor="two-round" className="cursor-pointer">Chế độ phân tích 2 vòng (Review Mode)</Label>
                            </div>
                            <FormItem label="Ngưỡng tin cậy (Confidence Threshold)" desc="Chỉ báo động nếu AI chắc chắn trên mức này (0.0 - 1.0)">
                                <Input type="number" step="0.1" max="1" min="0" 
                                       value={getVal('llm_analysis_settings.confidence_threshold')} 
                                       onChange={e => updateField('llm_analysis_settings.confidence_threshold', parseFloat(e.target.value))}/>
                            </FormItem>
                        </div>
                    </ConfigCard>
                </TabsContent>

                {/* === TAB: ACCOUNT === */}
                <TabsContent value="account" className="max-w-xl mx-auto space-y-6 mt-6">
                    <Card className="bg-zinc-950/40 border-zinc-800">
                        <CardHeader>
                            <CardTitle className="text-lg flex items-center gap-2">
                                <Key className="w-5 h-5 text-primary-500"/> Đổi Mật Khẩu
                            </CardTitle>
                            <CardDescription>Cập nhật mật khẩu đăng nhập hệ thống.</CardDescription>
                        </CardHeader>
                        <form onSubmit={handleChangePassword}>
                            <CardContent className="space-y-4">
                                <div className="space-y-2">
                                    <Label>Mật khẩu hiện tại</Label>
                                    <Input type="password" required value={passForm.currentPassword}
                                        onChange={e => setPassForm({...passForm, currentPassword: e.target.value})}
                                        className="bg-zinc-900 border-zinc-700 focus:border-primary-500"/>
                                </div>
                                <div className="space-y-2">
                                    <Label>Mật khẩu mới</Label>
                                    <Input type="password" required value={passForm.newPassword}
                                        onChange={e => setPassForm({...passForm, newPassword: e.target.value})}
                                        className="bg-zinc-900 border-zinc-700 focus:border-primary-500"/>
                                </div>
                                <div className="space-y-2">
                                    <Label>Xác nhận mật khẩu mới</Label>
                                    <Input type="password" required value={passForm.confirmPassword}
                                        onChange={e => setPassForm({...passForm, confirmPassword: e.target.value})}
                                        className="bg-zinc-900 border-zinc-700 focus:border-primary-500"/>
                                </div>
                            </CardContent>
                            <CardFooter className="flex justify-end border-t border-zinc-800/50 pt-4">
                                <Button type="submit" disabled={isPassLoading} className="bg-primary-600 hover:bg-primary-700">
                                    {isPassLoading ? "Đang xử lý..." : "Cập nhật mật khẩu"}
                                </Button>
                            </CardFooter>
                        </form>
                    </Card>

                    <Card className="bg-red-950/10 border-red-900/30">
                        <CardHeader>
                            <CardTitle className="text-lg text-red-500 flex items-center gap-2">
                                <LogOut className="w-5 h-5"/> Điều Khiển Phiên
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="flex justify-between items-center">
                            <div className="text-sm text-zinc-400">Đăng xuất khỏi phiên làm việc hiện tại.</div>
                            <Button variant="destructive" onClick={handleLogout} className="bg-red-600 hover:bg-red-700">Đăng Xuất</Button>
                        </CardContent>
                    </Card>
                </TabsContent>

            </div>
        </Tabs>
      </div>
    </div>
  );
}

// COMPONENTS CON
const ConfigCard = ({ title, desc, children }) => (
  <Card className="bg-zinc-950/40 border-zinc-800 h-full shadow-lg">
    <CardHeader className="pb-3 border-b border-zinc-800/50 mb-3 bg-zinc-900/20">
      <CardTitle className="text-base font-semibold text-zinc-100 flex items-center gap-2">
        {title}
      </CardTitle>
      {desc && <CardDescription className="text-xs text-zinc-400 leading-relaxed">{desc}</CardDescription>}
    </CardHeader>
    <CardContent className="pt-2">
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