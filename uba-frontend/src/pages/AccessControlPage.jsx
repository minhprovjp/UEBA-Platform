// uba_frontend/src/pages/AccessControlPage.jsx
import React, { useState, useEffect } from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { Toaster, toast } from "sonner";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { useConfig, useUpdateConfigMutation } from '@/api/queries';
import { Save, ShieldCheck, Clock, Server, PlusCircle, Trash2, Calendar, Zap, AlertTriangle } from 'lucide-react';
import { useTranslation } from 'react-i18next';

export default function AccessControlPage() {
  const [localConfig, setLocalConfig] = useState(null);
  const [isDirty, setIsDirty] = useState(false);
  const { t } = useTranslation();

  // State cho Form thêm mới Overtime
  const [newOvertime, setNewOvertime] = useState({ user: '', date: '', start: '', end: '', ip: '', reason: '' });
  
  // State cho Form thêm mới Service Account
  const [newServiceAccount, setNewServiceAccount] = useState({ user: '', hours: '', ips: '' });

  const { data: config, isLoading } = useConfig();
  const updateConfigMutation = useUpdateConfigMutation();

  useEffect(() => {
    if (config) {
      setLocalConfig(JSON.parse(JSON.stringify(config)));
      setIsDirty(false);
    }
  }, [config]);

  // --- HELPER FUNCTIONS ---
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

  const getVal = (path, fallback = "") => {
    return path.split('.').reduce((acc, key) => (acc && acc[key] !== undefined) ? acc[key] : undefined, localConfig) ?? fallback;
  };

  const handleSave = () => {
    updateConfigMutation.mutate(localConfig, { onSuccess: () => setIsDirty(false) });
  };

  // --- LOGIC 1: OVERTIME ---
  const handleAddOvertime = () => {
    if (!newOvertime.user || !newOvertime.date || !newOvertime.start || !newOvertime.end) {
        toast.error("Vui lòng nhập User, Ngày và Giờ."); return;
    }
    const currentList = getVal('security_rules.signatures.overtime_schedule', []);
    const updatedList = [...currentList, newOvertime];
    updateField('security_rules.signatures.overtime_schedule', updatedList);
    setNewOvertime({ user: '', date: '', start: '', end: '', ip: '', reason: '' });
    toast.success("Đã thêm lịch trình ngoại lệ.");
  };

  const handleRemoveOvertime = (index) => {
    const currentList = getVal('security_rules.signatures.overtime_schedule', []);
    const updatedList = currentList.filter((_, i) => i !== index);
    updateField('security_rules.signatures.overtime_schedule', updatedList);
  };

  // --- LOGIC 2: SERVICE ACCOUNTS ---
  const handleAddServiceAccount = () => {
    if (!newServiceAccount.user || !newServiceAccount.hours || !newServiceAccount.ips) {
        toast.error("Vui lòng nhập User, Hours và IPs."); return;
    }
    const hoursArr = newServiceAccount.hours.split(',').map(h => parseInt(h.trim())).filter(h => !isNaN(h) && h >= 0 && h <= 23);
    const ipsArr = newServiceAccount.ips.split(',').map(ip => ip.trim()).filter(Boolean);

    if (hoursArr.length === 0) { toast.error("Giờ hoạt động không hợp lệ."); return; }

    const currentMap = getVal('security_rules.service_accounts', {});
    const updatedMap = { ...currentMap };
    updatedMap[newServiceAccount.user] = { "allowed_hours": hoursArr, "allowed_ips": ipsArr };

    updateField('security_rules.service_accounts', updatedMap);
    setNewServiceAccount({ user: '', hours: '', ips: '' });
    toast.success(`Đã cấu hình Service Account: ${newServiceAccount.user}`);
  };

  const handleRemoveServiceAccount = (userKey) => {
    const currentMap = getVal('security_rules.service_accounts', {});
    const updatedMap = { ...currentMap };
    delete updatedMap[userKey];
    updateField('security_rules.service_accounts', updatedMap);
  };

  if (isLoading || !localConfig) return <div className="p-10 text-center text-zinc-500">Loading Access Control...</div>;
  const serviceAccountsList = Object.entries(getVal('security_rules.service_accounts', {}));

  return (
    <div className="h-full flex flex-col gap-4 overflow-hidden pr-2">
      <Toaster position="top-right" theme="dark" />
      
      {/* HEADER */}
      <header className="shrink-0 flex justify-between items-center pb-4 border-b border-zinc-800">
        <div>
          <h2 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
            <ShieldCheck className="w-6 h-6 text-green-500"/> Access Control Policies
          </h2>
          <p className="text-zinc-400 text-sm mt-1">
            Quản lý quyền truy cập đặc biệt (Overtime) và tài khoản hệ thống (Service Accounts).
          </p>
        </div>
        <div className="flex gap-2">
            <Button onClick={handleSave} disabled={!isDirty || updateConfigMutation.isPending}
                className={`min-w-[120px] transition-all ${isDirty ? 'bg-primary-600 hover:bg-primary-700' : 'bg-zinc-800 text-zinc-500'}`}>
                <Save className="w-4 h-4 mr-2"/> {updateConfigMutation.isPending ? "Saving..." : (isDirty ? t('common.save') : t('common.saved'))}
            </Button>
        </div>
      </header>

      {/* CONTENT */}
      <div className="flex-1 overflow-y-auto custom-scrollbar pb-10">
        
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
            
            {/* OVERTIME SCHEDULE */}
            <div className="space-y-4">
                <div className="flex items-center gap-2 mb-2">
                    <Clock className="w-5 h-5 text-blue-400"/>
                    <h3 className="text-lg font-semibold text-white">{t('access_control.overtime_title')}</h3>
                </div>
                
                <ConfigCard 
                    title="Đăng Ký Ngoại Lệ (Exception Schedule)" 
                    desc="Cho phép user truy cập hệ thống ngoài khung giờ hành chính hoặc truy cập từ IP lạ trong khoảng thời gian nhất định (Rule 7 & 4)."
                >
                    <div className="space-y-4">
                        {/* Form Thêm */}
                        <div className="bg-zinc-900/50 p-4 rounded-md border border-zinc-800 space-y-3">
                            <Label className="text-xs text-primary-400 font-semibold flex items-center gap-1 uppercase">
                                <PlusCircle className="w-3 h-3"/> Thêm Lịch Mới
                            </Label>
                            <div className="grid grid-cols-2 gap-3">
                                <FormItem label="Tên đăng nhập (User)">
                                    <Input placeholder="ví dụ: thanh.nguyen" className="h-8 text-xs bg-zinc-950" 
                                    value={newOvertime.user} onChange={e => setNewOvertime({...newOvertime, user: e.target.value})}/>
                                </FormItem>
                                <FormItem label="Ngày áp dụng">
                                    <Input type="date" className="h-8 text-xs bg-zinc-950"
                                    value={newOvertime.date} onChange={e => setNewOvertime({...newOvertime, date: e.target.value})}/>
                                </FormItem>
                            </div>
                            <div className="grid grid-cols-2 gap-3">
                                <FormItem label="Bắt đầu">
                                    <Input type="time" className="h-8 text-xs bg-zinc-950"
                                    value={newOvertime.start} onChange={e => setNewOvertime({...newOvertime, start: e.target.value})}/>
                                </FormItem>
                                <FormItem label="Kết thúc">
                                    <Input type="time" className="h-8 text-xs bg-zinc-950"
                                    value={newOvertime.end} onChange={e => setNewOvertime({...newOvertime, end: e.target.value})}/>
                                </FormItem>
                            </div>
                            <FormItem label="IP Cho phép (Tùy chọn)" desc="Nếu để trống, chấp nhận mọi IP. Nếu nhập, user bắt buộc phải dùng IP này.">
                                <Input placeholder="ví dụ: 14.162.x.x" className="h-8 text-xs bg-zinc-950" 
                                value={newOvertime.ip} onChange={e => setNewOvertime({...newOvertime, ip: e.target.value})}/>
                            </FormItem>
                            <div className="flex gap-2 items-end">
                                <div className="flex-1">
                                    <FormItem label="Lý do">
                                        <Input placeholder="ví dụ: Bảo trì server đêm Noel" className="h-8 text-xs bg-zinc-950"
                                        value={newOvertime.reason} onChange={e => setNewOvertime({...newOvertime, reason: e.target.value})}/>
                                    </FormItem>
                                </div>
                                <Button size="sm" onClick={handleAddOvertime} className="h-8 mb-0.5 bg-blue-600 hover:bg-blue-700">{t('common.add')}</Button>
                            </div>
                        </div>

                        {/* Danh sách */}
                        <div className="rounded-md border border-zinc-800 bg-zinc-950 overflow-hidden">
                            <table className="w-full text-xs text-left">
                                <thead className="bg-zinc-900 text-zinc-400 font-medium">
                                    <tr>
                                        <th className="p-3">User / Ngày</th>
                                        <th className="p-3">Chi tiết (Giờ & IP)</th>
                                        <th className="p-3 w-10"></th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-zinc-800">
                                    {getVal('security_rules.signatures.overtime_schedule', []).map((item, idx) => (
                                        <tr key={idx} className="hover:bg-zinc-900/50 transition-colors">
                                            <td className="p-3 align-top">
                                                <div className="font-mono text-zinc-200 font-bold">{item.user}</div>
                                                <div className="text-[11px] text-zinc-500 flex items-center gap-1">
                                                    <Calendar className="w-3 h-3"/> {item.date}
                                                </div>
                                            </td>
                                            <td className="p-3 align-top space-y-1">
                                                <div className="text-zinc-300 font-medium">{item.start} ➝ {item.end}</div>
                                                {item.ip ? (
                                                    <div className="text-[10px] text-green-400 bg-green-950/30 px-1.5 py-0.5 rounded inline-block border border-green-900">
                                                        IP: {item.ip}
                                                    </div>
                                                ) : (
                                                    <div className="text-[10px] text-zinc-600 italic">IP: Any (Không giới hạn)</div>
                                                )}
                                                <div className="text-[11px] text-zinc-500 italic">"{item.reason}"</div>
                                            </td>
                                            <td className="p-3 text-right align-middle">
                                                <button onClick={() => handleRemoveOvertime(idx)} className="p-2 bg-zinc-900 hover:bg-red-900/50 text-zinc-500 hover:text-red-500 rounded transition-all">
                                                    <Trash2 className="w-4 h-4"/>
                                                </button>
                                            </td>
                                        </tr>
                                    ))}
                                    {getVal('security_rules.signatures.overtime_schedule', []).length === 0 && (
                                        <tr><td colSpan={3} className="p-6 text-center text-zinc-600 italic">Chưa có lịch trình ngoại lệ nào.</td></tr>
                                    )}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </ConfigCard>
            </div>

            {/* SERVICE ACCOUNTS */}
            <div className="space-y-4">
                <div className="flex items-center gap-2 mb-2">
                    <Server className="w-5 h-5 text-orange-400"/>
                    <h3 className="text-lg font-semibold text-white">Tài Khoản Dịch Vụ (Service Accounts)</h3>
                </div>

                <ConfigCard 
                    title="Giới Hạn Bot & Tool Tự Động" 
                    desc="Cấu hình danh sách trắng (Whitelist) cho các tài khoản chạy tự động (Backup, ETL...). Bất kỳ truy cập nào sai IP hoặc sai giờ quy định sẽ bị chặn (Rule 4)."
                >
                    <div className="space-y-4">
                        {/* Form Thêm */}
                        <div className="bg-zinc-900/50 p-4 rounded-md border border-zinc-800 space-y-3">
                            <Label className="text-xs text-primary-400 font-semibold flex items-center gap-1 uppercase">
                                <PlusCircle className="w-3 h-3"/> Thêm Service User
                            </Label>
                            <div className="grid grid-cols-2 gap-3">
                                <FormItem label="Tên tài khoản (User)" desc="Tên user trong MySQL">
                                    <Input placeholder="ví dụ: backup_bot" className="h-8 text-xs bg-zinc-950"
                                        value={newServiceAccount.user} onChange={e => setNewServiceAccount({...newServiceAccount, user: e.target.value})} />
                                </FormItem>
                                <FormItem label="Giờ được phép chạy" desc="Nhập số giờ (0-23), cách nhau dấu phẩy">
                                    <Input placeholder="ví dụ: 1, 2, 3 (nghĩa là 1h-3h sáng)" className="h-8 text-xs bg-zinc-950"
                                        value={newServiceAccount.hours} onChange={e => setNewServiceAccount({...newServiceAccount, hours: e.target.value})} />
                                </FormItem>
                            </div>
                            <div className="flex gap-2 items-end">
                                <div className="flex-1">
                                    <FormItem label="IP Nguồn được phép" desc="Danh sách IP được phép chạy, cách nhau dấu phẩy">
                                        <Input placeholder="ví dụ: 192.168.1.10, 10.0.0.5" className="h-8 text-xs bg-zinc-950 flex-1"
                                            value={newServiceAccount.ips} onChange={e => setNewServiceAccount({...newServiceAccount, ips: e.target.value})} />
                                    </FormItem>
                                </div>
                                <Button size="sm" onClick={handleAddServiceAccount} className="h-8 mb-6 bg-orange-600 hover:bg-orange-700">{t('common.add')}</Button>
                            </div>
                        </div>

                        {/* Danh sách */}
                        <div className="rounded-md border border-zinc-800 bg-zinc-950 overflow-hidden">
                            <table className="w-full text-xs text-left">
                                <thead className="bg-zinc-900 text-zinc-400 font-medium">
                                    <tr>
                                        <th className="p-3">User</th>
                                        <th className="p-3">Quy tắc (Hours & IPs)</th>
                                        <th className="p-3 w-10"></th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-zinc-800">
                                    {serviceAccountsList.map(([userKey, conf]) => (
                                        <tr key={userKey} className="hover:bg-zinc-900/50 transition-colors">
                                            <td className="p-3 align-top">
                                                <div className="font-bold text-orange-400 font-mono text-sm">{userKey}</div>
                                                <div className="text-[10px] text-zinc-600 mt-1">Service Account</div>
                                            </td>
                                            <td className="p-3 align-top space-y-1.5">
                                                <div>
                                                    <span className="text-[10px] uppercase text-zinc-500 font-bold mr-2">Giờ chạy:</span>
                                                    <span className="text-zinc-300 font-mono bg-zinc-800 px-1.5 py-0.5 rounded text-[11px]">
                                                        {conf.allowed_hours?.join(', ') || 'Chưa cấu hình'}h
                                                    </span>
                                                </div>
                                                <div>
                                                    <span className="text-[10px] uppercase text-zinc-500 font-bold mr-2">IP Nguồn:</span>
                                                    <span className="text-zinc-300 font-mono text-[11px]">
                                                        {conf.allowed_ips?.join(', ') || 'Chưa cấu hình'}
                                                    </span>
                                                </div>
                                            </td>
                                            <td className="p-3 w-8 text-right align-middle">
                                                <button onClick={() => handleRemoveServiceAccount(userKey)} className="p-2 bg-zinc-900 hover:bg-red-900/50 text-zinc-500 hover:text-red-500 rounded transition-all">
                                                    <Trash2 className="w-4 h-4"/>
                                                </button>
                                            </td>
                                        </tr>
                                    ))}
                                    {serviceAccountsList.length === 0 && (
                                        <tr><td colSpan={3} className="p-6 text-center text-zinc-600 italic">Chưa có tài khoản dịch vụ nào được định nghĩa.</td></tr>
                                    )}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </ConfigCard>
            </div>
        </div>
      </div>
    </div>
  );
}

// Sub-components dùng chung
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