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
import { useConfig, useUpdateConfigMutation, useAuditLogs } from '@/api/queries';
import { apiClient } from '@/api/client';
import {
    Save, Settings, Shield, BrainCircuit, List, Clock, Key, LogOut, User, Mail, Zap, AlertTriangle, FileText
} from 'lucide-react';
import { useTranslation } from 'react-i18next';

export default function SettingsPage() {
    const [localConfig, setLocalConfig] = useState(null);
    const [isDirty, setIsDirty] = useState(false);
    const { t } = useTranslation();
    const { data: auditLogs, isLoading: isLogLoading } = useAuditLogs();
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

    const getListDisplayValue = (path) => {
        const val = getVal(path, []);
        if (Array.isArray(val)) return val.join(', ');
        return val;
    };

    const handleListChange = (path, stringValue) => {
        updateField(path, stringValue);
    };

    const getVal = (path, fallback = "") => {
        return path.split('.').reduce((acc, key) => (acc && acc[key] !== undefined) ? acc[key] : undefined, localConfig) ?? fallback;
    };

    const handleSave = () => {
        const payload = JSON.parse(JSON.stringify(localConfig));
        const fieldsToConvert = [
            'email_alert_config.to_recipients',
            'email_alert_config.bcc_recipients',
            'security_rules.signatures.sqli_keywords',
            'security_rules.signatures.admin_keywords',
            'security_rules.signatures.disallowed_programs',
            'security_rules.signatures.sensitive_tables',
            'security_rules.settings.sensitive_allowed_users',
            'security_rules.signatures.large_dump_tables',
            'security_rules.signatures.hr_authorized_users',
            'security_rules.signatures.restricted_connection_users'
        ];

        const setDeepValue = (obj, path, value) => {
            const keys = path.split('.');
            let current = obj;
            for (let i = 0; i < keys.length - 1; i++) {
                if (!current[keys[i]]) current[keys[i]] = {};
                current = current[keys[i]];
            }
            current[keys[keys.length - 1]] = value;
        };

        const getDeepValue = (obj, path) => {
            return path.split('.').reduce((acc, k) => (acc && acc[k] !== undefined) ? acc[k] : undefined, obj);
        };

        fieldsToConvert.forEach(path => {
            const rawVal = getDeepValue(payload, path);
            if (typeof rawVal === 'string') {
                const arr = rawVal.split(',').map(s => s.trim()).filter(Boolean);
                setDeepValue(payload, path, arr);
            }
        });

        updateConfigMutation.mutate(payload, {
            onSuccess: () => setIsDirty(false)
        });
    };

    const handleChangePassword = async (e) => {
        e.preventDefault();
        if (!passForm.currentPassword) { toast.error(t('settings.account.error_1')); return; }
        if (passForm.newPassword !== passForm.confirmPassword) { toast.error(t('settings.account.error_2')); return; }
        if (passForm.newPassword.length < 6) { toast.error(t('settings.account.error_3')); return; }

        setIsPassLoading(true);
        try {
            await apiClient.post('/api/change-password', {
                current_password: passForm.currentPassword,
                new_password: passForm.newPassword
            });
            toast.success(t('settings.account.success'));
            setPassForm({ currentPassword: '', newPassword: '', confirmPassword: '' });
        } catch (error) {
            toast.error(error.response?.data?.detail || t('settings.account.error_4'));
        } finally {
            setIsPassLoading(false);
        }
    };

    const handleLogout = () => {
        if (confirm(t('settings.account.confirm_1'))) {
            localStorage.removeItem('uba_token');
            window.location.href = '/';
        }
    };

    if (isLoading || !localConfig) return <div className="p-10 text-center text-zinc-500">{t('common.loading')}</div>;

    return (
        <div className="h-full flex flex-col gap-4 overflow-hidden pr-2">
            <Toaster position="top-right" theme="dark" />

            {/* HEADER */}
            <header className="shrink-0 flex justify-between items-center pb-4 border-b border-zinc-800">
                <div>
                    <h2 className="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                        <Settings className="w-6 h-6 text-primary-500" /> {t('settings.title')}
                    </h2>
                    <p className="text-zinc-400 text-sm mt-1">
                        {t('settings.subtitle')}
                    </p>
                </div>
                <div className="flex gap-2">
                    <Button
                        onClick={handleSave}
                        disabled={!isDirty || updateConfigMutation.isPending}
                        className={`min-w-[120px] transition-all ${isDirty ? 'bg-primary-600 hover:bg-primary-700' : 'bg-zinc-800 text-zinc-500'}`}
                    >
                        <Save className="w-4 h-4 mr-2" />
                        {updateConfigMutation.isPending ? t('common.saving') : (isDirty ? t('common.save') : t('common.saved'))}
                    </Button>
                </div>
            </header>

            {/* TABS CONTENT */}
            <div className="flex-1 min-h-0 overflow-hidden">
                <Tabs defaultValue="alerts" className="h-full flex flex-col">
                    <TabsList className="bg-zinc-900 border border-zinc-800 w-full justify-start rounded-lg p-1 h-auto mb-4 shrink-0 overflow-x-auto">
                        <TabsTrigger value="alerts" className="data-[state=active]:bg-zinc-800"><Mail className="w-4 h-4 mr-2" /> {t('settings.tabs.alerts')}</TabsTrigger>
                        <TabsTrigger value="thresholds" className="data-[state=active]:bg-zinc-800"><Shield className="w-4 h-4 mr-2" /> {t('settings.tabs.thresholds')}</TabsTrigger>
                        <TabsTrigger value="signatures" className="data-[state=active]:bg-zinc-800"><List className="w-4 h-4 mr-2" /> {t('settings.tabs.signatures')}</TabsTrigger>
                        <TabsTrigger value="analysis" className="data-[state=active]:bg-zinc-800"><Clock className="w-4 h-4 mr-2" /> {t('settings.tabs.analysis')}</TabsTrigger>
                        <TabsTrigger value="llm" className="data-[state=active]:bg-zinc-800"><BrainCircuit className="w-4 h-4 mr-2" /> {t('settings.tabs.llm')}</TabsTrigger>
                        <TabsTrigger value="account" className="data-[state=active]:bg-zinc-800 ml-auto text-zinc-300 data-[state=active]:text-white">
                            <User className="w-4 h-4 mr-2" /> {t('settings.tabs.account')}
                        </TabsTrigger>
                    </TabsList>

                    <div className="flex-1 overflow-y-auto custom-scrollbar pr-2 pb-10">

                        {/* === TAB: RESPONSE & HISTORY === */}
                        <TabsContent value="alerts" className="space-y-4 mt-0 h-full overflow-hidden">
                            <Card className="bg-red-950/20 border-red-900/30">
                                <CardHeader className="pb-4">
                                    <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-4">
                                        {/* PHẦN BÊN TRÁI: TIÊU ĐỀ & MÔ TẢ */}
                                        <div className="space-y-1.5">
                                            <CardTitle className="text-base font-semibold text-red-400 flex items-center gap-2">
                                                <Zap className="w-5 h-5" /> {t('settings.response.title')}
                                            </CardTitle>
                                            <CardDescription className="text-zinc-400">
                                                {t('settings.response.desc')}
                                            </CardDescription>
                                            <div className="flex items-center gap-2 text-xs text-orange-400/90 pt-1">
                                                <AlertTriangle className="w-3.5 h-3.5" />
                                                <span>{t('settings.response.warning')}</span>
                                            </div>
                                        </div>

                                        {/* PHẦN BÊN PHẢI: CÁC NÚT ĐIỀU KHIỂN */}
                                        <div className="flex flex-col items-end gap-3 shrink-0">
                                            {/* 1. Nút Enable Switch */}
                                            <div className="flex items-center space-x-2 bg-red-950/40 border border-red-900/50 px-3 py-1.5 rounded-md shadow-sm">
                                                <Switch
                                                    id="ar-enable"
                                                    checked={getVal('active_response_config.enable_active_response', true)}
                                                    onCheckedChange={c => updateField('active_response_config.enable_active_response', c)}
                                                    className="data-[state=checked]:bg-red-600"
                                                />
                                                <Label htmlFor="ar-enable" className="text-sm font-bold text-red-100 cursor-pointer select-none">
                                                    {t('settings.response.enable')}
                                                </Label>
                                            </div>

                                            {/* 2. Input Threshold */}
                                            <div className="flex items-center gap-2 bg-zinc-900/30 px-2 py-1 rounded border border-zinc-800/50">
                                                <Label className="text-xs text-zinc-400 whitespace-nowrap">
                                                    {t('settings.response.threshold_label')}
                                                </Label>
                                                <Input
                                                    type="number"
                                                    className="w-16 h-7 bg-zinc-950 border-red-900/30 focus:border-red-500 text-center text-xs p-0"
                                                    value={getVal('active_response_config.max_violation_threshold', 3)}
                                                    onChange={e => updateField('active_response_config.max_violation_threshold', parseInt(e.target.value))}
                                                />
                                            </div>
                                        </div>
                                    </div>
                                </CardHeader>
                            </Card>
                            <Card className="bg-zinc-950/40 border-zinc-800 flex flex-col h-full">
                                <CardHeader className="pb-3 border-b border-zinc-800/50 bg-zinc-900/20 shrink-0">
                                    <CardTitle className="text-base font-semibold text-zinc-100 flex items-center gap-2">
                                        <FileText className="w-5 h-5 text-blue-400" /> {t('settings.response.history')}
                                    </CardTitle>
                                    <CardDescription className="text-zinc-400">
                                        {t('settings.response.log')}
                                    </CardDescription>
                                </CardHeader>
                                <CardContent className="p-0 flex-1 overflow-auto">
                                    {!auditLogs || auditLogs.length === 0 ? (
                                        <div className="p-8 text-center text-zinc-500 text-sm">
                                            {isLogLoading ? t('settings.response.loag') : t('settings.response.no_action')}
                                        </div>
                                    ) : (
                                        <table className="w-full text-left text-sm text-zinc-400">
                                            <thead className="bg-zinc-900/50 text-zinc-300 sticky top-0 z-10">
                                                <tr>
                                                    <th className="px-4 py-3 font-medium border-b border-zinc-800">{t('settings.response.time')}</th>
                                                    <th className="px-4 py-3 font-medium border-b border-zinc-800">{t('settings.response.action')}</th>
                                                    <th className="px-4 py-3 font-medium border-b border-zinc-800">{t('settings.response.target')}</th>
                                                    <th className="px-4 py-3 font-medium border-b border-zinc-800">{t('settings.response.reason')}</th>
                                                </tr>
                                            </thead>
                                            <tbody className="divide-y divide-zinc-800/50">
                                                {auditLogs.map((log, index) => (
                                                    <tr key={index} className="hover:bg-zinc-900/30 transition-colors">
                                                        <td className="px-4 py-2 whitespace-nowrap font-mono text-xs text-zinc-500">
                                                            {log.timestamp}
                                                        </td>
                                                        <td className="px-4 py-2">
                                                            <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium 
                                                        ${log.action === 'LOCK_ACCOUNT' ? 'bg-red-950 text-red-400 border border-red-900/50' :
                                                                    log.action === 'KILL_SESSION' ? 'bg-orange-950 text-orange-400 border border-orange-900/50' :
                                                                        'bg-zinc-800 text-zinc-300'}`}>
                                                                {log.action}
                                                            </span>
                                                        </td>
                                                        <td className="px-4 py-2 font-mono text-xs text-zinc-300">
                                                            {log.target}
                                                        </td>
                                                        <td className="px-4 py-2 text-zinc-400 truncate max-w-xs" title={log.reason}>
                                                            {log.reason}
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    )}
                                </CardContent>
                            </Card>
                        </TabsContent>

                        {/* === TAB: THRESHOLDS === */}
                        <TabsContent value="thresholds" className="space-y-4 mt-0">
                            <ConfigCard title={t('settings.thresholds.anomaly_detection')} desc="Adjust sensitivity of Rules.">
                                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                                    <FormItem label={t('settings.thresholds.deletion_thresholds')} desc="Rule 25: Mass Deletion">
                                        <Input type="number" value={getVal('security_rules.thresholds.mass_deletion_rows')}
                                            onChange={e => updateField('security_rules.thresholds.mass_deletion_rows', parseInt(e.target.value))} />
                                    </FormItem>
                                    <FormItem label={t('settings.thresholds.execution_time')} desc="Rule 12: DoS / Slow Query">
                                        <Input type="number" value={getVal('security_rules.thresholds.execution_time_limit_ms')}
                                            onChange={e => updateField('security_rules.thresholds.execution_time_limit_ms', parseInt(e.target.value))} />
                                    </FormItem>
                                    <FormItem label={t('settings.thresholds.cpu_time')} desc="Rule 13: High CPU Usage">
                                        <Input type="number" value={getVal('security_rules.thresholds.cpu_time_limit_ms')}
                                            onChange={e => updateField('security_rules.thresholds.cpu_time_limit_ms', parseInt(e.target.value))} />
                                    </FormItem>
                                    <FormItem label={t('settings.thresholds.lock_time')} desc="Rule 20: Excessive Locking">
                                        <Input type="number" value={getVal('security_rules.thresholds.lock_time_limit_ms')}
                                            onChange={e => updateField('security_rules.thresholds.lock_time_limit_ms', parseInt(e.target.value))} />
                                    </FormItem>

                                    <FormItem label={t('settings.thresholds.brute_force')} desc="Rule 2: Login Failure Limit">
                                        <Input type="number" value={getVal('security_rules.thresholds.brute_force_attempts')}
                                            onChange={e => updateField('security_rules.thresholds.brute_force_attempts', parseInt(e.target.value))} />
                                    </FormItem>
                                    <FormItem label={t('settings.thresholds.warnings')} desc="Rule 22: Warning Flooding">
                                        <Input type="number" value={getVal('security_rules.thresholds.warning_count_threshold')}
                                            onChange={e => updateField('security_rules.thresholds.warning_count_threshold', parseInt(e.target.value))} />
                                    </FormItem>

                                    <FormItem label={t('settings.thresholds.concurrent_ips')} desc="Rule 1: Concurrent IPs">
                                        <Input type="number" value={getVal('security_rules.thresholds.concurrent_ips_limit')}
                                            onChange={e => updateField('security_rules.thresholds.concurrent_ips_limit', parseInt(e.target.value))} />
                                    </FormItem>
                                    <FormItem label={t('settings.thresholds.entropy')} desc="Rule 16: SQL Injection / Obfuscation">
                                        <Input type="number" step="0.1" value={getVal('security_rules.thresholds.max_query_entropy')}
                                            onChange={e => updateField('security_rules.thresholds.max_query_entropy', parseFloat(e.target.value))} />
                                    </FormItem>
                                    <FormItem label={t('settings.thresholds.travel_speed')} desc="Rule 3: Impossible Travel">
                                        <Input type="number" value={getVal('security_rules.thresholds.impossible_travel_speed_kmh')}
                                            onChange={e => updateField('security_rules.thresholds.impossible_travel_speed_kmh', parseInt(e.target.value))} />
                                    </FormItem>

                                    <FormItem label={t('settings.thresholds.scan_efficiency')} desc="Rule 14: Scan Efficiency">
                                        <Input type="number" step="0.01" value={getVal('security_rules.thresholds.scan_efficiency_min')}
                                            onChange={e => updateField('security_rules.thresholds.scan_efficiency_min', parseFloat(e.target.value))} />
                                    </FormItem>
                                    <FormItem label={t('settings.thresholds.min_rows')} desc="Rule 14: Min Rows">
                                        <Input type="number" value={getVal('security_rules.thresholds.scan_efficiency_min_rows')}
                                            onChange={e => updateField('security_rules.thresholds.scan_efficiency_min_rows', parseInt(e.target.value))} />
                                    </FormItem>
                                    <FormItem label={t('settings.thresholds.profile_occurrences')} desc="Rule 31: Behavior Profile (Redis)">
                                        <Input type="number" value={getVal('security_rules.thresholds.min_occurrences_threshold')}
                                            onChange={e => updateField('security_rules.thresholds.min_occurrences_threshold', parseInt(e.target.value))} />
                                    </FormItem>
                                </div>
                            </ConfigCard>
                        </TabsContent>

                        {/* === TAB: SIGNATURES === */}
                        <TabsContent value="signatures" className="space-y-4 mt-0">
                            <div className="grid grid-cols-1 gap-6">
                                <div className="space-y-6">
                                    <ConfigCard title={t('settings.signatures.attack_signatures')} desc="Keywords identifying dangerous behaviors.">
                                        <div className="space-y-4">
                                            <FormItem label={t('settings.signatures.sql_injection')} desc="Rule 11: SQL injection attack detected">
                                                <Textarea className="h-32 font-mono text-xs"
                                                    value={getListDisplayValue('security_rules.signatures.sqli_keywords')}
                                                    onChange={e => handleListChange('security_rules.signatures.sqli_keywords', e.target.value)} />
                                            </FormItem>
                                            <FormItem label={t('settings.signatures.admin_privilege')} desc="Rule 5: Escalation of privilege detected">
                                                <Textarea className="h-24 font-mono text-xs"
                                                    value={getListDisplayValue('security_rules.signatures.admin_keywords')}
                                                    onChange={e => handleListChange('security_rules.signatures.admin_keywords', e.target.value)} />
                                            </FormItem>
                                            <FormItem label={t('settings.signatures.lacklisted')} desc="Rule 17: Tools/clients are not allowed to connect">
                                                <Textarea className="h-24 font-mono text-xs"
                                                    value={getListDisplayValue('security_rules.signatures.disallowed_programs')}
                                                    onChange={e => handleListChange('security_rules.signatures.disallowed_programs', e.target.value)} />
                                            </FormItem>
                                            <FormItem label={t('settings.signatures.sensitive_tables')} desc="Rule 6: Table containing confidential data (salary, card details, etc.)">
                                                <Textarea className="h-24 font-mono text-xs"
                                                    value={getListDisplayValue('security_rules.signatures.sensitive_tables')}
                                                    onChange={e => handleListChange('security_rules.signatures.sensitive_tables', e.target.value)} />
                                            </FormItem>
                                            <FormItem label={t('settings.signatures.allowed_users')} desc="Rule 6: Whitelist users view sensitive table">
                                                <Textarea className="h-24 font-mono text-xs"
                                                    value={getListDisplayValue('security_rules.settings.sensitive_allowed_users')}
                                                    onChange={e => handleListChange('security_rules.settings.sensitive_allowed_users', e.target.value)} />
                                            </FormItem>
                                            <FormItem label={t('settings.signatures.large_dump')} desc="Rule 27: Monitoring unauthorized data dumping">
                                                <Textarea className="h-24 font-mono text-xs"
                                                    value={getListDisplayValue('security_rules.signatures.large_dump_tables')}
                                                    onChange={e => handleListChange('security_rules.signatures.large_dump_tables', e.target.value)} />
                                            </FormItem>
                                            <FormItem label={t('settings.signatures.authorized_users')} desc="Rule 8: Users are allowed to create new accounts.">
                                                <Textarea className="h-24 font-mono text-xs"
                                                    value={getListDisplayValue('security_rules.signatures.hr_authorized_users')}
                                                    onChange={e => handleListChange('security_rules.signatures.hr_authorized_users', e.target.value)} />
                                            </FormItem>
                                            <FormItem label={t('settings.signatures.restricted_users')} desc="Rule 10: User is prohibited from connecting via Insecure (TCP/IP)">
                                                <Textarea className="h-24 font-mono text-xs"
                                                    value={getListDisplayValue('security_rules.signatures.restricted_connection_users')}
                                                    onChange={e => handleListChange('security_rules.signatures.restricted_connection_users', e.target.value)} />
                                            </FormItem>
                                        </div>
                                    </ConfigCard>
                                </div>
                            </div>
                        </TabsContent>

                        {/* === TAB: ANALYSIS PARAMS === */}
                        <TabsContent value="analysis" className="space-y-4 mt-0">
                            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                                <ConfigCard title={t('settings.analysis.engine_configuration')} desc="Parameters controlling scanning frequency.">
                                    <FormItem label={t('settings.analysis.sleep_interval')} desc="Engine rest time">
                                        <Input type="number" value={getVal('engine_sleep_interval_seconds')}
                                            onChange={e => updateField('engine_sleep_interval_seconds', parseInt(e.target.value))} />
                                    </FormItem>
                                    <div className="grid grid-cols-2 gap-4 mt-4">
                                        <FormItem label={t('settings.analysis.frouping_window')} desc="Rule 30: The time period to count the number of accessed tables (E.g: 4)">
                                            <Input type="number" value={getVal('security_rules.thresholds.multi_table_window_minutes')}
                                                onChange={e => updateField('security_rules.thresholds.multi_table_window_minutes', parseInt(e.target.value))} />
                                        </FormItem>
                                        <FormItem label={t('settings.analysis.table_count')} desc="Rule 30: The number of different tables accessed triggers the Multi-table alert (E.g: 3)">
                                            <Input type="number" value={getVal('security_rules.thresholds.multi_table_min_count')}
                                                onChange={e => updateField('security_rules.thresholds.multi_table_min_count', parseInt(e.target.value))} />
                                        </FormItem>
                                    </div>
                                </ConfigCard>

                                <ConfigCard title={t('settings.analysis.working_definition')} desc="Standard business hours and late night hours.">
                                    <div className="grid grid-cols-2 gap-4">
                                        <FormItem label={t('settings.analysis.start_hour')} desc="(E.g: 8">
                                            <Input type="number" value={getVal('security_rules.settings.sensitive_safe_hours_start')}
                                                onChange={e => updateField('security_rules.settings.sensitive_safe_hours_start', parseInt(e.target.value))} />
                                        </FormItem>
                                        <FormItem label={t('settings.analysis.end_hour')} desc="(E.g: 17)">
                                            <Input type="number" value={getVal('security_rules.settings.sensitive_safe_hours_end')}
                                                onChange={e => updateField('security_rules.settings.sensitive_safe_hours_end', parseInt(e.target.value))} />
                                        </FormItem>
                                        <FormItem label={t('settings.analysis.night_start')} desc="(E.g: 00:00:00)">
                                            <Input value={getVal('security_rules.settings.late_night_start')}
                                                onChange={e => updateField('security_rules.settings.late_night_start', e.target.value)} />
                                        </FormItem>
                                        <FormItem label={t('settings.analysis.night_end')} desc="(E.g: 05:30:00)">
                                            <Input value={getVal('security_rules.settings.late_night_end')}
                                                onChange={e => updateField('security_rules.settings.late_night_end', e.target.value)} />
                                        </FormItem>
                                    </div>
                                </ConfigCard>
                            </div>
                        </TabsContent>

                        {/* === TAB: AI & LLM === */}
                        <TabsContent value="llm" className="space-y-4 mt-0">
                            <ConfigCard title={t('settings.llm.ai')} desc={t('settings.llm.connect')}>
                                <div className="flex items-center space-x-2 mb-4 border border-zinc-800 p-2 rounded">
                                    <Switch id="ollama-mode"
                                        checked={getVal('llm_config.enable_ollama')}
                                        onCheckedChange={c => updateField('llm_config.enable_ollama', c)}
                                    />
                                    <Label htmlFor="ollama-mode" className="text-white font-bold cursor-pointer">{t('settings.llm.enable')}</Label>
                                </div>

                                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                                    <FormItem label={t('settings.llm.host')} desc="E.g: http://192.168.1.0:11434">
                                        <Input value={getVal('llm_config.ollama_host')}
                                            onChange={e => updateField('llm_config.ollama_host', e.target.value)} />
                                    </FormItem>
                                    <FormItem label={t('settings.llm.model')} desc="E.g: uba-expert">
                                        <Input value={getVal('llm_config.ollama_model')}
                                            onChange={e => updateField('llm_config.ollama_model', e.target.value)} />
                                    </FormItem>
                                    <FormItem label={t('settings.llm.timeout')} desc="E.g: 360">
                                        <Input type="number" value={getVal('llm_config.ollama_timeout')}
                                            onChange={e => updateField('llm_config.ollama_timeout', parseInt(e.target.value))} />
                                    </FormItem>
                                    <FormItem label={t('settings.llm.keep_alive')} desc="Waiting time while alive. E.g: 30m">
                                        <Input value={getVal('llm_config.keep_alive')}
                                            onChange={e => updateField('llm_config.keep_alive', parseInt(e.target.value))} />
                                    </FormItem>
                                </div>
                            </ConfigCard>
                            <ConfigCard title={t('settings.email.title')} desc={t('settings.email.desc')}>
                                <div className="flex items-center space-x-2 mb-6 border border-zinc-800 p-3 rounded bg-zinc-900/50">
                                    <Switch id="email-enable"
                                        checked={getVal('email_alert_config.enable_email_alerts', true)}
                                        onCheckedChange={c => updateField('email_alert_config.enable_email_alerts', c)}
                                    />
                                    <Label htmlFor="email-enable" className="text-white font-bold cursor-pointer">{t('settings.email.enable')}</Label>
                                </div>

                                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                                    <div className="space-y-4">
                                        <h4 className="text-sm font-semibold text-primary-400 uppercase tracking-wider">{t('settings.email.smtp_server')}</h4>
                                        <FormItem label={t('settings.email.host')} desc="E.g: smtp.gmail.com">
                                            <Input value={getVal('email_alert_config.smtp_server')}
                                                onChange={e => updateField('email_alert_config.smtp_server', e.target.value)} />
                                        </FormItem>
                                        <FormItem label={t('settings.email.port')} desc="TLS: 587, SSL: 465">
                                            <Input type="number" value={getVal('email_alert_config.smtp_port')}
                                                onChange={e => updateField('email_alert_config.smtp_port', parseInt(e.target.value))} />
                                        </FormItem>
                                        <div className="border-t border-zinc-800 my-2 pt-2"></div>
                                        <FormItem label={t('settings.email.sender')} desc="Email address">
                                            <Input value={getVal('email_alert_config.sender_email')}
                                                onChange={e => updateField('email_alert_config.sender_email', e.target.value)} />
                                        </FormItem>
                                        <FormItem label={t('settings.email.password')} desc="If you use Gmail 2FA, App Password">
                                            <Input type="password" value={getVal('email_alert_config.sender_password')}
                                                onChange={e => updateField('email_alert_config.sender_password', e.target.value)} />
                                        </FormItem>
                                    </div>

                                    <div className="space-y-4">
                                        <h4 className="text-sm font-semibold text-primary-400 uppercase tracking-wider">{t('settings.email.recipients')}</h4>
                                        <FormItem label={t('settings.email.to')} desc="Email to receive direct alerts (Separated by comma)">
                                            <Textarea className="h-32 font-mono text-xs"
                                                value={getListDisplayValue('email_alert_config.to_recipients')}
                                                onChange={e => handleListChange('email_alert_config.to_recipients', e.target.value)} />
                                        </FormItem>
                                        <FormItem label={t('settings.email.bcc')} desc="Email to receive a hidden copy for monitoring (Separated by comma)">
                                            <Textarea className="h-32 font-mono text-xs"
                                                value={getListDisplayValue('email_alert_config.bcc_recipients')}
                                                onChange={e => handleListChange('email_alert_config.bcc_recipients', e.target.value)} />
                                        </FormItem>
                                    </div>
                                </div>
                            </ConfigCard>
                        </TabsContent>

                        {/* === TAB: ACCOUNT === */}
                        <TabsContent value="account" className="max-w-xl mx-auto space-y-6 mt-6">
                            <Card className="bg-zinc-950/40 border-zinc-800">
                                <CardHeader>
                                    <CardTitle className="text-lg flex items-center gap-2">
                                        <Key className="w-5 h-5 text-primary-500" /> {t('settings.account.change_pass')}
                                    </CardTitle>
                                    <CardDescription>{t('settings.account.change_pass_desc')}</CardDescription>
                                </CardHeader>
                                <form onSubmit={handleChangePassword}>
                                    <CardContent className="space-y-4">
                                        <div className="space-y-2">
                                            <Label>{t('settings.account.current')}</Label>
                                            <Input type="password" required value={passForm.currentPassword}
                                                onChange={e => setPassForm({ ...passForm, currentPassword: e.target.value })}
                                                className="bg-zinc-900 border-zinc-700 focus:border-primary-500" />
                                        </div>
                                        <div className="space-y-2">
                                            <Label>{t('settings.account.new')}</Label>
                                            <Input type="password" required value={passForm.newPassword}
                                                onChange={e => setPassForm({ ...passForm, newPassword: e.target.value })}
                                                className="bg-zinc-900 border-zinc-700 focus:border-primary-500" />
                                        </div>
                                        <div className="space-y-2">
                                            <Label>{t('settings.account.confirm')}</Label>
                                            <Input type="password" required value={passForm.confirmPassword}
                                                onChange={e => setPassForm({ ...passForm, confirmPassword: e.target.value })}
                                                className="bg-zinc-900 border-zinc-700 focus:border-primary-500" />
                                        </div>
                                    </CardContent>
                                    <CardFooter className="flex justify-end border-t border-zinc-800/50 pt-4">
                                        <Button type="submit" disabled={isPassLoading} className="bg-primary-600 hover:bg-primary-700">
                                            {isPassLoading ? "Processing..." : t('settings.account.update_btn')}
                                        </Button>
                                    </CardFooter>
                                </form>
                            </Card>

                            <Card className="bg-red-950/10 border-red-900/30">
                                <CardHeader>
                                    <CardTitle className="text-lg text-red-500 flex items-center gap-2">
                                        <LogOut className="w-5 h-5" /> {t('settings.account.logout_title')}
                                    </CardTitle>
                                </CardHeader>
                                <CardContent className="flex justify-between items-center">
                                    <div className="text-sm text-zinc-400">{t('settings.account.logout_desc')}</div>
                                    <Button variant="destructive" onClick={handleLogout} className="bg-red-600 hover:bg-red-700">{t('settings.account.logout_btn')}</Button>
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