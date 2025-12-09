// uba_frontend/src/pages/Dashboard.jsx
import React, { useMemo, useState } from 'react';
import { 
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, ResponsiveContainer,
  PieChart, Pie, Cell, BarChart, Bar
} from 'recharts';
import { 
  AlertTriangle, Users, Activity, Zap, Server, BrainCircuit, ShieldAlert 
} from 'lucide-react';

import { useAnomalyStats } from '@/api/queries'; 
import { AnomalyDetailModal } from '@/components/AnomalyDetailModal';

export default function Dashboard() {
  const [selectedLog, setSelectedLog] = useState(null);
  const [timeRange, setTimeRange] = useState('24h');
  
  // Gọi API lấy thống kê
  const { data: statsData, isLoading } = useAnomalyStats(timeRange);

  const displayStats = useMemo(() => {
    const defaults = {
      total_scanned: 0, total_anomalies: 0, critical_alerts: 0,
      riskiest_users: [], detection_stats: [], targeted_entities: [],
      chartData: [], latestLogs: []
    };
    if (isLoading || !statsData) return defaults;
    return { ...defaults, ...statsData };
  }, [statsData, isLoading]);

  return (
    <div className="h-full flex flex-col gap-3 overflow-hidden pr-2">
      
      {/* HEADER: Đã xóa badge 'Engine Active', chỉ giữ Title */}
      <header className="shrink-0 flex justify-between items-end pb-2 border-b border-zinc-800 h-10">
        <div>
            <h2 className="text-lg font-bold tracking-tight text-white flex items-center gap-2">
                <ShieldAlert className="w-5 h-5 text-primary-500"/> Security Command Center
            </h2>
        </div>
        {/* Nơi hiển thị Time Filter nhỏ gọn nếu cần */}
      </header>

      {/* MAIN GRID */}
      <div className="flex-1 min-h-0 grid grid-cols-12 gap-4">
        
        {/* === LEFT COLUMN (9 COLS) === */}
        <div className="col-span-9 flex flex-col gap-3 h-full">
            
            {/* ROW 1: KPIs */}
            <div className="grid grid-cols-3 gap-3 h-20 shrink-0">
                <StatCard 
                    title="Queries Scanned" 
                    value={displayStats.total_scanned.toLocaleString()} 
                    icon={Activity} 
                    color="text-blue-400" 
                    borderColor="border-blue-900/30"
                />
                <StatCard 
                    title="Total Anomalies" 
                    value={displayStats.total_anomalies.toLocaleString()} 
                    icon={AlertTriangle} 
                    color="text-yellow-400" 
                    borderColor="border-yellow-900/30"
                />
                <StatCard 
                    title="Critical Threats" 
                    value={displayStats.critical_alerts.toLocaleString()} 
                    icon={Zap} 
                    color="text-red-500" 
                    borderColor="border-red-900/50 bg-red-900/10"
                />
            </div>

            {/* ROW 2: VELOCITY CHART */}
            <div className="flex-[4] bg-zinc-900/40 border border-zinc-800 rounded-xl p-3 flex flex-col min-h-0 relative">
                 <div className="flex justify-between items-start mb-2">
                    <h3 className="text-xs font-semibold text-zinc-400 flex items-center gap-2">
                        <Activity className="w-3 h-3 text-blue-500"/> Anomaly Velocity
                    </h3>
                    <div className="flex bg-zinc-950 rounded border border-zinc-800 p-0.5">
                        {['D', 'W', 'M', '6M', 'Y'].map((range) => (
                            <button
                                key={range}
                                onClick={() => setTimeRange(range)}
                                className={`px-2 py-0.5 text-[10px] font-medium rounded transition-all ${
                                    timeRange === range 
                                    ? 'bg-zinc-800 text-white shadow-sm' 
                                    : 'text-zinc-500 hover:text-zinc-300'
                                }`}
                            >
                                {range}
                            </button>
                        ))}
                    </div>
                </div>
                
                <div className="flex-1 min-h-0 w-full">
                    <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={displayStats.chartData} margin={{ top: 5, right: 10, left: -20, bottom: 0 }}>
                        <defs>
                        <linearGradient id="colorAnomaly" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#f97316" stopOpacity={0.3}/>
                            <stop offset="95%" stopColor="#f97316" stopOpacity={0}/>
                        </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="#27272a" vertical={false} />
                        <XAxis dataKey="name" stroke="#52525b" fontSize={10} tickLine={false} axisLine={false} minTickGap={15}/>
                        <YAxis stroke="#52525b" fontSize={10} tickLine={false} axisLine={false} />
                        <RechartsTooltip contentStyle={{ backgroundColor: '#09090b', borderColor: '#27272a', fontSize: '12px' }} itemStyle={{ color: '#fff' }}/>
                        <Area type="monotone" dataKey="anomalies" stroke="#f97316" strokeWidth={2} fill="url(#colorAnomaly)" isAnimationActive={false} />
                    </AreaChart>
                    </ResponsiveContainer>
                </div>
            </div>

            {/* ROW 3: DISTRIBUTION & TARGETS */}
            <div className="flex-[3] grid grid-cols-2 gap-3 min-h-0">
                
                {/* Threat Distribution (Pie Chart) */}
                <div className="bg-zinc-900/40 border border-zinc-800 rounded-xl p-3 flex flex-col">
                    <div className="flex justify-between items-center mb-1">
                        <h3 className="text-xs font-semibold text-zinc-400 flex items-center gap-2">
                            <BrainCircuit className="w-3 h-3 text-purple-500"/> Threat Distribution
                        </h3>
                        <span className="text-[10px] text-zinc-500 bg-zinc-900 px-2 py-0.5 rounded border border-zinc-800">
                             Total: <span className="text-white font-bold">{displayStats.total_anomalies}</span>
                        </span>
                    </div>
                    
                    <div className="flex-1 min-h-0 relative flex items-center">
                        {/* Biểu đồ tròn (Không có chữ ở giữa) */}
                        <div className="w-[50%] h-full">
                            <ResponsiveContainer width="100%" height="100%">
                                <PieChart>
                                    <Pie
                                        data={displayStats.detection_stats}
                                        cx="50%"
                                        cy="50%"
                                        innerRadius={0}
                                        outerRadius={110}
                                        paddingAngle={0}
                                        dataKey="value"
                                        stroke="none"
                                    >
                                        {displayStats.detection_stats.map((entry, index) => (
                                            <Cell key={`cell-${index}`} fill={entry.color} />
                                        ))}
                                    </Pie>
                                    <RechartsTooltip formatter={(value, name) => [value, name]} contentStyle={{backgroundColor: '#18181b', border: 'none', fontSize:'11px'}} itemStyle={{color: '#fff'}}/>
                                </PieChart>
                            </ResponsiveContainer>
                        </div>

                        {/* Chú thích (Legend) nằm bên phải */}
                        <div className="w-[50%] h-full overflow-y-auto custom-scrollbar pl-2 pr-1 py-1">
                            <div className="flex flex-col gap-1.5">
                                {displayStats.detection_stats.map((entry, index) => (
                                    <div key={index} className="flex items-center justify-between text-[13px] group">
                                        <div className="flex items-center gap-1.5 overflow-hidden">
                                            <span className="w-2 h-2 rounded-sm shrink-0" style={{ backgroundColor: entry.color }} />
                                            <span className="text-zinc-400 truncate group-hover:text-zinc-200 transition-colors" title={entry.name}>
                                                {entry.name}
                                            </span>
                                        </div>
                                        <span className="text-zinc-300 font-mono font-bold shrink-0">
                                            {entry.value}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                </div>

                {/* Top Targets */}
                <div className="bg-zinc-900/40 border border-zinc-800 rounded-xl p-3 flex flex-col">
                    <h3 className="text-xs font-semibold text-zinc-400 mb-1 flex items-center gap-2">
                        <Server className="w-3 h-3 text-emerald-500"/> Top Targets
                    </h3>
                    <div className="flex-1 min-h-0">
                        <ResponsiveContainer width="100%" height="100%">
                            <BarChart layout="vertical" data={displayStats.targeted_entities} margin={{left: 0, right: 20, top: 5, bottom: 0}} barCategoryGap={15}>
                                <XAxis type="number" hide />
                                <YAxis dataKey="name" type="category" width={90} tick={{fontSize: 10, fill: '#a1a1aa'}} axisLine={false} tickLine={false}/>
                                <RechartsTooltip cursor={{fill: '#27272a'}} contentStyle={{backgroundColor: '#18181b', border: 'none', fontSize:'10px'}} />
                                <Bar dataKey="value" fill="#10b981" radius={[0, 4, 4, 0]} barSize={12} />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>
        </div>

        {/* === RIGHT COLUMN (3 COLS) === */}
        <div className="col-span-3 flex flex-col gap-3 h-full">
            
            {/* RISKY USERS (Top 10) */}
            <div className="flex-1 bg-zinc-900/40 border border-zinc-800 rounded-xl p-3 flex flex-col overflow-hidden">
                <div className="flex justify-between items-center mb-2">
                    <h3 className="text-xs font-semibold text-zinc-400 flex items-center gap-2">
                        <Users className="w-3 h-3 text-red-500"/> Risky Users (UEBA)
                    </h3>
                    <span className="text-[9px] text-zinc-600 bg-zinc-900 px-1.5 py-0.5 rounded">Top 10</span>
                </div>
                
                <div className="flex-1 overflow-y-auto pr-1 space-y-1.5 custom-scrollbar">
                    {displayStats.riskiest_users.length === 0 && <div className="text-zinc-600 text-[10px] text-center py-10">No risky users detected</div>}
                    
                    {displayStats.riskiest_users.map((u, idx) => (
                        <div key={idx} className="flex items-center justify-between p-2 rounded bg-zinc-900/60 border border-zinc-800/50 hover:border-red-500/30 hover:bg-zinc-900 transition-all group">
                            <div className="flex items-center gap-2 min-w-0">
                                <div className={`shrink-0 w-5 h-5 rounded flex items-center justify-center text-[10px] font-bold ${idx < 3 ? 'bg-red-900/30 text-red-400' : 'bg-zinc-800 text-zinc-500'}`}>
                                    {idx + 1}
                                </div>
                                <div className="min-w-0">
                                    <div className="text-xs font-medium text-zinc-300 group-hover:text-white truncate">{u.user}</div>
                                </div>
                            </div>
                            <div className="text-right shrink-0">
                                <div className="text-xs font-bold text-red-400">{u.score.toFixed(0)} <span className="text-[9px] font-normal text-zinc-600">pts</span></div>
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            {/* LIVE FEED (Color Coded) */}
            <div className="flex-1 bg-zinc-900/40 border border-zinc-800 rounded-xl p-0 flex flex-col overflow-hidden">
                <div className="p-3 border-b border-zinc-800 bg-zinc-900/80 backdrop-blur flex justify-between items-center">
                    <h3 className="text-xs font-semibold flex items-center gap-2 text-zinc-400">
                        <Zap className="w-3 h-3 text-yellow-500"/> Live Feed
                    </h3>
                    <span className="w-1.5 h-1.5 rounded-full bg-green-500 animate-pulse"/>
                </div>
                <div className="flex-1 overflow-y-auto p-2 custom-scrollbar space-y-1">
                    {displayStats.latestLogs.length === 0 && <div className="text-zinc-600 text-[10px] text-center py-10">Waiting for events...</div>}
                    {displayStats.latestLogs.map((log, index) => (
                        <LogItem 
                            key={index}
                            log={log}
                            onClick={() => setSelectedLog(log)}
                        />
                    ))}
                </div>
            </div>

        </div>

      </div>

      <AnomalyDetailModal
        isOpen={!!selectedLog}
        onClose={() => setSelectedLog(null)}
        log={selectedLog}
        onAnalyze={() => {}} 
        onFeedback={() => {}}
        isAiLoading={false}
        isFeedbackLoading={false}
      />
    </div>
  );
}

// === COMPONENTS ===

const StatCard = ({ title, value, icon: Icon, color, borderColor }) => (
  <div className={`h-full bg-zinc-900/40 border ${borderColor || 'border-zinc-800'} px-4 flex items-center justify-between rounded-xl backdrop-blur-sm shadow-sm`}>
    <div>
        <p className="text-2xl font-bold text-white tracking-tight leading-none">{value}</p>
        <p className="text-[10px] text-zinc-500 font-medium uppercase mt-1 tracking-wider">{title}</p>
    </div>
    <div className={`p-2 rounded-lg bg-zinc-950/50 border border-zinc-800 ${color}`}>
        <Icon className="h-5 w-5" />
    </div>
  </div>
);

// [MODIFIED] LogItem với logic màu sắc
const LogItem = ({ log, onClick }) => {
    // Xác định mức độ nguy hiểm
    // Các nhóm này được coi là Nguy hiểm (Đỏ)
    const CRITICAL_GROUPS = ['TECHNICAL_ATTACK', 'DATA_DESTRUCTION', 'SQL Injection', 'Privilege Escalation'];
    
    // Check nếu thuộc nhóm nguy hiểm HOẶC score rất cao
    const isCritical = CRITICAL_GROUPS.some(g => log.anomaly_type.includes(g) || log.reason?.includes(g)) || log.score >= 0.8;
    
    // Style classes
    const containerClass = isCritical 
        ? "border-red-900/30 bg-red-950/10 hover:bg-red-900/20" 
        : "border-yellow-900/20 bg-yellow-950/5 hover:bg-yellow-900/10";
    
    const textClass = isCritical ? "text-red-400" : "text-yellow-500";
    const badgeClass = isCritical ? "bg-red-900/20 text-red-300 border-red-800/50" : "bg-yellow-900/20 text-yellow-300 border-yellow-800/50";

    const isAggregate = log.source === 'aggregate';

    return (
        <div 
            onClick={onClick}
            className={`flex items-center justify-between p-2 rounded border ${containerClass} cursor-pointer transition-all group`}
        >
            <div className="flex items-center gap-2 min-w-0">
                {/* Badge loại lỗi */}
                <span className={`text-[8px] font-bold px-1.5 py-0.5 rounded border ${badgeClass} w-20 truncate uppercase text-center`}>
                    {log.anomaly_type.split(';')[0]} {/* Lấy rule đầu tiên nếu có nhiều */}
                </span>
                
                {/* User Info */}
                <div className="flex flex-col min-w-0">
                    <span className={`text-xs font-medium truncate ${textClass} group-hover:brightness-125`}>
                        {log.user}
                    </span>
                    {isAggregate && <span className="text-[8px] text-zinc-600 uppercase tracking-tight">Session Aggregation</span>}
                </div>
            </div>
            
            {/* Timestamp */}
            <span className="text-[9px] font-mono text-zinc-600 shrink-0">
                {new Date(log.timestamp).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'})}
            </span>
        </div>
    );
};