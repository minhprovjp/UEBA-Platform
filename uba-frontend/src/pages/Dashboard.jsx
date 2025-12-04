import React, { useMemo, useState } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { AlertTriangle, ShieldCheck, Users, Activity, Zap } from 'lucide-react';
import { useAnomalySearch } from '@/api/queries'; 
import { AnomalyDetailModal } from '@/components/AnomalyDetailModal'; // Đảm bảo import đúng đường dẫn

export default function Dashboard() {
  const [selectedLog, setSelectedLog] = useState(null);

  // 1. Thêm state để lưu mốc thời gian đang chọn (Mặc định là 24h)
  const [timeRange, setTimeRange] = useState('24h');

  // 2. Tính toán bộ lọc dựa trên timeRange
  const filters = useMemo(() => {
    const now = new Date();
    const fromDate = new Date();

    // Trừ thời gian tương ứng
    if (timeRange === '24h') {
      fromDate.setHours(fromDate.getHours() - 24);
    } else if (timeRange === '7d') {
      fromDate.setDate(fromDate.getDate() - 7);
    } else if (timeRange === '30d') {
      fromDate.setDate(fromDate.getDate() - 30);
    }

    return {
      pageIndex: 0,
      pageSize: 1000, // Lấy mẫu lớn hơn chút để biểu đồ 30 ngày nhìn đẹp hơn
      search: '',
      user: '',
      anomaly_type: '',
      // Backend của bạn đã hỗ trợ lọc theo date_from/date_to (định dạng ISO)
      date_from: fromDate.toISOString(),
      date_to: now.toISOString(),
    };
  }, [timeRange]); // Khi timeRange thay đổi, filters sẽ tự tính lại

  const { data, isLoading } = useAnomalySearch(filters);

  const displayStats = useMemo(() => {
    if (!data?.items) return {
        totalAnomalies: 0, criticalAlerts: 0, riskiestUser: "—", chartData: [], latestLogs: []
    };

    const items = data.items;
    
    // Tính toán lại như cũ
    const criticalAlerts = items.filter(item => (item.score || 0) > 0.8).length;
    
    // Riskiest User
    const userCounts = {};
    items.forEach(i => i.user && (userCounts[i.user] = (userCounts[i.user] || 0) + 1));
    let riskiestUser = "—";
    let maxCount = 0;
    Object.entries(userCounts).forEach(([u, c]) => { if(c > maxCount) { maxCount = c; riskiestUser = u; }});

    // Chart Data
    const hoursMap = {};
    for (let i = 0; i < 24; i++) hoursMap[`${i}:00`] = 0;
    items.forEach(item => {
        if (item.timestamp) {
            const h = new Date(item.timestamp).getHours();
            hoursMap[`${h}:00`] = (hoursMap[`${h}:00`] || 0) + 1;
        }
    });
    const chartData = Object.keys(hoursMap)
        .map(k => ({ name: k, anomalies: hoursMap[k] }))
        .sort((a, b) => parseInt(a.name) - parseInt(b.name));

    return {
      totalAnomalies: data.total,
      criticalAlerts,
      riskiestUser,
      chartData,
      latestLogs: items.slice(0, 10) // Lấy 7 log để danh sách dài đẹp hơn
    };
  }, [data]);

  return (
    <div className="h-full flex flex-col min-h-0 overflow-hidden space-y-4">
      {/* Header với hiệu ứng Live */}
      <header className="shrink-0 flex justify-between items-end">
        <div>
            <h2 className="text-2xl font-bold tracking-tight">Command Center</h2>
            <p className="text-muted-foreground">Real-time threat intelligence overview.</p>
        </div>
        <div className="flex items-center gap-2 px-3 py-1 bg-zinc-900 rounded-full border border-zinc-800">
            <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
            </span>
            <span className="text-xs font-mono text-green-400">SYSTEM LIVE</span>
        </div>
      </header>

      {/* KPI Cards */}
      <div className="grid grid-cols-4 gap-4 shrink-0">
        <StatCard title="Total Events" value={displayStats.totalAnomalies} icon={AlertTriangle} color="text-orange-500" sub="Last 24 hours" />
        <StatCard title="Critical Threats" value={displayStats.criticalAlerts} icon={ShieldCheck} color="text-red-500" sub="Require attention" />
        <StatCard title="Top Risk User" value={displayStats.riskiestUser} icon={Users} color="text-yellow-500" sub="Behavior analysis" />
        <StatCard title="System Health" value="99.9%" icon={Activity} color="text-emerald-500" sub="All services optimal" />
      </div>

      {/* Main Content */}
      <div className="flex-1 grid grid-cols-12 gap-6 min-h-0 overflow-hidden">
        
        {/* Chart Section (9 cols) */}
        <div className="col-span-9 bg-zinc-900/50 border border-zinc-800 rounded-xl p-5 flex flex-col min-h-0 relative backdrop-blur-sm">
          <div className="flex justify-between items-center mb-4">
            <h3 className="font-semibold flex items-center gap-2">
                <Activity className="w-4 h-4 text-orange-500"/> Anomaly Velocity
            </h3>
            {/* Giả lập bộ lọc thời gian */}
            <div className="flex text-xs bg-zinc-950 rounded p-1 border border-zinc-800">
                {['24h', '7d', '30d'].map((range) => (
                    <button
                        key={range}
                        onClick={() => setTimeRange(range)} // Bấm vào thì set state
                        className={`px-2 py-0.5 rounded transition-colors ${
                            timeRange === range 
                                ? 'bg-zinc-800 text-white font-medium' // Style khi đang chọn
                                : 'text-zinc-400 hover:text-white hover:bg-zinc-800/50' // Style khi chưa chọn
                        }`}
                    >
                        {range}
                    </button>
                ))}
            </div>
          </div>
          <div className="flex-1 min-h-0">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={displayStats.chartData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id="colorAnomaly" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#f97316" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#f97316" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#27272a" vertical={false} />
                <XAxis dataKey="name" stroke="#52525b" fontSize={12} tickLine={false} axisLine={false} interval={0} />
                <YAxis stroke="#52525b" fontSize={12} tickLine={false} axisLine={false} />
                <Tooltip 
                    contentStyle={{ backgroundColor: '#09090b', borderColor: '#27272a', borderRadius: '8px' }}
                    itemStyle={{ color: '#fff' }}
                />
                <Area type="monotone" dataKey="anomalies" stroke="#f97316" strokeWidth={2} fill="url(#colorAnomaly)" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
        
        {/* Activity Log (3 cols) */}
        <div className="col-span-3 bg-zinc-900/50 border border-zinc-800 rounded-xl p-0 flex flex-col min-h-0 backdrop-blur-sm overflow-hidden">
          <div className="p-4 border-b border-zinc-800 bg-zinc-900/80">
            <h3 className="font-semibold flex items-center gap-2">
                <Zap className="w-4 h-4 text-yellow-500"/> Live Feed
            </h3>
          </div>
          <div className="flex-1 overflow-auto p-2">
            {!isLoading && displayStats.latestLogs.length === 0 && (
                <div className="p-4 text-center text-muted-foreground text-sm">No recent activity detected.</div>
            )}
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

      {/* Modal - Hidden until clicked */}
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

// Sub-components đã được làm đẹp
const StatCard = ({ title, value, icon: Icon, color, sub }) => (
  <div className="bg-zinc-900/50 border border-zinc-800 p-4 rounded-xl backdrop-blur-sm hover:border-zinc-700 transition-colors">
    <div className="flex justify-between items-start mb-2">
      <div className={`p-2 rounded-lg bg-zinc-950 border border-zinc-800 ${color}`}>
        <Icon className="h-5 w-5" />
      </div>
      {/* Sparkline giả lập (nếu muốn) */}
    </div>
    <div>
        <p className="text-3xl font-bold text-foreground tracking-tight">{value}</p>
        <p className="text-xs text-muted-foreground font-medium uppercase mt-1">{title}</p>
    </div>
  </div>
);

const LogItem = ({ log, onClick }) => {
    const riskColor = (log.score || 0) > 0.8 ? 'text-red-400 bg-red-400/10 border-red-400/20' 
        : (log.score || 0) > 0.5 ? 'text-yellow-400 bg-yellow-400/10 border-yellow-400/20' 
        : 'text-blue-400 bg-blue-400/10 border-blue-400/20';

    return (
        <div 
            onClick={onClick}
            className="group flex flex-col gap-1 p-3 mb-2 rounded-lg border border-transparent hover:border-zinc-700 hover:bg-zinc-800/50 cursor-pointer transition-all"
        >
            <div className="flex justify-between items-center">
                <span className="text-xs font-mono text-zinc-500">{new Date(log.timestamp).toLocaleTimeString()}</span>
                <span className={`text-[10px] px-1.5 py-0.5 rounded border ${riskColor} font-medium`}>
                    {(log.score || 0) > 0.8 ? 'CRITICAL' : 'WARNING'}
                </span>
            </div>
            <div className="text-sm">
                <span className="font-semibold text-zinc-200">{log.user}</span>
                <span className="text-zinc-400"> triggered </span>
                <span className="text-zinc-300 font-medium">{log.anomaly_type}</span>
            </div>
        </div>
    );
};