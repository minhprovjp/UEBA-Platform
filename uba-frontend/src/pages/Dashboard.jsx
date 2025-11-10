// src/pages/Dashboard.jsx
import React from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { AlertTriangle, ShieldCheck, Users, Activity } from 'lucide-react';
import { useAnomalyStats } from '@/api/queries'; // Import hook dữ liệu thật

export default function Dashboard() {
  // --- Lấy dữ liệu thật từ React Query ---
  const { data: stats, isLoading } = useAnomalyStats();

  // Dữ liệu mặc định khi đang tải
  const displayStats = {
    totalAnomalies: stats?.totalAnomalies || 0,
    criticalAlerts: stats?.criticalAlerts || 0,
    chartData: stats?.chartData || [],
    riskiestUser: "Loading...", // Logic này cần phức tạp hơn
  };

  return (
    <div className="h-full flex flex-col">
      <header>
        <h2 className="text-2xl font-semibold">Command Center</h2>
        <p className="text-muted-foreground">Tổng quan an ninh và hoạt động hệ thống.</p>
      </header>

      {/* 1. Thẻ KPI (Dữ liệu thật) */}
      <div className="grid grid-cols-4 gap-4 my-4">
        <StatCard 
          title="Total Anomalies" 
          value={isLoading ? "..." : displayStats.totalAnomalies} 
          icon={AlertTriangle} 
          color="text-red-500" 
        />
        <StatCard 
          title="Critical Alerts" 
          value={isLoading ? "..." : displayStats.criticalAlerts} 
          icon={ShieldCheck} 
          color="text-primary-500" 
        />
        <StatCard 
          title="Riskiest User" 
          value={isLoading ? "..." : displayStats.riskiestUser} 
          icon={Users} 
          color="text-yellow-500" 
        />
        <StatCard 
          title="Engine Health" 
          value="99.7%" 
          icon={Activity} 
          color="text-green-500" 
        />
      </div>

      {/* 2. Biểu đồ (Dữ liệu thật) */}
      <div className="flex-1 grid grid-cols-3 gap-6">
        <div className="col-span-2 bg-zinc-900 border border-border rounded-lg p-4">
          <h3 className="text-lg font-semibold mb-4">Anomaly Activity Overview</h3>
          <ResponsiveContainer width="100%" height={300}>
            {isLoading ? (
              <div className="flex items-center justify-center h-full text-muted-foreground">Đang tải dữ liệu biểu đồ...</div>
            ) : (
              <AreaChart data={displayStats.chartData}>
                <defs>
                  <linearGradient id="colorAnomaly" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#f97316" stopOpacity={0.8}/>
                    <stop offset="95%" stopColor="#f97316" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
                <XAxis dataKey="name" stroke="#71717a" />
                <YAxis stroke="#71717a" />
                <Tooltip
                  contentStyle={{ backgroundColor: '#18181b', border: '1px solid #3f3f46' }}
                  labelStyle={{ color: '#f4f4f5' }}
                />
                <Area type="monotone" dataKey="anomalies" stroke="#f97316" fill="url(#colorAnomaly)" />
              </AreaChart>
            )}
          </ResponsiveContainer>
        </div>
        
        {/* Bảng Activity Log (Vẫn là dữ liệu giả, vì nó mang tính chất "live") */}
        <div className="col-span-1 bg-zinc-900 border border-border rounded-lg p-4 flex flex-col">
          <h3 className="text-lg font-semibold mb-4">Live Activity Log</h3>
          <div className="flex-1 overflow-auto space-y-3">
            <LogItem time="14:32:10" user="root" action="Triggered 'sensitive_access'" risk="High" />
            <LogItem time="14:30:05" user="admin" action="Triggered 'late_night_query'" risk="Medium" />
            <LogItem time="14:28:15" user="webapp" action="Logged in from new IP" risk="Low" />
          </div>
        </div>
      </div>
    </div>
  );
}

// (Các component StatCard và LogItem giữ nguyên như cũ)
const StatCard = ({ title, value, icon: Icon, color }) => (
  <div className="bg-zinc-900 border border-border p-4 rounded-lg">
    <div className="flex justify-between items-center mb-1">
      <p className="text-sm text-muted-foreground uppercase">{title}</p>
      <Icon className={`h-5 w-5 ${color || 'text-muted-foreground'}`} />
    </div>
    <p className={`text-3xl font-bold ${color || ''}`}>{value}</p>
  </div>
);

const LogItem = ({ time, user, action, risk }) => (
  <div className="text-sm">
    <p className="text-muted-foreground">{time}</p>
    <p>
      User <span className="text-primary-500">{user}</span> {action}.
      <span className={`ml-2 font-semibold ${risk === 'High' ? 'text-red-500' : 'text-yellow-500'}`}>
        [{risk}]
      </span>
    </p>
  </div>
);