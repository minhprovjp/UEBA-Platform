// uba_frontend/src/pages/AnomalyTriage.jsx
import React, { useState } from 'react'
import { useAnomalyKpis, useAnomalyFacets, useAnomalySearch, useAnalyzeMutation, useFeedbackMutation } from '@/api/queries'
import { AnomalyDetailModal } from '@/components/AnomalyDetailModal'
import { Toaster, toast } from 'sonner'
import { Activity, Layers, ShieldAlert, UserX, Database, Cpu, BrainCircuit, UserCog} from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query'

export default function AnomalyTriage() {
  // State quản lý bộ lọc
  const [filters, setFilters] = useState({
    pageIndex: 0,
    pageSize: 20,
    search: '',
    user: '',
    anomaly_type: '',    
    behavior_group: '',  
    date_from: '',
    date_to: '',
  })

  // Gọi các API Hooks
  const { data: kpis } = useAnomalyKpis()
  const { data: facets } = useAnomalyFacets(filters.behavior_group)
  const { data } = useAnomalySearch(filters)

  const rows = data?.items ?? []
  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / filters.pageSize))
  const users = facets?.users ?? []
  const types = facets?.types ?? []

  // State cho Modal chi tiết
  const [selectedLog, setSelectedLog] = useState(null)

  // Mutations cho AI và Feedback
  const analyzeMutation = useAnalyzeMutation();
  const feedbackMutation = useFeedbackMutation();

  // Hàm chuyển đổi nhóm hành vi khi click vào KPI Card
  const toggleGroup = (group) =>
    setFilters(p => ({ 
      ...p, 
      behavior_group: p.behavior_group === group ? '' : group, 
      anomaly_type: '',
      pageIndex: 0 
    }))

  // Hàm tính điểm (Score) cơ bản
  const getScoreValue = (r) => {
    const tryPick = (...ks) => {
      for (const k of ks) {
        const v = k.split('.').reduce((acc, key) => (acc && acc[key] != null ? acc[key] : undefined), r)
        if (v != null && !isNaN(Number(v))) return Number(v)
      }
      return null
    }
    let s = tryPick('score', 'severity', 'details.score', 'details.anomaly_score', 'details.probability', 'details.confidence')
    if (s != null && s > 1 && s <= 100) s = s / 100 
    return s
  }

  // --- LOGIC MỚI: Xử lý hiển thị Score ---
  // 1. Kiểm tra xem có nên hiện cột Score không? (Chỉ hiện ở Tab ML hoặc All Alerts)
  const showScoreCol = !filters.behavior_group || filters.behavior_group === 'ML_DETECTED';

  // 2. Xác định Grid Layout dựa trên việc có cột Score hay không
  // Nếu hiện Score: 6 cột. Nếu ẩn Score: 5 cột (cột cuối biến mất, các cột fr tự giãn ra)
  const gridLayoutClass = showScoreCol 
    ? "grid-cols-[11rem,8rem,10rem,2fr,1fr,5rem]" 
    : "grid-cols-[11rem,8rem,10rem,2fr,1fr]";

  // 3. Render nội dung bên trong ô Score
  const renderScoreCell = (r) => {
    const rawScore = getScoreValue(r);
    const displayScore = rawScore != null ? rawScore.toFixed(2) : '—';
    const scoreColor = rawScore >= 0.8 ? 'text-red-500' : 'text-zinc-400';

    // Trường hợp 1: Đang ở Tab ML Detected -> Luôn hiện điểm
    if (filters.behavior_group === 'ML_DETECTED') {
      return <span className={scoreColor}>{displayScore}</span>;
    }

    // Trường hợp 2: Đang ở Tab All Alerts
    if (!filters.behavior_group) {
      // Chỉ hiện điểm nếu log này thuộc nhóm ML, ngược lại hiện N/A
      if (r.behavior_group === 'ML_DETECTED') {
        return <span className={scoreColor}>{displayScore}</span>;
      } else {
        return <span className="text-zinc-600 font-normal text-xs">N/A</span>;
      }
    }
    
    return null;
  };

  const queryClient = useQueryClient();

  // Xử lý gọi phân tích AI
  const handleAnalyze = () => {
    if (!selectedLog) return;
    analyzeMutation.mutate(selectedLog, {
      onSuccess: (res) => {
        const data = res.data || res; 
        setSelectedLog(prev => ({
          ...prev, 
          aiAnalysis: data.final_analysis || data.first_analysis || data 
        }));
        queryClient.invalidateQueries(['anomalySearch']);
        toast.success("Phân tích AI hoàn tất và đã được lưu!");
      },
      onError: (error) => {
        toast.error(`Lỗi: ${error.message || "Không thể kết nối tới AI"}`);
      }
    });
  };

  // Xử lý gửi feedback
  const handleFeedback = (label) => {
    if (!selectedLog) return;
    feedbackMutation.mutate(
      { label, anomaly_data: selectedLog },
      { onSuccess: () => setSelectedLog(null) }
    );
  };

  // Hàm render cột "Content"
  const renderContent = (r) => {
    if (r.source === 'aggregate') {
      const tableCount = r.details?.tables?.length || 0;
      const queryCount = r.details?.query_count || 0;
      const duration = r.details?.duration_sec ? `${r.details.duration_sec.toFixed(1)}s` : 'N/A';
      return (
        <div className="flex flex-col justify-center h-full text-sm">
          <div className="font-semibold text-orange-400 flex items-center gap-2">
            <Layers className="w-3 h-3" />
            Session Aggregation
          </div>
          <div className="text-zinc-500 text-xs mt-0.5">
            Accessed <span className="text-zinc-300 font-mono">{tableCount} tables</span> with {queryCount} queries in {duration}.
          </div>
        </div>
      );
    }
    return (
      <div className="bg-zinc-950/40 rounded-md px-2 py-1 text-sm whitespace-pre-wrap h-16 overflow-y-auto font-mono text-zinc-300 custom-scrollbar">
        {r.query || '—'}
      </div>
    );
  };

  return (
    <div className="h-full min-h-0 flex flex-col gap-4 overflow-hidden">
      <Toaster position="top-right" theme="dark"/>
      
      {/* --- KPI CARDS --- */}
      <div className="grid grid-cols-8 gap-3 shrink-0">
        <KpiCard title="Technical Attacks" value={kpis?.technical_attack ?? 0} icon={Cpu} active={filters.behavior_group==='TECHNICAL_ATTACK'} onClick={()=>toggleGroup('TECHNICAL_ATTACK')} color="text-red-400" borderColor="hover:border-red-500/50"/>
        <KpiCard title="Insider Threats" value={kpis?.insider_threat ?? 0} icon={UserX} active={filters.behavior_group==='INSIDER_THREAT'} onClick={()=>toggleGroup('INSIDER_THREAT')} color="text-orange-400" borderColor="hover:border-orange-500/50"/>
        <KpiCard title="Behavioral Profile" value={kpis?.behavioral_profile ?? 0} icon={UserCog} active={filters.behavior_group==='UNUSUAL_BEHAVIOR'} onClick={()=>toggleGroup('UNUSUAL_BEHAVIOR')} color="text-indigo-400" borderColor="hover:border-indigo-500/50"/>
        <KpiCard title="Data Destruction" value={kpis?.data_destruction ?? 0} icon={Database} active={filters.behavior_group==='DATA_DESTRUCTION'} onClick={()=>toggleGroup('DATA_DESTRUCTION')} color="text-pink-500" borderColor="hover:border-pink-500/50"/>
        <KpiCard title="Access Anomalies" value={kpis?.access_anomaly ?? 0} icon={ShieldAlert} active={filters.behavior_group==='ACCESS_ANOMALY'} onClick={()=>toggleGroup('ACCESS_ANOMALY')} color="text-yellow-400" borderColor="hover:border-yellow-500/50"/>
        <KpiCard title="Multi-Table" value={kpis?.multi_table ?? 0} icon={Layers} active={filters.behavior_group==='MULTI_TABLE_ACCESS'} onClick={()=>toggleGroup('MULTI_TABLE_ACCESS')} color="text-blue-400" borderColor="hover:border-blue-500/50"/>
        <KpiCard title="AI / ML Detected" value={kpis?.ml_detected ?? 0} icon={BrainCircuit} active={filters.behavior_group==='ML_DETECTED'} onClick={()=>toggleGroup('ML_DETECTED')} color="text-purple-400" borderColor="hover:border-purple-500/50"/>
        <KpiCard title="ALL ALERTS" value={kpis?.total ?? 0} icon={Activity} active={!filters.behavior_group} onClick={()=>toggleGroup('')} color="text-white"/>
      </div>

      {/* --- THANH CÔNG CỤ --- */}
      <div className="flex gap-2 items-center shrink-0">
        <input
          className="bg-zinc-900 border border-zinc-700 rounded-md px-3 py-2 w-80 text-sm focus:outline-none focus:border-zinc-500"
          placeholder="Search query / reason…"
          value={filters.search}
          onChange={(e) => setFilters(p => ({ ...p, search: e.target.value, pageIndex: 0 }))}
        />
        <select className="bg-zinc-900 border border-zinc-700 rounded-md px-3 py-2 text-sm focus:outline-none focus:border-zinc-500" value={filters.user} onChange={(e) => setFilters(p => ({ ...p, user: e.target.value.trim(), pageIndex: 0 }))}>
          <option value="">All Users</option>
          {users.map((u) => <option key={u} value={u}>{u}</option>)}
        </select>
        <select className="bg-zinc-900 border border-zinc-700 rounded-md px-3 py-2 text-sm focus:outline-none focus:border-zinc-500" value={filters.anomaly_type} onChange={(e) => setFilters(p => ({ ...p, anomaly_type: e.target.value.trim(), pageIndex: 0 }))}>
          <option value="">All Rules</option>
          {types.map((t) => <option key={t} value={t}>{t}</option>)}
        </select>
        {(filters.behavior_group || filters.anomaly_type || filters.user || filters.search) && (
          <button className="px-3 py-2 rounded-md border border-zinc-700 bg-zinc-900 text-sm hover:bg-zinc-800 transition-colors" onClick={() => setFilters({ ...filters, search:'', user:'', anomaly_type:'', behavior_group: '', pageIndex:0 })}>
            Clear All
          </button>
        )}
      </div>

      {/* --- BẢNG DỮ LIỆU --- */}
      <div className="flex-1 min-h-0 rounded-lg border border-zinc-800 flex flex-col bg-zinc-950/30 overflow-hidden">
        <div className="flex-1 overflow-auto custom-scrollbar relative">
          
          {/* Header (Sticky) - Đã áp dụng gridLayoutClass */}
          <div className={`sticky top-0 z-10 bg-zinc-900/95 backdrop-blur border-b border-zinc-800 px-4 py-2 grid ${gridLayoutClass} gap-3 text-xs font-bold text-zinc-500 uppercase tracking-wider`}>
            <div>Time</div>
            <div>User</div>
            <div>Specific Rule</div>
            <div>Query / Content</div>
            <div>Reason</div>
            {/* Chỉ render cột Score Header nếu showScoreCol = true */}
            {showScoreCol && <div className="text-center">Score</div>}
          </div>

          {/* Body */}
          {rows.length === 0 && (
             <div className="text-center py-20 text-zinc-500 italic">No anomalies found matching your filters.</div>
          )}
          
          <div className="pb-2">
            {rows.map((r) => (
              <div
                key={r.id}
                // Grid cols phải khớp với Header
                className={`px-4 py-3 grid ${gridLayoutClass} gap-3 border-b border-zinc-800/50 hover:bg-zinc-900/80 cursor-pointer items-start transition-colors group`}
                onClick={() => setSelectedLog(r)}
              >
                {/* 1. Time */}
                <div className="flex items-center gap-2 truncate text-xs text-zinc-300 font-mono mt-0.5">
                   {r.source === 'aggregate' ? <Layers className="w-3.5 h-3.5 text-orange-500 shrink-0"/> : <Activity className="w-3.5 h-3.5 text-blue-500 shrink-0"/>}
                   {new Date(r.timestamp).toLocaleString()}
                </div>

                {/* 2. User */}
                <div className="truncate text-sm font-medium text-zinc-200" title={r.user}>{r.user ?? '—'}</div>

                {/* 3. Rule */}
                <div>
                  <span className="px-2 py-0.5 text-[13px] rounded-sm border truncate block w-fit max-w-full bg-zinc-800/80 border-zinc-700 text-zinc-300 font-mono" title={r.anomaly_type}>
                    {r.anomaly_type}
                  </span>
                </div>

                {/* 4. Content */}
                <div className="h-14 overflow-hidden text-xs text-zinc-400 font-mono bg-zinc-950/50 p-1.5 rounded border border-zinc-800/50 group-hover:border-zinc-700 transition-colors">
                  {renderContent(r)}
                </div>

                {/* 5. Reason */}
                <div className="text-xs text-zinc-500 h-14 overflow-hidden line-clamp-3 leading-relaxed">
                  {r.reason || '—'}
                </div>

                {/* 6. Score (Chỉ render nếu showScoreCol = true) */}
                {showScoreCol && (
                  <div className="text-center font-bold text-sm mt-0.5">
                      {renderScoreCell(r)}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* --- PHÂN TRANG --- */}
      <div className="flex items-center gap-2 shrink-0 justify-end py-1">
        <button className="px-3 py-1.5 rounded-md border border-zinc-700 bg-zinc-900 text-sm disabled:opacity-50 hover:bg-zinc-800 transition-colors" disabled={filters.pageIndex === 0} onClick={() => setFilters(p => ({ ...p, pageIndex: Math.max(0, p.pageIndex - 1) }))}>Prev</button>
        <span className="text-sm text-zinc-400 font-mono">Page {filters.pageIndex + 1} / {totalPages}</span>
        <button className="px-3 py-1.5 rounded-md border border-zinc-700 bg-zinc-900 text-sm disabled:opacity-50 hover:bg-zinc-800 transition-colors" disabled={(filters.pageIndex + 1) >= totalPages} onClick={() => setFilters(p => ({ ...p, pageIndex: p.pageIndex + 1 }))}>Next</button>
      </div>

      {/* --- MODAL --- */}
      <AnomalyDetailModal isOpen={!!selectedLog} onClose={() => setSelectedLog(null)} log={selectedLog} onAnalyze={handleAnalyze} onFeedback={handleFeedback} isAiLoading={analyzeMutation.isPending} isFeedbackLoading={feedbackMutation.isPending}/>
    </div>
  )
}

function KpiCard({ title, value, active, onClick, icon: Icon, color, borderColor = "hover:border-zinc-600" }) {
  return (
    <button onClick={onClick} className={`relative overflow-hidden text-left p-3 rounded-xl border transition-all h-20 flex flex-col justify-between group ${active ? 'border-primary-500 bg-zinc-900/90 shadow-[0_0_15px_rgba(59,130,246,0.15)]' : `border-neutral-800 bg-neutral-900/50 ${borderColor} hover:bg-zinc-900`}`}>
      <div className="flex justify-between items-start z-10">
         <span className={`text-[10px] tracking-widest uppercase font-bold ${active ? 'text-primary-300' : 'text-neutral-500 group-hover:text-neutral-400'}`}>{title}</span>
         {Icon && <Icon className={`w-4 h-4 ${color} opacity-70 group-hover:opacity-100 transition-opacity`} />}
      </div>
      <div className={`text-2xl font-bold z-10 ${active ? 'text-white' : 'text-zinc-300 group-hover:text-white'}`}>{value}</div>
      {active && <div className="absolute inset-0 bg-gradient-to-br from-primary-500/10 to-transparent pointer-events-none"/>}
    </button>
  )
}