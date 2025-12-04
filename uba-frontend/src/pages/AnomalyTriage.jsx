// uba_frontend/src/pages/AnomalyTriage.jsx
import React, { useState } from 'react'
import { useAnomalyKpis, useAnomalyFacets, useAnomalySearch, useAnalyzeMutation, useFeedbackMutation } from '@/api/queries'
import { AnomalyDetailModal } from '@/components/AnomalyDetailModal'

export default function AnomalyTriage() {
  // giữ tất cả filter ở server-side để lọc "toàn bộ", không chỉ trang đang xem
  const [filters, setFilters] = useState({
    pageIndex: 0,
    pageSize: 20,
    search: '',
    user: '',
    anomaly_type: '',
    date_from: '',
    date_to: '',
  })

  const { data: kpis } = useAnomalyKpis()
  const { data: facets } = useAnomalyFacets()
  const { data } = useAnomalySearch(filters)

  const rows = data?.items ?? []
  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / filters.pageSize))
  const users = facets?.users ?? []
  const types = facets?.types ?? []

  const [selectedLog, setSelectedLog] = useState(null)

  const analyzeMutation = useAnalyzeMutation();
  const feedbackMutation = useFeedbackMutation();

  const toggleType = (t) =>
    setFilters(p => ({ ...p, anomaly_type: p.anomaly_type === t ? '' : t, pageIndex: 0 }))

  const scoreOf = (r) => {
    // score có thể từ r.score, r.severity (aggregate), r.details.{score,probability,...}
    const tryPick = (...ks) => {
      for (const k of ks) {
        const v = k.split('.').reduce((acc, key) => (acc && acc[key] != null ? acc[key] : undefined), r)
        if (v != null && !isNaN(Number(v))) return Number(v)
      }
      return null
    }
    let s = tryPick('score', 'severity', 'details.score', 'details.anomaly_score', 'details.probability', 'details.confidence', 'details.risk_score')
    if (s != null && s > 1 && s <= 100) s = s / 100 // chuẩn hóa % -> 0..1 nếu cần
    return s != null ? s.toFixed(2) : '—'
  }

  // 3. Hàm xử lý khi bấm nút "Phân tích AI"
  const handleAnalyze = () => {
    if (!selectedLog) return;
    
    analyzeMutation.mutate(selectedLog, {
      onSuccess: (res) => {
        // Backend trả về: { first_analysis, final_analysis, ... }
        // Ta cập nhật vào state đang mở để Modal hiển thị ngay lập tức
        setSelectedLog(prev => ({
          ...prev,
          // Ưu tiên lấy final_analysis, nếu không có thì lấy toàn bộ res.data
          aiAnalysis: res.data.final_analysis || res.data
        }));
      }
    });
  };

  // 4. Hàm xử lý Feedback (Đánh dấu đúng/sai)
  const handleFeedback = (label) => {
    if (!selectedLog) return;
    feedbackMutation.mutate(
      { label, anomaly_data: selectedLog },
      { onSuccess: () => setSelectedLog(null) } // Đóng modal sau khi feedback xong
    );
  };

  return (
    // min-h-0 để con có thể cuộn trong layout flex (không kéo cả trang)
    <div className="h-full min-h-0 flex flex-col gap-4 overflow-hidden">
      {/* KPI: bấm để lọc theo rule (bấm lần 2 để bỏ lọc) */}
      <div className="grid grid-cols-8 gap-4 shrink-0">
        <KpiCard title="Late-night"        value={kpis?.late_night ?? '—'}       active={filters.anomaly_type==='late_night'} onClick={()=>toggleType('late_night')} />
        <KpiCard title="Large dump"        value={kpis?.large_dump ?? '—'}       active={filters.anomaly_type==='dump'}       onClick={()=>toggleType('dump')} />
        <KpiCard title="Machine Learning"       value={kpis?.multi_table ?? '—'}      active={filters.anomaly_type==='ml'}onClick={()=>toggleType('ml')} />
        <KpiCard title="Sensitive access"  value={kpis?.sensitive_access ?? '—'} active={filters.anomaly_type==='sensitive'}  onClick={()=>toggleType('sensitive')} />
        <KpiCard title="Profile deviation" value={kpis?.profile_deviation ?? '—'}active={filters.anomaly_type==='user_time'}  onClick={()=>toggleType('user_time')} />
        <KpiCard 
            title="SQL Injection" 
            value={kpis?.sqli ?? '—'} 
            active={filters.anomaly_type==='sqli'} 
            onClick={()=>toggleType('sqli')} 
            // (Tuỳ chọn) Thêm màu đỏ cảnh báo nếu muốn
            className={filters.anomaly_type==='sqli' ? "border-red-500 bg-red-900/20" : ""}
        />
        <KpiCard 
            title="Privilege Esc" 
            value={kpis?.privilege ?? '—'} 
            active={filters.anomaly_type==='privilege'} 
            onClick={()=>toggleType('privilege')} 
        />
        <KpiCard title="TOTAL"             value={kpis?.total ?? '—'}            active={!filters.anomaly_type}                onClick={()=>toggleType('')} />
      </div>

      {/* Filters */}
      <div className="flex gap-2 items-center shrink-0">
        <input
          className="bg-zinc-900 border border-zinc-700 rounded-md px-3 py-2 w-80"
          placeholder="Search query / reason…"
          value={filters.search}
          onChange={(e) => setFilters(p => ({ ...p, search: e.target.value, pageIndex: 0 }))}
        />

        <select
          className="bg-zinc-900 border border-zinc-700 rounded-md px-3 py-2"
          value={filters.user}
          onChange={(e) => setFilters(p => ({ ...p, user: e.target.value.trim(), pageIndex: 0 }))}
        >
          <option value="">All users</option>
          {users.map((u) => <option key={u} value={u}>{u}</option>)}
        </select>

        <select
          className="bg-zinc-900 border border-zinc-700 rounded-md px-3 py-2"
          value={filters.anomaly_type}
          onChange={(e) => setFilters(p => ({ ...p, anomaly_type: e.target.value.trim(), pageIndex: 0 }))}
        >
          <option value="">All types</option>
          {types.map((t) => <option key={t} value={t}>{t}</option>)}
        </select>

        {(filters.anomaly_type || filters.user || filters.search) && (
          <button
            className="px-3 py-2 rounded-md border border-zinc-700 bg-zinc-900"
            onClick={() => setFilters({ ...filters, search:'', user:'', anomaly_type:'', pageIndex:0 })}
          >
            Clear
          </button>
        )}
      </div>

      {/* Bảng: header sticky, body cuộn bên trong – không kéo trang */}
      <div className="flex-1 min-h-0 overflow-hidden rounded-lg border border-border">
        {/* Header */}
        <div
          className="
            bg-zinc-900 px-4 py-2 sticky top-0 z-10
            grid grid-cols-[14rem,9rem,8rem,1fr,1fr,6rem]
            text-sm text-muted-foreground
          ">
          <div>Time</div><div>User</div><div>Type</div><div>Query</div><div>Reason</div><div>Score</div>
        </div>

        {/* Body (scroll) */}
        <div className="h-full min-h-0 overflow-auto">
          {rows.map((r) => (
            <div
              key={`${r.id}-${r.timestamp}`}
              className="
                px-4 py-3 grid grid-cols-[14rem,9rem,8rem,1fr,1fr,6rem] gap-3
                border-t border-zinc-800 hover:bg-zinc-900 cursor-pointer
              "
              onClick={() => setSelectedLog(r)}
            >
              <div className="truncate">{new Date(r.timestamp).toLocaleString()}</div>
              <div className="truncate">{r.user ?? '—'}</div>

              <div>
                <span className="px-2 py-0.5 text-xs rounded-full bg-zinc-800 border border-zinc-700">
                  {r.anomaly_type}
                </span>
              </div>

              {/* Query & Reason: có chiều cao cố định và cuộn NỘI BỘ */}
              <div className="bg-zinc-950/40 rounded-md px-2 py-1 text-sm whitespace-pre-wrap h-16 overflow-y-auto">
                {r.query || '—'}
              </div>
              <div className="text-sm text-zinc-300 whitespace-pre-wrap h-16 overflow-y-auto">
                {r.reason || '—'}
              </div>

              <div className="text-center">{scoreOf(r)}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Pagination cố định dưới */}
      <div className="flex items-center gap-2 shrink-0">
        <button
          className="px-3 py-2 rounded-md border border-zinc-700 bg-zinc-900"
          disabled={filters.pageIndex === 0}
          onClick={() => setFilters(p => ({ ...p, pageIndex: Math.max(0, p.pageIndex - 1) }))}
        >
          Prev
        </button>
        <span>Page {filters.pageIndex + 1} / {totalPages}</span>
        <button
          className="px-3 py-2 rounded-md border border-zinc-700 bg-zinc-900"
          disabled={(filters.pageIndex + 1) >= totalPages}
          onClick={() => setFilters(p => ({ ...p, pageIndex: p.pageIndex + 1 }))}
        >
          Next
        </button>
      </div>

      <AnomalyDetailModal
        isOpen={!!selectedLog}
        onClose={() => setSelectedLog(null)}
        log={selectedLog}
        onAnalyze={handleAnalyze}       
        onFeedback={handleFeedback}
        isAiLoading={analyzeMutation.isPending}
        isFeedbackLoading={feedbackMutation.isPending}
      />
    </div>
  )
}

function KpiCard({ title, value, active, onClick }) {
  return (
    <button
      onClick={onClick}
      className={`text-left p-4 rounded-xl border transition
        ${active ? 'border-primary-500 bg-zinc-900/60' : 'border-neutral-800 bg-neutral-900 hover:bg-zinc-900/40'}
      `}
    >
      <div className="text-xs tracking-widest text-neutral-400">{title}</div>
      <div className="text-3xl font-bold mt-2">{value}</div>
    </button>
  )
}
