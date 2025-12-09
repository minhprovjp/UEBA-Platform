// uba_frontend/src/components/AnomalyDetailModal.jsx
import React from 'react';
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from "@/components/ui/dialog";
// [NEW] Import thêm icon cần thiết
import { Bot, Check, X, Database, Clock, List, FileText, Layers } from 'lucide-react';
import { Badge } from "@/components/ui/badge";

export const AnomalyDetailModal = ({ 
  isOpen, 
  onClose, 
  log, 
  onAnalyze, 
  onFeedback, 
  isAiLoading, 
  isFeedbackLoading 
}) => {
  if (!log) return null;

  // [NEW] Kiểm tra xem đây là log đơn (event) hay phiên gộp (aggregate)
  const isAggregate = log.source === 'aggregate';

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-4xl bg-zinc-950 border-zinc-800 max-h-[90vh] flex flex-col">
        <DialogHeader>
          <div className="flex justify-between items-start mr-4">
            <div>
              {/* [MODIFIED] Tiêu đề động dựa trên loại log */}
              <DialogTitle className="text-2xl flex items-center gap-2">
                {isAggregate ? (
                    <><Layers className="w-6 h-6 text-orange-500"/> Session Anomaly Detail</>
                ) : (
                    <><FileText className="w-6 h-6 text-blue-500"/> Log Event Detail</>
                )}
                
                <Badge variant="outline" className={isAggregate ? "text-orange-400 border-orange-800 ml-2" : "text-blue-400 border-blue-800 ml-2"}>
                  {log.anomaly_type}
                </Badge>
              </DialogTitle>
              <DialogDescription className="mt-1 text-base">
                User: <span className="text-primary-500 font-bold">{log.user}</span> 
                {log.client_ip && <span className="text-zinc-400"> @ {log.client_ip}</span>}
              </DialogDescription>
            </div>
            <div className="text-right text-xs text-muted-foreground">
              <p>ID: <span className="font-mono text-zinc-500">{log.id}</span></p>
              <p>{new Date(log.timestamp).toLocaleString()}</p>
            </div>
          </div>
        </DialogHeader>

        {/* --- NỘI DUNG CHÍNH (Scrollable) --- */}
        <div className="flex-1 overflow-y-auto pr-2 py-2 space-y-6 custom-scrollbar">
          
          {/* 1. REASON (Luôn hiển thị) */}
          <div className="bg-red-900/10 border border-red-900/30 p-3 rounded-lg">
            <h4 className="text-xs font-bold text-red-400 mb-1 uppercase tracking-wider">Detection Reason</h4>
            <p className="text-sm text-red-200">{log.reason}</p>
          </div>

          {/* 2. CHI TIẾT DỮ LIỆU (Phân nhánh Event vs Aggregate) */}
          {isAggregate ? (
            /* --- [NEW] SESSION VIEW (Hiển thị cho Multi-table) --- */
            <div className="space-y-4">
              {/* Thống kê Session */}
              <div className="grid grid-cols-3 gap-3">
                <InfoBox label="Duration" value={`${log.details?.duration_sec?.toFixed(1) || 0}s`} icon={Clock} />
                <InfoBox label="Distinct Tables" value={log.details?.tables?.length || 0} icon={Database} />
                <InfoBox label="Total Queries" value={log.details?.query_count || 0} icon={List} />
              </div>

              {/* Danh sách bảng bị truy cập */}
              <div>
                <h4 className="text-xs font-bold text-zinc-500 mb-2 uppercase">Tables Accessed</h4>
                <div className="flex flex-wrap gap-2">
                  {log.details?.tables?.map((t, i) => (
                    <Badge key={i} variant="secondary" className="bg-zinc-800 text-zinc-300 hover:bg-zinc-700">
                      {t}
                    </Badge>
                  ))}
                  {(!log.details?.tables || log.details.tables.length === 0) && <span className="text-zinc-600 text-sm italic">No specific tables recorded</span>}
                </div>
              </div>

              {/* Danh sách Query Bằng chứng (Evidence) */}
              <div>
                <h4 className="text-xs font-bold text-zinc-500 mb-2 uppercase">Evidence Queries ({log.details?.evidence_queries?.length || 0})</h4>
                <div className="bg-zinc-900 rounded-md border border-zinc-800 max-h-60 overflow-y-auto custom-scrollbar">
                  {log.details?.evidence_queries?.map((q, idx) => (
                    <div key={idx} className="p-2 border-b border-zinc-800 last:border-0 hover:bg-zinc-800/50">
                      <div className="text-[10px] text-zinc-500 mb-0.5 font-mono">
                        {q.timestamp ? new Date(q.timestamp).toLocaleTimeString() : '-'}
                      </div>
                      <code className="text-xs font-mono text-zinc-300 block whitespace-pre-wrap break-all">{q.query}</code>
                    </div>
                  ))}
                  {(!log.details?.evidence_queries || log.details.evidence_queries.length === 0) && (
                     <div className="p-4 text-center text-zinc-600 text-sm">No raw queries available for this session.</div>
                  )}
                </div>
              </div>
            </div>
          ) : (
            /* --- [MODIFIED] EVENT VIEW (Hiển thị cho Log đơn lẻ cũ) --- */
            <div className="space-y-4">
              <div>
                 <h4 className="text-xs font-bold text-zinc-500 mb-1 uppercase">Database</h4>
                 <p className="text-sm font-mono text-zinc-300 bg-zinc-900 px-2 py-1 rounded inline-block">
                    {log.database || 'N/A'}
                 </p>
              </div>
              <div>
                <h4 className="text-xs font-bold text-zinc-500 mb-1 uppercase">Full Query</h4>
                <div className="bg-zinc-900 p-3 rounded-md border border-zinc-800 max-h-60 overflow-y-auto custom-scrollbar">
                  <code className="text-sm font-mono text-zinc-300 whitespace-pre-wrap break-all">{log.query}</code>
                </div>
              </div>
            </div>
          )}

          {/* 3. AI ANALYSIS (Giữ nguyên) */}
          <div>
            <h4 className="text-xs font-bold text-purple-400 mb-2 uppercase flex items-center gap-2">
              <Bot className="w-3 h-3" /> AI Investigation
            </h4>
            <div className="bg-zinc-900 border border-zinc-800 p-4 rounded-md min-h-[100px] text-sm">
              {isAiLoading && <span className="animate-pulse text-zinc-400 flex items-center gap-2">Analyzing behavior patterns...</span>}
              {!isAiLoading && log.aiAnalysis && (
                <div className="prose prose-invert max-w-none text-sm whitespace-pre-wrap font-sans text-zinc-300">
                    {/* Xử lý hiển thị JSON string hoặc Object */}
                    {typeof log.aiAnalysis === 'string' ? log.aiAnalysis : JSON.stringify(log.aiAnalysis, null, 2)}
                </div>
              )}
               {!isAiLoading && !log.aiAnalysis && (
                <span className="text-zinc-600 italic">Click "Analyze with AI" to generate insight for this specific {isAggregate ? 'session' : 'event'}.</span>
               )}
            </div>
          </div>
        </div>

        {/* --- FOOTER --- */}
        <DialogFooter className="mt-2 pt-2 border-t border-zinc-800 sm:justify-between">
          <Button 
            onClick={onAnalyze} 
            disabled={isAiLoading || isFeedbackLoading} 
            className="bg-purple-600 hover:bg-purple-700 text-white"
          >
            <Bot className="h-4 w-4 mr-2" />
            {isAiLoading ? "Processing..." : "Analyze with AI"}
          </Button>

          <div className="flex gap-2">
            <Button 
              variant="outline" 
              className="border-green-900 text-green-500 hover:bg-green-900/20" 
              onClick={() => onFeedback(0)}
              disabled={isAiLoading || isFeedbackLoading}
            >
              <Check className="h-4 w-4 mr-2" /> False Positive
            </Button>
            <Button 
              variant="outline" 
              className="border-red-900 text-red-500 hover:bg-red-900/20" 
              onClick={() => onFeedback(1)}
              disabled={isAiLoading || isFeedbackLoading}
            >
              <X className="h-4 w-4 mr-2" /> Confirm Threat
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

// Component phụ để hiển thị thông tin thống kê
const InfoBox = ({ label, value, icon: Icon }) => (
  <div className="bg-zinc-900 p-2 rounded border border-zinc-800 flex items-center gap-3">
    <div className="p-2 bg-zinc-800 rounded text-zinc-400">
      <Icon className="w-4 h-4" />
    </div>
    <div>
      <div className="text-[10px] text-zinc-500 uppercase font-bold">{label}</div>
      <div className="text-sm font-semibold text-zinc-200">{value}</div>
    </div>
  </div>
);

export default AnomalyDetailModal;