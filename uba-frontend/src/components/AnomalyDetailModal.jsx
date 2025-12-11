// uba_frontend/src/components/AnomalyDetailModal.jsx
import React from 'react';
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from "@/components/ui/dialog";
import { 
  Bot, Check, X, Database, Clock, List, FileText, Layers, 
  ShieldAlert, ShieldCheck, Activity, Lightbulb, Zap 
} from 'lucide-react';
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";

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

  const isAggregate = log.source === 'aggregate';

  // --- Helper để lấy màu sắc dựa trên mức độ rủi ro ---
  const getRiskColor = (level) => {
    if (!level) return "bg-zinc-800 text-zinc-400 border-zinc-700";
    const l = level.toString().toLowerCase();
    if (l.includes('critical') || l.includes('high')) return "bg-red-950/40 text-red-400 border-red-900";
    if (l.includes('medium')) return "bg-yellow-950/40 text-yellow-400 border-yellow-900";
    if (l.includes('low') || l.includes('none')) return "bg-green-950/40 text-green-400 border-green-900";
    return "bg-zinc-800 text-zinc-400 border-zinc-700";
  };

  // --- Helper để render kết quả AI đẹp mắt ---
  const renderAiResult = () => {
    if (isAiLoading) {
      return (
        <div className="flex flex-col items-center justify-center h-40 space-y-3 animate-pulse">
            <Bot className="w-8 h-8 text-purple-500" />
            <div className="text-sm text-zinc-400">DeepSeek is analyzing security patterns...</div>
        </div>
      );
    }

    if (!log.aiAnalysis) {
      return (
        <div className="flex flex-col items-center justify-center h-32 text-zinc-500 border border-dashed border-zinc-800 rounded-lg">
           <Bot className="w-6 h-6 mb-2 opacity-50"/>
           <span className="text-sm italic">Click "Analyze with AI" to generate insights.</span>
        </div>
      );
    }

    // Xử lý lấy dữ liệu final_analysis từ backend
    let data = log.aiAnalysis;
    if (data.final_analysis) data = data.final_analysis;

    // Trường hợp data vẫn là string (lỗi parse)
    if (typeof data === 'string') {
        return <div className="p-4 bg-zinc-950 border border-zinc-800 rounded text-xs font-mono whitespace-pre-wrap">{data}</div>;
    }

    return (
      <div className="space-y-4 animate-in fade-in zoom-in-95 duration-300">
        {/* 1. Header Cards: Risk Level & Confidence */}
        <div className="grid grid-cols-2 gap-3">
            <div className={`p-3 rounded-lg border flex items-center justify-between ${getRiskColor(data.security_risk_level)}`}>
                <div className="flex items-center gap-2">
                    <ShieldAlert className="w-4 h-4" />
                    <span className="text-xs font-bold uppercase tracking-wider">Risk Level</span>
                </div>
                <span className="text-sm font-bold capitalize">{data.security_risk_level || 'Unknown'}</span>
            </div>

            <div className="p-3 rounded-lg border border-purple-900/30 bg-purple-900/10 text-purple-300 flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <Zap className="w-4 h-4" />
                    <span className="text-xs font-bold uppercase tracking-wider">Confidence</span>
                </div>
                <span className="text-sm font-bold">{(data.confidence_score * 100).toFixed(0)}%</span>
            </div>
        </div>

        {/* 2. Summary Section */}
        <div className="bg-zinc-900/50 p-3 rounded-lg border border-zinc-800">
            <h4 className="text-xs font-bold text-zinc-400 mb-2 uppercase flex items-center gap-2">
                <Activity className="w-3 h-3"/> Executive Summary
            </h4>
            <p className="text-sm text-zinc-200 leading-relaxed">
                {data.summary || data.session_summary || "No summary provided."}
            </p>
        </div>

        {/* 3. Detailed Analysis */}
        <div>
            <h4 className="text-xs font-bold text-zinc-400 mb-2 uppercase flex items-center gap-2">
                <FileText className="w-3 h-3"/> Technical Analysis
            </h4>
            <div className="text-sm text-zinc-300 leading-relaxed bg-zinc-950 p-3 rounded border border-zinc-800/50">
                {data.detailed_analysis}
            </div>
        </div>

        {/* 4. Recommendation (Highlight) */}
        <div className="bg-gradient-to-r from-blue-950/30 to-zinc-900 p-3 rounded-lg border-l-4 border-l-blue-500 border-y border-r border-zinc-800">
            <h4 className="text-xs font-bold text-blue-400 mb-2 uppercase flex items-center gap-2">
                <Lightbulb className="w-3 h-3"/> Recommendation
            </h4>
            <p className="text-sm text-zinc-200 italic">
                {data.recommendation}
            </p>
        </div>
        
        {/* 5. Footer Info */}
        <div className="flex justify-between items-center text-[10px] text-zinc-500 pt-2 border-t border-zinc-800/50">
             <span>Model: Ollama (DeepSeek-R1)</span>
             <span>Type: {data.anomaly_type || data.behavior_type || "Generic"}</span>
        </div>
      </div>
    );
  };

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-4xl bg-zinc-950 border-zinc-800 h-[85vh] flex flex-col p-0 overflow-hidden">
        
        {/* HEADER */}
        <div className="px-6 py-4 border-b border-zinc-800 bg-zinc-900/50">
          <DialogHeader>
            <div className="flex justify-between items-start">
              <div>
                <DialogTitle className="text-xl flex items-center gap-2">
                  {isAggregate ? (
                      <><Layers className="w-5 h-5 text-orange-500"/> Session Detail</>
                  ) : (
                      <><Database className="w-5 h-5 text-blue-500"/> Event Detail</>
                  )}
                  <Badge variant="outline" className="ml-2 bg-zinc-800 text-zinc-300 border-zinc-700 font-normal">
                    {log.anomaly_type}
                  </Badge>
                </DialogTitle>
                <DialogDescription className="mt-1 flex items-center gap-3">
                  <span className="text-primary-400 font-semibold">{log.user}</span>
                  <span className="text-zinc-600">|</span>
                  <span className="font-mono text-xs text-zinc-400">{log.client_ip || 'Internal IP'}</span>
                  <span className="text-zinc-600">|</span>
                  <span className="text-zinc-400 text-xs">{new Date(log.timestamp).toLocaleString()}</span>
                </DialogDescription>
              </div>
              <div className="text-right">
                <Badge variant="secondary" className="font-mono text-[10px] bg-zinc-900 text-zinc-500">
                    ID: {log.id}
                </Badge>
              </div>
            </div>
          </DialogHeader>
        </div>

        {/* BODY - Split View (Left: Raw Data, Right: AI Analysis) */}
        <div className="flex-1 min-h-0 flex divide-x divide-zinc-800">
            
            {/* LEFT COLUMN: RAW LOG DATA */}
            <ScrollArea className="w-[45%] p-6">
                <div className="space-y-6">
                    {/* Reason */}
                    <div>
                        <h4 className="text-xs font-bold text-red-400 mb-2 uppercase tracking-wider">Detection Trigger</h4>
                        <div className="bg-red-950/10 border border-red-900/30 p-3 rounded text-sm text-red-200/80">
                            {log.reason}
                        </div>
                    </div>

                    {/* Content (Query or Session Info) */}
                    {isAggregate ? (
                        <div className="space-y-4">
                            <div className="grid grid-cols-2 gap-3">
                                <InfoBox label="Duration" value={`${log.details?.duration_sec?.toFixed(1) || 0}s`} icon={Clock} />
                                <InfoBox label="Queries" value={log.details?.query_count || 0} icon={List} />
                            </div>
                            <div>
                                <h4 className="text-xs font-bold text-zinc-500 mb-2 uppercase">Tables Accessed</h4>
                                <div className="flex flex-wrap gap-2">
                                {log.details?.tables?.map((t, i) => (
                                    <Badge key={i} variant="secondary" className="bg-zinc-800 text-zinc-300 hover:bg-zinc-700">
                                    {t}
                                    </Badge>
                                ))}
                                </div>
                            </div>
                        </div>
                    ) : (
                        <div>
                            <h4 className="text-xs font-bold text-zinc-500 mb-2 uppercase">Executed Query</h4>
                            <div className="bg-zinc-900 p-3 rounded border border-zinc-800 overflow-x-auto">
                                <code className="text-xs font-mono text-blue-300 break-all whitespace-pre-wrap">
                                    {log.query}
                                </code>
                            </div>
                            <div className="mt-2 text-xs text-zinc-500 flex justify-between">
                                <span>Database: {log.database || 'N/A'}</span>
                            </div>
                        </div>
                    )}
                </div>
            </ScrollArea>

            {/* RIGHT COLUMN: AI ANALYSIS */}
            <ScrollArea className="flex-1 p-6 bg-zinc-900/20">
                <div className="flex items-center justify-between mb-4">
                     <h4 className="text-sm font-bold text-purple-400 uppercase flex items-center gap-2">
                        <Bot className="w-4 h-4" /> AI Investigation Report
                    </h4>
                    {log.aiAnalysis && !isAiLoading && (
                        <Badge variant="outline" className="text-[10px] border-purple-900 text-purple-500 bg-purple-950/20">
                            Verified by LLM
                        </Badge>
                    )}
                </div>
                
                {renderAiResult()}

            </ScrollArea>
        </div>

        {/* FOOTER */}
        <div className="p-4 border-t border-zinc-800 bg-zinc-900/50 flex justify-between items-center shrink-0">
          <Button 
            onClick={onAnalyze} 
            disabled={isAiLoading || isFeedbackLoading} 
            className="bg-purple-600 hover:bg-purple-700 text-white min-w-[140px]"
          >
            <Bot className="h-4 w-4 mr-2" />
            {isAiLoading ? "Analyzing..." : (log.aiAnalysis ? "Re-Analyze" : "Analyze with AI")}
          </Button>

          <div className="flex gap-2">
            <Button 
              variant="outline" 
              className="border-zinc-700 hover:bg-green-950/30 hover:text-green-400 hover:border-green-800 transition-colors"
              onClick={() => onFeedback(0)}
              disabled={isAiLoading || isFeedbackLoading}
            >
              <Check className="h-4 w-4 mr-2" /> Mark Safe
            </Button>
            <Button 
              variant="outline" 
              className="border-zinc-700 hover:bg-red-950/30 hover:text-red-400 hover:border-red-800 transition-colors"
              onClick={() => onFeedback(1)}
              disabled={isAiLoading || isFeedbackLoading}
            >
              <X className="h-4 w-4 mr-2" /> Confirm Threat
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
};

const InfoBox = ({ label, value, icon: Icon }) => (
  <div className="bg-zinc-900 p-2 rounded border border-zinc-800 flex items-center gap-3">
    <div className="p-1.5 bg-zinc-800 rounded text-zinc-400">
      <Icon className="w-3.5 h-3.5" />
    </div>
    <div>
      <div className="text-[10px] text-zinc-500 uppercase font-bold">{label}</div>
      <div className="text-sm font-semibold text-zinc-200">{value}</div>
    </div>
  </div>
);

export default AnomalyDetailModal;