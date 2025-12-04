// uba_frontend/src/components/AnomalyDetailModal.jsx
import React from 'react';
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from "@/components/ui/dialog";
import { Bot, Check, X } from 'lucide-react';

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

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-3xl bg-zinc-950 border-border">
        <DialogHeader>
          <DialogTitle className="text-2xl">Log Details</DialogTitle>
          <DialogDescription>
            User: <span className="text-primary-500">{log.user}</span> @ {log.client_ip}
          </DialogDescription>
        </DialogHeader>

        {/* Chi tiết Log */}
        <div className="grid grid-cols-2 gap-4 py-4 max-h-[60vh] overflow-y-auto">
          <div>
            <h4 className="font-semibold text-muted-foreground">TIMESTAMP</h4>
            <p>{new Date(log.timestamp).toLocaleString()}</p>
          </div>
          <div>
            <h4 className="font-semibold text-muted-foreground">DATABASE</h4>
            <p>{log.database}</p>
          </div>
          <div className="col-span-2">
            <h4 className="font-semibold text-muted-foreground">FULL QUERY</h4>
            <code className="block bg-zinc-900 p-2 rounded-md w-full whitespace-pre-wrap">{log.query}</code>
          </div>
          {log.reason && (
            <div className="col-span-2">
              <h4 className="font-semibold text-muted-foreground">REASON</h4>
              <p className="text-red-400">{log.reason}</p>
            </div>
          )}
          
          {/* Vùng phân tích AI */}
          <div className="col-span-2 mt-4">
            <h4 className="font-semibold text-muted-foreground">AI ANALYSIS</h4>
            <div className="bg-zinc-900 p-4 rounded-md min-h-[100px]">
              {isAiLoading && "Đang phân tích, xin chờ..."}
              {!isAiLoading && log.aiAnalysis && (
                <pre className="text-sm whitespace-pre-wrap break-words">
                  {JSON.stringify(log.aiAnalysis, null, 2)}
                </pre>
              )}
               {!isAiLoading && !log.aiAnalysis && (
                <span className="text-muted-foreground">Nhấn nút "Phân tích với AI" để xem chi tiết.</span>
               )}
            </div>
          </div>
        </div>

        <DialogFooter className="sm:justify-between">
          {/* Nút Phân tích AI */}
          <Button 
            onClick={onAnalyze} 
            disabled={isAiLoading || isFeedbackLoading} 
            className="bg-primary-600 hover:bg-primary-700 text-white"
          >
            <Bot className="h-4 w-4 mr-2" />
            {isAiLoading ? "Đang chạy AI..." : "Phân tích với AI"}
          </Button>

          {/* Nút Feedback */}
          <div className="flex space-x-2">
            <Button 
              variant="outline" 
              className="text-green-400 border-green-700 hover:bg-green-900" 
              onClick={() => onFeedback(0)}
              disabled={isAiLoading || isFeedbackLoading}
            >
              <Check className="h-4 w-4 mr-2" />
              Đánh dấu: Bình thường
            </Button>
            <Button 
              variant="outline" 
              className="text-red-400 border-red-700 hover:bg-red-900" 
              onClick={() => onFeedback(1)}
              disabled={isAiLoading || isFeedbackLoading}
            >
              <X className="h-4 w-4 mr-2" />
              Đánh dấu: Bất thường
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

export default AnomalyDetailModal;