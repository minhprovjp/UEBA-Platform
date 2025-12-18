// uba_frontend/src/pages/LogExplorer.jsx
import React, { useState, useMemo } from 'react';
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Search, Filter, ChevronLeft, ChevronRight, Copy, Database, Terminal, AlertCircle, CheckCircle } from 'lucide-react';
import { useLogs, useAnalyzeMutation, useFeedbackMutation } from '@/api/queries';
import { AnomalyDetailModal } from '@/components/AnomalyDetailModal';
import { Toaster, toast } from 'sonner';
import { useTranslation } from 'react-i18next';

const PAGE_SIZE = 20;

export default function LogExplorer() {
  const { t } = useTranslation();
  
  const [filters, setFilters] = useState({
    search: '',
    user: '', 
    date_from: '',
    date_to: '',
  });
  
  const [showAnomaliesOnly, setShowAnomaliesOnly] = useState(false);

  const [pagination, setPagination] = useState({
    pageIndex: 0, 
    pageSize: PAGE_SIZE,
  });

  const { data, isLoading, isError, error } = useLogs({
    ...filters,
    ...pagination,
  });

  const logs = data?.logs || [];
  const hasMore = data?.hasMore || false;
  
  const displayedLogs = useMemo(() => {
    if (!showAnomaliesOnly) return logs;
    return logs.filter(l => l.is_anomaly || l.ml_anomaly_score > 0.5);
  }, [logs, showAnomaliesOnly]);

  const [selectedLog, setSelectedLog] = useState(null); 

  const analyzeMutation = useAnalyzeMutation();
  const feedbackMutation = useFeedbackMutation();

  const handleCopyQuery = (e, query) => {
    e.stopPropagation();
    navigator.clipboard.writeText(query);
    toast.success("Copied query to clipboard");
  };

  const handleAnalyze = () => {
    analyzeMutation.mutate(selectedLog, {
      onSuccess: (data) => {
        const res = data.data || data;
        setSelectedLog(prev => ({
          ...prev, 
          aiAnalysis: res.final_analysis || res.first_analysis 
        }));
      }
    });
  };

  const handleFeedback = (label) => {
    feedbackMutation.mutate(
      { label, anomaly_data: selectedLog },
      { onSuccess: () => setSelectedLog(null) } 
    );
  };

  return (
    <>
      <Toaster position="top-right" theme="dark" />
      <div className="h-full flex flex-col gap-4 overflow-hidden pr-2">
        
        {/* HEADER */}
        <header className="shrink-0 pb-4 border-b border-zinc-800">
          <h2 className="text-lg font-bold tracking-tight text-white flex items-center gap-2">
            <Terminal className="w-5 h-5 text-primary-500"/> {t('log_explorer.title')}
          </h2>
          <p className="text-zinc-400 text-xs mt-1">
            {t('log_explorer.subtitle')}
          </p>
        </header>

        {/* TOOLBAR */}
        <div className="flex items-center justify-between gap-3 bg-zinc-900/50 p-3 rounded-lg border border-zinc-800 shrink-0">
          {/* Left: Search & Inputs */}
          <div className="flex items-center gap-2 flex-1">
            <div className="relative w-64">
                <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-zinc-500" />
                <Input 
                    placeholder={t('log_explorer.search_placeholder')} 
                    className="pl-9 bg-zinc-950 border-zinc-700 h-9 text-sm"
                    value={filters.search}
                    onChange={(e) => setFilters(prev => ({...prev, search: e.target.value, pageIndex: 0}))}
                />
            </div>
            
            <Input 
                placeholder={t('log_explorer.filter_user')} 
                className="w-40 bg-zinc-950 border-zinc-700 h-9 text-sm"
                value={filters.user || ''}
                onChange={(e) => setFilters(prev => ({...prev, user: e.target.value, pageIndex: 0}))}
            />

            <Input 
                type="datetime-local"
                className="w-48 bg-zinc-950 border-zinc-700 h-9 text-sm text-zinc-400"
                value={filters.date_from || ''}
                onChange={(e) => setFilters(prev => ({...prev, date_from: e.target.value, pageIndex: 0}))}
            />
          </div>

          {/* Right: Toggles */}
          <div className="flex items-center gap-4 border-l border-zinc-700 pl-4">
             <div className="flex items-center space-x-2">
                <Switch 
                    id="anomaly-mode" 
                    checked={showAnomaliesOnly}
                    onCheckedChange={setShowAnomaliesOnly}
                />
                <Label htmlFor="anomaly-mode" className={`text-xs font-medium ${showAnomaliesOnly ? 'text-red-400' : 'text-zinc-400'}`}>
                    {t('log_explorer.anomalies_only')}
                </Label>
            </div>
            <Button variant="outline" size="sm" className="h-9 bg-zinc-800 border-zinc-700 hover:bg-zinc-700">
                <Filter className="h-3.5 w-3.5 mr-2" />
                {t('log_explorer.more_filters')}
            </Button>
          </div>
        </div>

        {/* TABLE */}
        <div className="flex-1 overflow-auto rounded-md border border-zinc-800 bg-zinc-950/30 custom-scrollbar">
          <Table>
            <TableHeader className="bg-zinc-900 sticky top-0 z-10">
              <TableRow className="hover:bg-zinc-900 border-zinc-800">
                <TableHead className="w-[180px]">{t('log_explorer.table.timestamp')}</TableHead>
                <TableHead className="w-[150px]">{t('log_explorer.table.identity')}</TableHead>
                <TableHead className="w-[120px]">{t('log_explorer.table.database')}</TableHead>
                <TableHead>{t('log_explorer.table.snapshot')}</TableHead>
                <TableHead className="w-[100px] text-center">{t('log_explorer.table.status')}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading && <TableRow><TableCell colSpan={5} className="text-center py-10 text-zinc-500">{t('common.loading')}</TableCell></TableRow>}
              {isError && <TableRow><TableCell colSpan={5} className="text-center py-10 text-red-500">{error.message}</TableCell></TableRow>}
              
              {!isLoading && displayedLogs.length === 0 && (
                 <TableRow><TableCell colSpan={5} className="text-center py-10 text-zinc-500">{t('log_explorer.no_data')}</TableCell></TableRow>
              )}

              {!isLoading && displayedLogs.map((log) => {
                const isAnomaly = log.is_anomaly || log.ml_anomaly_score > 0.5;
                const rowClass = isAnomaly 
                    ? "bg-red-950/10 hover:bg-red-950/20 border-l-2 border-l-red-500" 
                    : "hover:bg-zinc-900 border-l-2 border-l-transparent";

                return (
                  <TableRow key={log.id} onClick={() => setSelectedLog(log)} className={`cursor-pointer transition-colors border-b border-zinc-800/50 ${rowClass}`}>
                    
                    <TableCell className="font-mono text-xs text-zinc-400">
                        {new Date(log.timestamp).toLocaleString()}
                    </TableCell>
                    
                    <TableCell>
                        <div className="flex flex-col">
                            <span className="font-medium text-zinc-200 text-xs">{log.user}</span>
                            <span className="text-[10px] text-zinc-500">{log.client_ip}</span>
                        </div>
                    </TableCell>
                    
                    <TableCell>
                         <div className="flex items-center gap-1.5">
                            <Database className="w-3 h-3 text-zinc-600"/>
                            <span className="text-xs text-zinc-300">{log.database || 'default'}</span>
                         </div>
                    </TableCell>
                    
                    <TableCell>
                        <div className="group flex items-center justify-between gap-2">
                             <code className="text-[15px] text-zinc-400 font-mono truncate max-w-[400px] bg-zinc-900/50 px-1.5 py-0.5 rounded">
                                {log.query}
                             </code>
                             <Button 
                                variant="ghost" size="icon" className="h-6 w-6 opacity-0 group-hover:opacity-100 transition-opacity"
                                onClick={(e) => handleCopyQuery(e, log.query)}
                             >
                                <Copy className="w-3 h-3 text-zinc-500 hover:text-white"/>
                             </Button>
                        </div>
                    </TableCell>
                    
                    <TableCell className="text-center">
                      {isAnomaly ? (
                        <Badge variant="outline" className="bg-red-950/30 text-red-400 border-red-900 text-[14px] px-2 py-0.5 whitespace-nowrap">
                            <AlertCircle className="w-3 h-3 mr-1"/>
                            {log.specific_rule || log.behavior_group || log.analysis_type || t('common.anomaly')}
                        </Badge>
                      ) : (
                        <Badge variant="outline" className="bg-green-950/30 text-green-500 border-green-900 text-[10px] px-2 py-0.5 whitespace-nowrap">
                            <CheckCircle className="w-3 h-3 mr-1"/>
                            {t('common.normal')}
                        </Badge>
                      )}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>

        {/* PAGINATION */}
        <div className="flex items-center justify-between shrink-0 py-2 border-t border-zinc-800">
          <div className="text-xs text-zinc-500">
             {t('log_explorer.showing')} {displayedLogs.length} {t('log_explorer.logs')}
          </div>
          <div className="flex items-center gap-2">
            <Button
                variant="outline" size="sm"
                className="h-8 bg-zinc-900 border-zinc-700 hover:bg-zinc-800"
                onClick={() => setPagination(prev => ({...prev, pageIndex: Math.max(0, prev.pageIndex - 1)}))}
                disabled={pagination.pageIndex === 0}
            >
                <ChevronLeft className="h-4 w-4" /> {t('common.previous')}
            </Button>
            <Button
                variant="outline" size="sm"
                className="h-8 bg-zinc-900 border-zinc-700 hover:bg-zinc-800"
                onClick={() => setPagination(prev => ({...prev, pageIndex: prev.pageIndex + 1}))}
                disabled={!hasMore}
            >
                {t('common.next')} <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
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
    </>
  );
}