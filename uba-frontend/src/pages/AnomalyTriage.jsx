// src/pages/AnomalyTriage.jsx
import React, { useState, useMemo } from 'react';
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Filter, Bot, Check, X, ChevronLeft, ChevronRight } from 'lucide-react';
import { useAnomalies, useAnomalyStats, useAnalyzeMutation, useFeedbackMutation } from '@/api/queries';
import { AnomalyDetailModal } from '@/components/AnomalyDetailModal';
import { Toaster } from 'sonner';

const PAGE_SIZE = 20;

export default function AnomalyTriage() {
  // --- State cho Bộ lọc và Phân trang ---
  const [filters, setFilters] = useState({
    search: '',
    user: null,
    anomaly_type: null,
    date_from: null,
    date_to: null,
  });
  const [pagination, setPagination] = useState({
    pageIndex: 0,
    pageSize: PAGE_SIZE,
  });

  // --- Lấy dữ liệu ---
  // 1. Lấy dữ liệu cho Bảng (có phân trang)
  const { data, isLoading, isError, error } = useAnomalies({
    ...filters,
    ...pagination,
  });
  // 2. Lấy dữ liệu cho Thống kê (tải tất cả)
  const { data: stats, isLoading: isStatsLoading } = useAnomalyStats();

  // Lấy ra logs và trạng thái phân trang
  const anomalies = data?.anomalies || [];
  const hasMore = data?.hasMore || false;
  
  // --- State cho Modal ---
  const [selectedLog, setSelectedLog] = useState(null); 
  const [isModalOpen, setIsModalOpen] = useState(false);

  // --- Lấy ra các Mutations ---
  const analyzeMutation = useAnalyzeMutation();
  const feedbackMutation = useFeedbackMutation();

  // --- Lấy danh sách filter động ---
  const uniqueUsers = useMemo(() => {
    return [...new Set(stats?.rawAnomalies.map(log => log.user).filter(Boolean))];
  }, [stats]);
  const uniqueTypes = useMemo(() => {
    return Object.keys(stats?.countsPerType || {});
  }, [stats]);


  // --- Handlers (Giống hệt LogExplorer) ---
  const handleRowClick = (log) => {
    setSelectedLog(log);
    setIsModalOpen(true);
  };

  const handleAnalyze = () => {
    analyzeMutation.mutate(selectedLog, {
      onSuccess: (data) => {
        setSelectedLog(prev => ({ ...prev, aiAnalysis: data.data.final_analysis }));
      }
    });
  };

  const handleFeedback = (label) => {
    feedbackMutation.mutate(
      { label, anomaly_data: selectedLog },
      { onSuccess: () => setIsModalOpen(false) }
    );
  };

  return (
    <>
      <Toaster position="top-right" theme="dark" />
      <div className="h-full flex flex-col">
        <header>
          <h2 className="text-2xl font-semibold">Anomaly Triage</h2>
          <p className="text-muted-foreground">Xem xét và xử lý các bất thường đã được phát hiện.</p>
        </header>

        {/* Thẻ Thống kê (Dữ liệu thật) */}
        <div className="grid grid-cols-4 gap-4 my-4">
          <StatCard title="Total Anomalies" value={isStatsLoading ? "..." : stats.totalAnomalies} />
          {/* Hiển thị 3 loại bất thường hàng đầu */}
          {Object.entries(stats?.countsPerType || {})
            .sort(([,a], [,b]) => b - a) // Sắp xếp
            .slice(0, 3) // Lấy 3 cái đầu
            .map(([type, count]) => (
              <StatCard key={type} title={type} value={count} />
            ))
          }
        </div>

        {/* Thanh tìm kiếm và bộ lọc (ĐÃ HOẠT ĐỘNG) */}
        <div className="flex items-center space-x-2 py-4">
          <Input 
            placeholder="Tìm kiếm query, reason..." 
            className="max-w-sm bg-zinc-900"
            value={filters.search}
            onChange={(e) => setFilters(prev => ({...prev, search: e.target.value, pageIndex: 0}))}
          />
          {/* TODO: Thêm Dropdown cho uniqueUsers và uniqueTypes */}
          <Button variant="outline" className="bg-zinc-900">
            <Filter className="h-4 w-4 mr-2" />
            Filter
          </Button>
        </div>

        {/* Bảng dữ liệu */}
        <div className="flex-1 overflow-auto rounded-md border border-border">
          <Table>
            <TableHeader className="bg-zinc-900">
              <TableRow>
                <TableHead>Timestamp</TableHead>
                <TableHead>User</TableHead>
                <TableHead>Anomaly Type</TableHead>
                <TableHead>Query</TableHead>
                <TableHead>Reason</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading && <TableRow><TableCell colSpan={5} className="text-center">Đang tải bất thường...</TableCell></TableRow>}
              {isError && <TableRow><TableCell colSpan={5} className="text-center text-red-500">{error.message}</TableCell></TableRow>}
              {!isLoading && anomalies.map((log) => (
                <TableRow key={log.id} onClick={() => handleRowClick(log)} className="cursor-pointer hover:bg-zinc-900">
                  <TableCell>{new Date(log.timestamp).toLocaleString()}</TableCell>
                  <TableCell>{log.user}</TableCell>
                  <TableCell>
                    <span className="bg-red-900 text-red-300 px-2 py-1 rounded-full text-xs font-semibold">
                      {log.anomaly_type}
                    </span>
                  </TableCell>
                  <TableCell><code className="text-sm">{log.query.substring(0, 50)}...</code></TableCell>
                  <TableCell className="text-muted-foreground">{log.reason || 'N/A'}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        {/* Phân trang (Pagination) */}
        <div className="flex items-center justify-end space-x-2 py-4">
           {/* (Code phân trang giống hệt LogExplorer) */}
           <span className="text-sm text-muted-foreground">
            Trang {pagination.pageIndex + 1}
          </span>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setPagination(prev => ({...prev, pageIndex: prev.pageIndex - 1}))}
            disabled={pagination.pageIndex === 0}
          >
            <ChevronLeft className="h-4 w-4" />
            Previous
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setPagination(prev => ({...prev, pageIndex: prev.pageIndex + 1}))}
            disabled={!hasMore}
          >
            Next
            <ChevronRight className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Modal chi tiết */}
      <AnomalyDetailModal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        log={selectedLog}
        onAnalyze={handleAnalyze}
        onFeedback={handleFeedback}
        isAiLoading={analyzeMutation.isPending}
        isFeedbackLoading={feedbackMutation.isPending}
      />
    </>
  );
}

// Component phụ cho thẻ KPI
const StatCard = ({ title, value }) => (
  <div className="bg-zinc-900 border border-border p-4 rounded-lg">
      <p className="text-sm text-muted-foreground uppercase">{title}</p>
      <p className="text-3xl font-bold text-primary-500">{value}</p>
  </div>
);