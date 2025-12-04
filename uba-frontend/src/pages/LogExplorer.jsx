// // uba_frontend/src/pages/LogExplorer.jsx
import React, { useState, useMemo } from 'react';
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Filter, Bot, Check, X, ChevronLeft, ChevronRight } from 'lucide-react';
import { useLogs, useAnalyzeMutation, useFeedbackMutation } from '@/api/queries'; // Import hooks mới
import { AnomalyDetailModal } from '@/components/AnomalyDetailModal'; // Import Modal (sẽ tạo ở bước 4)
import { Toaster, toast } from 'sonner';

const PAGE_SIZE = 20; // Hiển thị 20 log mỗi trang

export default function LogExplorer() {
  // --- State cho Bộ lọc và Phân trang ---
  const [filters, setFilters] = useState({
    search: '',
    user: null, // 'user:root'
    date_from: null,
    date_to: null,
  });
  const [pagination, setPagination] = useState({
    pageIndex: 0, // Bắt đầu từ trang 0
    pageSize: PAGE_SIZE,
  });

  // --- Lấy dữ liệu bằng React Query ---
  // `useLogs` sẽ tự động tải lại khi `filters` hoặc `pagination` thay đổi
  const { data, isLoading, isError, error } = useLogs({
    ...filters,
    ...pagination,
  });

  // Lấy ra logs và trạng thái phân trang
  const logs = data?.logs || [];
  const hasMore = data?.hasMore || false;
  
  // --- State cho Modal ---
  const [selectedLog, setSelectedLog] = useState(null); 
  const [isModalOpen, setIsModalOpen] = useState(false);

  // --- Lấy ra các Mutations ---
  const analyzeMutation = useAnalyzeMutation();
  const feedbackMutation = useFeedbackMutation();

  // --- Lấy danh sách filter động ---
  // Lấy ra danh sách User duy nhất từ log đã tải
  const uniqueUsers = useMemo(() => {
    return [...new Set(logs.map(log => log.user).filter(Boolean))];
  }, [logs]);

  // --- Handlers ---
  const handleRowClick = (log) => {
    setSelectedLog(log);
    setIsModalOpen(true);
  };

  const handleAnalyze = () => {
    analyzeMutation.mutate(selectedLog, {
      onSuccess: (data) => {
        // Cập nhật state để hiển thị kết quả AI
        setSelectedLog(prev => ({
          ...prev, 
          aiAnalysis: data.data.final_analysis 
        }));
      }
    });
  };

  const handleFeedback = (label) => {
    feedbackMutation.mutate(
      { label, anomaly_data: selectedLog },
      { onSuccess: () => setIsModalOpen(false) } // Đóng modal khi feedback thành công
    );
  };

  // -------------------------
  // PHẦN GIAO DIỆN (RENDER)
  // -------------------------
  return (
    <>
      <Toaster position="top-right" theme="dark" />
      <div className="h-full flex flex-col">
        <header>
          <h2 className="text-2xl font-semibold">Log Explorer</h2>
          <p className="text-muted-foreground">Quản lý và giám sát log truy vấn CSDL.</p>
        </header>

        {/* Thanh tìm kiếm và bộ lọc (ĐÃ HOẠT ĐỘNG) */}
        <div className="flex items-center space-x-2 py-4">
          <Input 
            placeholder="Tìm kiếm query..." 
            className="max-w-sm bg-zinc-900"
            value={filters.search}
            onChange={(e) => setFilters(prev => ({...prev, search: e.target.value, pageIndex: 0}))}
          />
          {/* TODO: Thêm Dropdown cho uniqueUsers và Bộ lọc Ngày */}
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
                <TableHead>Client IP</TableHead>
                <TableHead>Query</TableHead>
                <TableHead>Risk</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {isLoading && <TableRow><TableCell colSpan={5} className="text-center">Đang tải dữ liệu...</TableCell></TableRow>}
              {isError && <TableRow><TableCell colSpan={5} className="text-center text-red-500">{error.message}</TableCell></TableRow>}
              {!isLoading && logs.map((log) => (
                <TableRow key={log.id} onClick={() => handleRowClick(log)} className="cursor-pointer hover:bg-zinc-900">
                  <TableCell>{new Date(log.timestamp).toLocaleString()}</TableCell>
                  <TableCell>{log.user}</TableCell>
                  <TableCell>{log.client_ip}</TableCell>
                  <TableCell><code className="text-sm">{log.query.substring(0, 70)}...</code></TableCell>
                  <TableCell>
                    {log.is_anomaly && (
                      <span className="bg-red-900 text-red-300 px-2 py-1 rounded-full text-xs font-semibold">
                        {log.anomaly_type || log.analysis_type || 'ANOMALY'}
                      </span>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        {/* Phân trang (Pagination) */}
        <div className="flex items-center justify-end space-x-2 py-4">
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
            disabled={!hasMore} // Tắt nút Next nếu không còn dữ liệu
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