// src/components/UI/Pagination.tsx
import React from 'react';
import './Pagination.css'; // Sẽ tạo file này

interface PaginationProps {
  currentPage: number;
  totalItems: number;
  itemsPerPage: number;
  onPageChange: (page: number) => void;
}

const Pagination: React.FC<PaginationProps> = ({ currentPage, totalItems, itemsPerPage, onPageChange }) => {
  const totalPages = Math.ceil(totalItems / itemsPerPage);
  if (totalPages <= 1) return null;

  return (
    <div className="pagination-container">
      <button onClick={() => onPageChange(currentPage - 1)} disabled={currentPage === 1}>
        &laquo; Trước
      </button>
      <span> Trang {currentPage} / {totalPages} </span>
      <button onClick={() => onPageChange(currentPage + 1)} disabled={currentPage === totalPages}>
        Sau &raquo;
      </button>
    </div>
  );
};

export default Pagination;