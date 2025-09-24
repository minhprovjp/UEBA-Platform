// src/components/UI/SidebarToggle.tsx

import React from 'react';
import './SidebarToggle.css';

interface SidebarToggleProps {
  expanded: boolean;
  onToggle: () => void;
  position?: 'left' | 'right';
  size?: 'small' | 'medium' | 'large';
  className?: string;
}

const SidebarToggle: React.FC<SidebarToggleProps> = ({
  expanded,
  onToggle,
  position = 'left',
  size = 'medium',
  className = ''
}) => {
  const getToggleIcon = () => {
    if (position === 'left') {
      return expanded ? '◀' : '▶';
    } else {
      return expanded ? '▶' : '◀';
    }
  };

  const getToggleTitle = () => {
    if (position === 'left') {
      return expanded ? 'Thu gọn Sidebar' : 'Mở rộng Sidebar';
    } else {
      return expanded ? 'Thu gọn Sidebar' : 'Mở rộng Sidebar';
    }
  };

  return (
    <button
      className={`sidebar-toggle-btn ${position} ${size} ${className}`}
      onClick={onToggle}
      title={getToggleTitle()}
      aria-label={getToggleTitle()}
    >
      <span className="toggle-icon">{getToggleIcon()}</span>
    </button>
  );
};

export default SidebarToggle;
