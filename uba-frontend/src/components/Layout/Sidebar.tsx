// src/components/Layout/Sidebar.tsx
import React from 'react';
import './Layout.css'; 

// Định nghĩa props để nhận hàm điều hướng từ App.tsx
interface SidebarProps {
  onNavigate: (page: 'dashboard' | 'engine-control' | 'configuration' | 'anomaly-explorer') => void;
}

const Sidebar: React.FC<SidebarProps> = ({ onNavigate }) => {
  return (
    <nav className="sidebar">
      <div className="sidebar-header">
        <h3>🕵️ UBA Platform</h3>
      </div>
      <ul className="sidebar-menu">
        {/* Dashboard button with icon */}
        <li>
          <button onClick={() => onNavigate('dashboard')}>
            📊 Dashboard
          </button>
        </li>
        {/* Engine Control button with icon */}
        <li>
          <button onClick={() => onNavigate('engine-control')}>
            ⚙️ Engine Control
          </button>
        </li>
        {/* Configuration button with icon */}
        <li>
          <button onClick={() => onNavigate('configuration')}>
            🔧 Configuration
          </button>
        </li>
        <li>
          <button onClick={() => onNavigate('anomaly-explorer')}>
            🔍 Anomaly Explorer
          </button>
        </li>
        {/* Các mục menu khác sẽ được thêm sau */}
      </ul>
    </nav>
  );
};

export default Sidebar;