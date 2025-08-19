// src/components/Layout/Sidebar.tsx
import React from 'react';
import './Layout.css'; 

// Định nghĩa props để nhận hàm điều hướng từ App.tsx
interface SidebarProps {
<<<<<<< Updated upstream
<<<<<<< Updated upstream
  onNavigate: (page: 'dashboard' | 'engine-control') => void;
=======
  onNavigate: (page: 'dashboard' | 'engine-control' | 'configuration' | 'anomaly-explorer') => void;
>>>>>>> Stashed changes
=======
  onNavigate: (page: 'dashboard' | 'engine-control' | 'configuration' | 'anomaly-explorer') => void;
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
<<<<<<< Updated upstream
=======
=======
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
        {/* Các mục menu khác sẽ được thêm sau */}
      </ul>
    </nav>
  );
};

export default Sidebar;