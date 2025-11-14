// src/App.jsx
import { Routes, Route, Link, useLocation } from 'react-router-dom'
import { Database, ShieldAlert, Settings, LayoutDashboard } from "lucide-react";

// Import các trang (chúng ta sẽ tạo chúng ở bước sau)
import LogExplorer from "./pages/LogExplorer";
import Dashboard from './pages/Dashboard';
import AnomalyTriage from './pages/AnomalyTriage';
import SettingsPage from './pages/SettingsPage'; // Đổi tên để tránh xung đột

const navItems = [
  { name: "Dashboard", icon: LayoutDashboard, path: "/" },
  { name: "Log Explorer", icon: Database, path: "/logs" },
  { name: "Anomaly Triage", icon: ShieldAlert, path: "/anomalies" },
  { name: "Settings", icon: Settings, path: "/settings" },
];

export default function App() {
  const location = useLocation(); // Hook để biết bạn đang ở trang nào

  return (
    <div className="flex h-screen bg-background text-foreground">
      
      {/* 1. Sidebar (Thanh bên trái) */}
      <nav className="w-64 border-r border-border/60 p-4 flex flex-col">
        <h1 className="text-2xl font-bold text-primary-500 mb-6">UEBA PLATFORM</h1>
        
        <div className="flex flex-col space-y-2">
          {navItems.map((item) => (
            <Link
              key={item.name}
              to={item.path} // Dùng <Link> để điều hướng
              className={`
                flex items-center space-x-3 p-2 rounded-md transition-colors text-left
                ${location.pathname === item.path
                  ? 'bg-primary-600 text-white' // Màu cam khi được chọn
                  : 'hover:bg-zinc-800'
                }
              `}
            >
              <item.icon className="h-5 w-5" />
              <span>{item.name}</span>
            </Link>
          ))}
        </div>

        <div className="mt-auto text-xs text-muted-foreground">
          <p className="text-green-400">● SYSTEM ONLINE</p>
          <p>UPTIME: 72:14:33</p>
          <p>LOGS PROCESSED: 12,345</p>
        </div>
      </nav>

      {/* 2. Vùng nội dung chính (sử dụng Routes) */}
      <main className="flex-1 p-6 overflow-hidden">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/logs" element={<LogExplorer />} />
          <Route path="/anomalies" element={<AnomalyTriage />} />
          <Route path="/settings" element={<SettingsPage />} />
        </Routes>
      </main>
    </div>
  );
}