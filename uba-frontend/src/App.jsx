// uba_frontend/src/App.jsx
import { Routes, Route, Link, useLocation } from 'react-router-dom';
import { Database, ShieldAlert, Settings, LayoutDashboard, ChevronLeft, ChevronRight, LogOut, User, ShieldCheck, Shield } from "lucide-react";
import React, { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import LanguageSwitcher from './components/LanguageSwitcher';

import LogExplorer from "./pages/LogExplorer";
import Dashboard from './pages/Dashboard';
import AnomalyTriage from './pages/AnomalyTriage';
import SettingsPage from './pages/SettingsPage';
import AccessControlPage from './pages/AccessControlPage';
import SelfMonitoring from './pages/SelfMonitoring';
import LoginPage from './pages/LoginPage';

export default function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [isCollapsed, setIsCollapsed] = useState(false);
  const location = useLocation();
  const { t } = useTranslation();
  const navItems = [
  { name: t('sidebar.dashboard'), icon: LayoutDashboard, path: "/" },
  { name: t('sidebar.logs'), icon: Database, path: "/logs" },
  { name: t('sidebar.anomalies'), icon: ShieldAlert, path: "/anomalies" },
  { name: t('sidebar.self_monitoring'), icon: Shield, path: "/self-monitoring" },
  { name: t('sidebar.access_control'), icon: ShieldCheck, path: "/access-control" }
  ];

  useEffect(() => {
    const token = localStorage.getItem('uba_token');
    if (token) setIsAuthenticated(true);
    setIsLoading(false);
  }, []);

  if (isLoading) return <div className="h-screen bg-black flex items-center justify-center text-white">Loading...</div>;

  if (!isAuthenticated) {
    return <LoginPage onLoginSuccess={() => setIsAuthenticated(true)} />;
  }

  // Helper render Nav Item
  const NavItem = ({ item, isBottom = false }) => {
    const isActive = item.path === "/" 
        ? location.pathname === "/" 
        : location.pathname.startsWith(item.path);
        
    return (
      <Link
        to={item.path}
        title={isCollapsed ? item.name : ""}
        className={`
          flex items-center space-x-3 p-2 rounded-md transition-all duration-200
          ${isActive ? 'bg-primary-600 text-white shadow-lg shadow-primary-900/20' : 'text-zinc-400 hover:bg-zinc-800 hover:text-white'}
          ${isCollapsed ? 'justify-center' : ''}
          ${isBottom ? 'mt-auto' : ''}
        `}
      >
        <item.icon className={`shrink-0 ${isCollapsed ? "h-6 w-6" : "h-5 w-5"}`} />
        {!isCollapsed && <span className="truncate font-medium text-sm">{item.name}</span>}
      </Link>
    );
  };

  return (
    <div className="flex h-screen bg-background text-foreground overflow-hidden">
      
      {/* SIDEBAR: Width thay đổi dựa trên isCollapsed */}
      <nav 
        className={`
          relative border-r border-border/40 bg-zinc-950 flex flex-col p-3 transition-all duration-300 ease-in-out
          ${isCollapsed ? 'w-20' : 'w-64'}
        `}
      >
        {/* Toggle Button */}
        <button 
            onClick={() => setIsCollapsed(!isCollapsed)}
            className="absolute -right-3 top-6 bg-zinc-800 border border-zinc-700 rounded-full p-1 text-zinc-400 hover:text-white hover:bg-primary-600 transition-colors z-50 shadow-md"
        >
            {isCollapsed ? <ChevronRight className="w-3 h-3"/> : <ChevronLeft className="w-3 h-3"/>}
        </button>

        {/* Logo Area */}
        <div className={`flex items-center mb-8 px-2 ${isCollapsed ? 'justify-center' : ''}`}>
           <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-primary-500 to-purple-600 flex items-center justify-center shrink-0 shadow-lg shadow-primary-500/20">
              <ShieldAlert className="text-white w-5 h-5" />
           </div>
           {!isCollapsed && (
              <div className="ml-3 overflow-hidden whitespace-nowrap">
                  <h1 className="text-lg font-bold tracking-tight text-white">UEBA <span className="text-primary-500">Platform</span></h1>
                  <p className="text-[10px] text-zinc-500 font-mono">v2.4.0-release</p>
              </div>
           )}
        </div>
        
        {/* Main Navigation */}
        <div className="flex flex-col space-y-1.5 flex-1">
          {navItems.map((item) => (
            <NavItem key={item.name} item={item} />
          ))}
        </div>

        {/* Bottom Navigation (Settings & User) */}
        <div className="flex flex-col space-y-1.5 mt-auto pt-4 border-t border-zinc-900">
            {!isCollapsed && <div className="px-2 mb-2"><LanguageSwitcher /></div>}
            <NavItem item={{ name: t('sidebar.settings'), icon: Settings, path: "/settings" }} />
            
            {/* User Profile Mini (Optional) */}
            {!isCollapsed && (
                <div className="mt-2 p-2 rounded-lg bg-zinc-900/50 border border-zinc-800 flex items-center gap-3">
                    <div className="w-8 h-8 rounded-full bg-zinc-800 flex items-center justify-center text-zinc-400">
                        <User className="w-4 h-4" />
                    </div>
                    <div className="overflow-hidden">
                        <p className="text-xs font-bold text-white truncate">Admin User</p>
                        <p className="text-[10px] text-zinc-500 truncate">Security Analyst</p>
                    </div>
                </div>
            )}
        </div>
      </nav>

      {/* MAIN CONTENT */}
      <main className="flex-1 p-0 overflow-hidden bg-black/95 relative">
        {/* Background Grid Pattern cho đẹp */}
        <div className="absolute inset-0 bg-[url('https://grainy-gradients.vercel.app/noise.svg')] opacity-20 pointer-events-none"></div>
        
        <div className="h-full overflow-y-auto p-3 relative z-10 custom-scrollbar">
            <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/logs" element={<LogExplorer />} />
            <Route path="/anomalies" element={<AnomalyTriage />} />
            <Route path="/self-monitoring" element={<SelfMonitoring />} />
            <Route path="/access-control" element={<AccessControlPage />} />
            <Route path="/settings" element={<SettingsPage />} />
            </Routes>
        </div>
      </main>
    </div>
  );
}