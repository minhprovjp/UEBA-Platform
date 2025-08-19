// src/App.tsx
import React, { useState } from 'react';
import Sidebar from './components/Layout/Sidebar';
import MainContent from './components/Layout/MainContent';
import DashboardPage from './components/Dashboard/DashboardPage';
import EngineControlPage from './components/EngineControl/EngineControlPage';
import './components/Layout/Layout.css';

// Định nghĩa các trang có sẵn
type Page = 'dashboard' | 'engine-control';

const App: React.FC = () => {
  // State để theo dõi trang đang được hiển thị
  const [activePage, setActivePage] = useState<Page>('dashboard');

  const handleNavigate = (page: Page) => {
    setActivePage(page);
  };

  return (
    <div className="app-layout">
      {/* Truyền hàm điều hướng xuống cho Sidebar */}
      <Sidebar onNavigate={handleNavigate} />
      <MainContent>
        {/* Hiển thị component tương ứng với trang đang hoạt động */}
        {activePage === 'dashboard' && <DashboardPage />}
        {activePage === 'engine-control' && <EngineControlPage />}
      </MainContent>
    </div>
  );
};

export default App;