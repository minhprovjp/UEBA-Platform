// src/App.tsx
import { BrowserRouter as Router, Routes, Route, NavLink } from 'react-router-dom';
import { Home, ShieldAlert, SlidersHorizontal, Settings } from 'lucide-react';
import DashboardPage from './pages/DashboardPage';
import { AnomalyExplorerPage } from './pages/AnomalyExplorerPage.tsx';
// Import các trang (chúng ta sẽ tạo chúng ngay sau đây)
// import DashboardPage from './pages/DashboardPage';
// import AnomalyExplorerPage from './pages/AnomalyExplorerPage';
import ConfigurationPage from './pages/ConfigurationPage';
import EngineControlPage from './pages/EngineControlPage';

function App() {
  return (
    <Router>
      <div style={{ display: 'flex', height: '100vh' }}>
        <nav style={{ width: '250px', backgroundColor: '#1a1a1a', padding: '1rem' }}>
          <h2 style={{ color: 'white' }}>🕵️ UBA Platform</h2>
          <ul style={{ listStyle: 'none', padding: 0 }}>
            <li><NavLink to="/"><Home size={18} /> Dashboard</NavLink></li>
            <li><NavLink to="/explorer"><ShieldAlert size={18} /> Anomaly Explorer</NavLink></li>
            <li><NavLink to="/engine"><SlidersHorizontal size={18} /> Engine Control</NavLink></li>
            <li><NavLink to="/config"><Settings size={18} /> Configuration</NavLink></li>
          </ul>
        </nav>
        <main style={{ flex: 1, padding: '2rem', overflowY: 'auto' }}>
          <Routes>
            <Route path="/" element={<DashboardPage />} /> {/* <-- SỬA DÒNG NÀY */}
            <Route path="/explorer" element={<AnomalyExplorerPage />} /> 
            { <Route path="/engine" element={<EngineControlPage />} /> }
            { <Route path="/config" element={<ConfigurationPage />} /> }
            <Route path="/" element={<h1>Dashboard Page (Coming Soon)</h1>} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;