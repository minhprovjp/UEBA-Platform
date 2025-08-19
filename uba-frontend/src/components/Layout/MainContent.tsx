// src/components/Layout/MainContent.tsx
import React, { type ReactNode } from 'react';
import './Layout.css';

interface MainContentProps {
  children: ReactNode;
}

const MainContent: React.FC<MainContentProps> = ({ children }) => {
  return (
    <main className="main-content">
      {children}
    </main>
  );
};

export default MainContent;