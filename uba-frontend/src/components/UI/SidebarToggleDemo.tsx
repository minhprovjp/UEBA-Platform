// src/components/UI/SidebarToggleDemo.tsx

import React, { useState } from 'react';
import SidebarToggle from './SidebarToggle';
import './SidebarToggleDemo.css';

const SidebarToggleDemo: React.FC = () => {
  const [leftSidebarExpanded, setLeftSidebarExpanded] = useState(true);
  const [rightSidebarExpanded, setRightSidebarExpanded] = useState(true);

  return (
    <div className="sidebar-toggle-demo">
      <h2>Sidebar Toggle Component Demo</h2>
      
      <div className="demo-section">
        <h3>Left Sidebar Toggle</h3>
        <div className="demo-layout left-sidebar">
          <div className={`demo-sidebar ${!leftSidebarExpanded ? 'collapsed' : ''}`}>
            <SidebarToggle
              expanded={leftSidebarExpanded}
              onToggle={() => setLeftSidebarExpanded(!leftSidebarExpanded)}
              position="left"
              size="medium"
            />
            <div className="sidebar-content">
              {leftSidebarExpanded ? (
                <div className="expanded-content">
                  <h4>Left Sidebar Content</h4>
                  <p>This is the expanded left sidebar with full content.</p>
                  <ul>
                    <li>Navigation Item 1</li>
                    <li>Navigation Item 2</li>
                    <li>Navigation Item 3</li>
                  </ul>
                </div>
              ) : (
                <div className="collapsed-content">
                  <div className="collapsed-icon">📁</div>
                  <div className="collapsed-icon">🔍</div>
                  <div className="collapsed-icon">⚙️</div>
                </div>
              )}
            </div>
          </div>
          <div className="demo-main">
            <h4>Main Content Area</h4>
            <p>This area shows how the main content responds to sidebar state changes.</p>
            <p>Sidebar is currently: <strong>{leftSidebarExpanded ? 'Expanded' : 'Collapsed'}</strong></p>
          </div>
        </div>
      </div>

      <div className="demo-section">
        <h3>Right Sidebar Toggle</h3>
        <div className="demo-layout right-sidebar">
          <div className="demo-main">
            <h4>Main Content Area</h4>
            <p>This area shows how the main content responds to right sidebar state changes.</p>
            <p>Sidebar is currently: <strong>{rightSidebarExpanded ? 'Expanded' : 'Collapsed'}</strong></p>
          </div>
          <div className={`demo-sidebar ${!rightSidebarExpanded ? 'collapsed' : ''}`}>
            <SidebarToggle
              expanded={rightSidebarExpanded}
              onToggle={() => setRightSidebarExpanded(!rightSidebarExpanded)}
              position="right"
              size="medium"
            />
            <div className="sidebar-content">
              {rightSidebarExpanded ? (
                <div className="expanded-content">
                  <h4>Right Sidebar Content</h4>
                  <p>This is the expanded right sidebar with full content.</p>
                  <div className="widget">
                    <h5>Widget 1</h5>
                    <p>Some widget content here.</p>
                  </div>
                  <div className="widget">
                    <h5>Widget 2</h5>
                    <p>Another widget content.</p>
                  </div>
                </div>
              ) : (
                <div className="collapsed-content">
                  <div className="collapsed-icon">📊</div>
                  <div className="collapsed-icon">📈</div>
                  <div className="collapsed-icon">🔔</div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      <div className="demo-section">
        <h3>Different Sizes</h3>
        <div className="size-demo">
          <div className="size-item">
            <h4>Small</h4>
            <SidebarToggle
              expanded={true}
              onToggle={() => {}}
              position="left"
              size="small"
            />
          </div>
          <div className="size-item">
            <h4>Medium</h4>
            <SidebarToggle
              expanded={true}
              onToggle={() => {}}
              position="left"
              size="medium"
            />
          </div>
          <div className="size-item">
            <h4>Large</h4>
            <SidebarToggle
              expanded={true}
              onToggle={() => {}}
              position="left"
              size="large"
            />
          </div>
        </div>
      </div>

      <div className="demo-section">
        <h3>Usage Instructions</h3>
        <div className="usage-instructions">
          <h4>Basic Usage:</h4>
          <pre><code>{`import { SidebarToggle } from '../UI';

const [sidebarExpanded, setSidebarExpanded] = useState(true);

<SidebarToggle
  expanded={sidebarExpanded}
  onToggle={() => setSidebarExpanded(!sidebarExpanded)}
  position="left"
  size="medium"
/>`}</code></pre>
          
          <h4>Props:</h4>
          <ul>
            <li><strong>expanded</strong>: Boolean - Current state of sidebar</li>
            <li><strong>onToggle</strong>: Function - Called when toggle button is clicked</li>
            <li><strong>position</strong>: 'left' | 'right' - Position of toggle button</li>
            <li><strong>size</strong>: 'small' | 'medium' | 'large' - Size of toggle button</li>
            <li><strong>className</strong>: String - Additional CSS classes</li>
          </ul>

        </div>
      </div>
    </div>
  );
};

export default SidebarToggleDemo;
