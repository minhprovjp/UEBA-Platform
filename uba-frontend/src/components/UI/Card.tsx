// src/components/UI/Card.tsx
import React from 'react';
import './Card.css';

interface CardProps {
  children: React.ReactNode;
  variant?: 'default' | 'elevated' | 'outlined' | 'glass';
  size?: 'small' | 'medium' | 'large';
  className?: string;
  onClick?: () => void;
  hoverable?: boolean;
  loading?: boolean;
  header?: React.ReactNode;
  footer?: React.ReactNode;
  icon?: React.ReactNode;
}

const Card: React.FC<CardProps> = ({
  children,
  variant = 'default',
  size = 'medium',
  className = '',
  onClick,
  hoverable = false,
  loading = false,
  header,
  footer,
  icon
}) => {
  const cardClasses = [
    'card',
    `card-${variant}`,
    `card-${size}`,
    hoverable ? 'card-hoverable' : '',
    loading ? 'card-loading' : '',
    onClick ? 'card-clickable' : '',
    className
  ].filter(Boolean).join(' ');

  const handleClick = () => {
    if (onClick && !loading) {
      onClick();
    }
  };

  return (
    <div className={cardClasses} onClick={handleClick}>
      {loading && (
        <div className="card-loading-overlay">
          <div className="card-spinner">
            <div className="spinner-ring"></div>
          </div>
        </div>
      )}
      
      {header && (
        <div className="card-header">
          {icon && <span className="card-header-icon">{icon}</span>}
          <div className="card-header-content">{header}</div>
        </div>
      )}
      
      <div className="card-body">
        {children}
      </div>
      
      {footer && (
        <div className="card-footer">
          {footer}
        </div>
      )}
    </div>
  );
};

export default Card;
