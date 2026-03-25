import React, { ReactNode } from 'react';
import { Link } from 'react-router-dom';

interface FuturisticButtonProps {
  children: ReactNode;
  to?: string;
  onClick?: () => void;
  variant?: 'primary' | 'secondary' | 'outline';
  size?: 'sm' | 'md' | 'lg';
  className?: string;
  'data-testid'?: string;
}

export function FuturisticButton({
  children,
  to,
  onClick,
  variant = 'primary',
  size = 'md',
  className = '',
  'data-testid': dataTestId,
}: FuturisticButtonProps) {
  const baseClasses = 'relative overflow-hidden transition-all duration-300 font-semibold rounded-lg';

  const variantClasses = {
    primary: 'bg-primary text-primary-content hover:bg-primary-focus shadow-lg hover:shadow-primary/50',
    secondary: 'bg-secondary text-secondary-content hover:bg-secondary-focus shadow-lg hover:shadow-secondary/50',
    outline: 'border-2 border-primary text-primary hover:bg-primary hover:text-primary-content',
  };

  const sizeClasses = {
    sm: 'px-4 py-2 text-sm',
    md: 'px-6 py-3 text-base',
    lg: 'px-8 py-4 text-lg',
  };

  const combinedClasses = `${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`;

  if (to) {
    return (
      <Link to={to} className={combinedClasses} data-testid={dataTestId}>
        {children}
      </Link>
    );
  }

  return (
    <button onClick={onClick} className={combinedClasses} data-testid={dataTestId}>
      {children}
    </button>
  );
}
