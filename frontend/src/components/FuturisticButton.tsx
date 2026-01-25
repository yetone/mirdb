import React from 'react';
import { Link } from 'react-router-dom';

interface FuturisticButtonProps {
  children: React.ReactNode;
  to?: string;
  onClick?: () => void;
  variant?: 'primary' | 'secondary' | 'ghost';
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
  'data-testid': testId,
}: FuturisticButtonProps) {
  const baseClasses = 'btn transition-all duration-300 hover:scale-105';

  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary',
    ghost: 'btn-ghost',
  };

  const sizeClasses = {
    sm: 'btn-sm',
    md: 'btn-md',
    lg: 'btn-lg',
  };

  const classes = `${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`;

  if (to) {
    return (
      <Link to={to} className={classes} data-testid={testId}>
        {children}
      </Link>
    );
  }

  return (
    <button onClick={onClick} className={classes} data-testid={testId}>
      {children}
    </button>
  );
}
