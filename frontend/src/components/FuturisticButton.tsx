import React from 'react';
import { Link } from 'react-router-dom';

interface FuturisticButtonProps {
  children: React.ReactNode;
  onClick?: () => void;
  to?: string;
  variant?: 'primary' | 'secondary' | 'ghost';
  size?: 'sm' | 'md' | 'lg';
  disabled?: boolean;
  loading?: boolean;
  className?: string;
  type?: 'button' | 'submit' | 'reset';
  'data-testid'?: string;
}

export function FuturisticButton({
  children,
  onClick,
  to,
  variant = 'primary',
  size = 'md',
  disabled = false,
  loading = false,
  className = '',
  type = 'button',
  'data-testid': testId,
}: FuturisticButtonProps) {
  const baseClasses = 'btn transition-all duration-300 font-semibold';

  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary btn-outline',
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
        {loading ? <span className="loading loading-spinner loading-sm"></span> : children}
      </Link>
    );
  }

  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled || loading}
      className={classes}
      data-testid={testId}
    >
      {loading ? <span className="loading loading-spinner loading-sm"></span> : children}
    </button>
  );
}
