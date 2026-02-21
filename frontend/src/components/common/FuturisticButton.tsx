import React, { ButtonHTMLAttributes, ReactNode } from 'react';
import { Link } from 'react-router-dom';

interface FuturisticButtonProps extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'type'> {
  children: ReactNode;
  variant?: 'primary' | 'outline';
  loading?: boolean;
  to?: string;
  type?: 'button' | 'submit' | 'reset';
  'data-testid'?: string;
}

export function FuturisticButton({
  children,
  variant = 'primary',
  loading = false,
  className = '',
  disabled,
  to,
  type = 'button',
  'data-testid': dataTestId,
  ...props
}: FuturisticButtonProps) {
  const baseClasses = 'btn transition-transform duration-200 hover:scale-105 focus:ring-2 focus:ring-offset-2';
  const variantClasses = variant === 'primary'
    ? 'btn-primary'
    : 'btn-outline';

  const combinedClasses = `${baseClasses} ${variantClasses} ${className}`;

  // Render as Link if 'to' prop is provided
  if (to) {
    return (
      <Link
        to={to}
        className={combinedClasses}
        data-testid={dataTestId}
        aria-label={props['aria-label']}
      >
        {loading && <span className="loading loading-spinner loading-sm mr-2"></span>}
        {children}
      </Link>
    );
  }

  return (
    <button
      type={type}
      className={combinedClasses}
      disabled={disabled || loading}
      data-testid={dataTestId}
      {...props}
    >
      {loading && <span className="loading loading-spinner loading-sm mr-2"></span>}
      {children}
    </button>
  );
}
