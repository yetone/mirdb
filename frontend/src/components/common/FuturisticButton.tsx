import React, { ButtonHTMLAttributes, ReactNode } from 'react';

interface FuturisticButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode;
  variant?: 'primary' | 'outline';
  loading?: boolean;
}

export function FuturisticButton({
  children,
  variant = 'primary',
  loading = false,
  className = '',
  disabled,
  ...props
}: FuturisticButtonProps) {
  const baseClasses = 'btn transition-transform duration-200 hover:scale-105 focus:ring-2 focus:ring-offset-2';
  const variantClasses = variant === 'primary'
    ? 'btn-primary'
    : 'btn-outline';

  return (
    <button
      className={`${baseClasses} ${variantClasses} ${className}`}
      disabled={disabled || loading}
      {...props}
    >
      {loading && <span className="loading loading-spinner loading-sm mr-2"></span>}
      {children}
    </button>
  );
}
