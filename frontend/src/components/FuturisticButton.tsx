import type { ButtonHTMLAttributes, ReactNode } from 'react';

interface FuturisticButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode;
  variant?: 'primary' | 'secondary' | 'ghost' | 'outline';
  size?: 'sm' | 'md' | 'lg';
  isLoading?: boolean;
}

export function FuturisticButton({
  children,
  variant = 'primary',
  size = 'md',
  isLoading = false,
  className = '',
  disabled,
  ...props
}: FuturisticButtonProps) {
  const baseClasses = 'btn relative overflow-hidden transition-all duration-300';

  const variantClasses = {
    primary: 'btn-primary hover:brightness-110',
    secondary: 'btn-secondary hover:brightness-110',
    ghost: 'btn-ghost',
    outline: 'btn-outline',
  };

  const sizeClasses = {
    sm: 'btn-sm',
    md: 'btn-md',
    lg: 'btn-lg',
  };

  return (
    <button
      className={`${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`}
      disabled={disabled || isLoading}
      {...props}
    >
      {isLoading ? (
        <>
          <span className="loading loading-spinner loading-sm"></span>
          <span className="ml-2">Loading...</span>
        </>
      ) : (
        children
      )}
    </button>
  );
}
