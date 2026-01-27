import { ButtonHTMLAttributes, ReactNode } from 'react';
import { clsx } from 'clsx';

interface FuturisticButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode;
  variant?: 'primary' | 'secondary' | 'outline';
  size?: 'sm' | 'md' | 'lg';
}

export function FuturisticButton({
  children,
  variant = 'primary',
  size = 'md',
  className = '',
  ...props
}: FuturisticButtonProps) {
  const baseClasses = 'btn font-semibold transition-all duration-300 transform hover:scale-105 focus:ring-2 focus:ring-offset-2';

  const variantClasses = {
    primary: 'btn-primary text-primary-content shadow-lg hover:shadow-primary/50',
    secondary: 'btn-secondary text-secondary-content',
    outline: 'btn-outline border-2',
  };

  const sizeClasses = {
    sm: 'btn-sm text-sm px-4',
    md: 'btn-md text-base px-6',
    lg: 'btn-lg text-lg px-8',
  };

  return (
    <button
      className={clsx(baseClasses, variantClasses[variant], sizeClasses[size], className)}
      {...props}
    >
      {children}
    </button>
  );
}

export default FuturisticButton;
