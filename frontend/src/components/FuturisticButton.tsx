import type { ButtonHTMLAttributes, ReactNode } from 'react';

export interface FuturisticButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode;
  variant?: 'primary' | 'secondary' | 'ghost';
  size?: 'sm' | 'md' | 'lg';
}

export default function FuturisticButton({
  children,
  variant = 'primary',
  size = 'md',
  className = '',
  ...props
}: FuturisticButtonProps) {
  const variantClasses = {
    primary: [
      'futuristic-btn-primary',
      'bg-blue-600 text-white',
      'hover:bg-blue-700',
      'border border-blue-500',
    ],
    secondary: [
      'futuristic-btn-secondary',
      'bg-white/10 text-current',
      'hover:bg-white/20',
      'border border-white/30',
    ],
    ghost: [
      'futuristic-btn-ghost',
      'bg-transparent text-current',
      'hover:bg-white/10',
      'border border-transparent',
    ],
  };

  const sizeClasses = {
    sm: 'px-4 py-2 text-sm min-h-[36px]',
    md: 'px-6 py-3 text-base min-h-[44px]',
    lg: 'px-8 py-4 text-lg min-h-[52px]',
  };

  return (
    <button
      className={[
        'futuristic-button',
        'inline-flex items-center justify-center',
        'rounded-lg font-medium',
        'transition-all duration-200',
        'focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2',
        'disabled:opacity-50 disabled:cursor-not-allowed',
        ...variantClasses[variant],
        sizeClasses[size],
        className,
      ].join(' ')}
      {...props}
    >
      {children}
    </button>
  );
}
