import React, { ButtonHTMLAttributes, forwardRef } from 'react';
import { Link } from 'react-router-dom';

interface FuturisticButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'outline';
  size?: 'sm' | 'md' | 'lg';
  to?: string;
  children: React.ReactNode;
}

const FuturisticButton = forwardRef<HTMLButtonElement, FuturisticButtonProps>(
  ({ variant = 'primary', size = 'md', to, children, className = '', ...props }, ref) => {
    const baseStyles = 'relative overflow-hidden font-semibold rounded-lg transition-all duration-300 transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-offset-2';

    const sizeStyles = {
      sm: 'px-4 py-2 text-sm',
      md: 'px-6 py-3 text-base',
      lg: 'px-8 py-4 text-lg',
    };

    const variantStyles = {
      primary: 'bg-gradient-to-r from-purple-600 to-blue-500 text-white hover:from-purple-700 hover:to-blue-600 focus:ring-purple-500',
      secondary: 'bg-gradient-to-r from-pink-500 to-orange-400 text-white hover:from-pink-600 hover:to-orange-500 focus:ring-pink-500',
      outline: 'border-2 border-purple-500 text-purple-500 hover:bg-purple-500 hover:text-white focus:ring-purple-500',
    };

    const combinedClassName = `${baseStyles} ${sizeStyles[size]} ${variantStyles[variant]} ${className}`;

    if (to) {
      return (
        <Link
          to={to}
          className={combinedClassName}
          role="button"
          tabIndex={0}
        >
          <span className="relative z-10">{children}</span>
          <div className="absolute inset-0 bg-white opacity-0 hover:opacity-10 transition-opacity duration-300" />
        </Link>
      );
    }

    return (
      <button
        ref={ref}
        className={combinedClassName}
        {...props}
      >
        <span className="relative z-10">{children}</span>
        <div className="absolute inset-0 bg-white opacity-0 hover:opacity-10 transition-opacity duration-300" />
      </button>
    );
  }
);

FuturisticButton.displayName = 'FuturisticButton';

export default FuturisticButton;
