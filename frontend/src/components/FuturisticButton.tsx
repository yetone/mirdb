import React from 'react';

interface FuturisticButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  children: React.ReactNode;
  variant?: 'primary' | 'secondary';
}

const FuturisticButton: React.FC<FuturisticButtonProps> = ({
  children,
  variant = 'primary',
  className = '',
  disabled = false,
  type = 'button',
  ...props
}) => {
  const baseStyles = 'px-8 py-3 rounded-lg font-semibold text-lg transition-all duration-300 transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-offset-2';

  const variantStyles = {
    primary: 'bg-primary text-primary-content hover:bg-primary-focus focus:ring-primary shadow-lg hover:shadow-primary/50',
    secondary: 'bg-secondary text-secondary-content hover:bg-secondary-focus focus:ring-secondary shadow-lg hover:shadow-secondary/50',
  };

  return (
    <button
      type={type}
      disabled={disabled}
      className={`${baseStyles} ${variantStyles[variant]} ${disabled ? 'opacity-50 cursor-not-allowed' : ''} ${className}`}
      {...props}
    >
      {children}
    </button>
  );
};

export default FuturisticButton;
