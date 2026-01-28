import { ReactNode } from 'react';
import { motion } from 'framer-motion';
import { cn } from '../utils';

interface FuturisticButtonProps {
  children: ReactNode;
  onClick?: () => void;
  variant?: 'primary' | 'secondary' | 'ghost';
  size?: 'sm' | 'md' | 'lg';
  className?: string;
  type?: 'button' | 'submit' | 'reset';
  disabled?: boolean;
}

export const FuturisticButton = ({
  children,
  onClick,
  variant = 'primary',
  size = 'md',
  className,
  type = 'button',
  disabled = false,
}: FuturisticButtonProps) => {
  const baseStyles = 'btn relative overflow-hidden transition-all duration-300';

  const variantStyles = {
    primary: 'btn-primary hover:shadow-lg hover:shadow-primary/50',
    secondary: 'btn-secondary hover:shadow-lg hover:shadow-secondary/50',
    ghost: 'btn-ghost hover:bg-base-200',
  };

  const sizeStyles = {
    sm: 'btn-sm',
    md: '',
    lg: 'btn-lg',
  };

  return (
    <motion.button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={cn(baseStyles, variantStyles[variant], sizeStyles[size], className)}
      whileHover={{ scale: 1.02 }}
      whileTap={{ scale: 0.98 }}
    >
      {children}
    </motion.button>
  );
};

export default FuturisticButton;
