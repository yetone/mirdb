import React from 'react';
import { Link } from 'react-router-dom';
import { motion } from 'framer-motion';

interface FuturisticButtonProps {
  children: React.ReactNode;
  to?: string;
  onClick?: () => void;
  variant?: 'primary' | 'secondary' | 'outline';
  className?: string;
  type?: 'button' | 'submit';
  'data-testid'?: string;
}

const FuturisticButton: React.FC<FuturisticButtonProps> = ({
  children,
  to,
  onClick,
  variant = 'primary',
  className = '',
  type = 'button',
  'data-testid': testId,
}) => {
  const baseClasses = 'btn relative overflow-hidden transition-all duration-300';
  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary',
    outline: 'btn-outline',
  };

  const buttonClasses = `${baseClasses} ${variantClasses[variant]} ${className}`;

  const MotionComponent = motion.button;

  if (to) {
    return (
      <Link to={to} data-testid={testId}>
        <motion.span
          className={buttonClasses}
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.95 }}
          tabIndex={0}
        >
          {children}
        </motion.span>
      </Link>
    );
  }

  return (
    <MotionComponent
      type={type}
      onClick={onClick}
      className={buttonClasses}
      whileHover={{ scale: 1.05 }}
      whileTap={{ scale: 0.95 }}
      data-testid={testId}
    >
      {children}
    </MotionComponent>
  );
};

export default FuturisticButton;
