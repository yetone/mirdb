/**
 * FuturisticButton Component
 *
 * A stylized button component with futuristic design elements.
 * Supports multiple variants and can render as a button or link.
 *
 * Used across the homepage for CTAs.
 */

import React from 'react';
import { motion } from 'framer-motion';
import { Link } from 'react-router-dom';
import { cn } from '../utils/cn';

interface FuturisticButtonProps {
  children: React.ReactNode;
  href?: string;
  onClick?: () => void;
  variant?: 'primary' | 'secondary' | 'outline';
  size?: 'sm' | 'md' | 'lg';
  className?: string;
  type?: 'button' | 'submit' | 'reset';
  disabled?: boolean;
  'data-testid'?: string;
}

const FuturisticButton: React.FC<FuturisticButtonProps> = ({
  children,
  href,
  onClick,
  variant = 'primary',
  size = 'md',
  className = '',
  type = 'button',
  disabled = false,
  'data-testid': dataTestId,
}) => {
  const baseClasses = 'relative overflow-hidden font-semibold rounded-lg transition-all duration-300 transform hover:scale-105 focus:outline-none focus:ring-2 focus:ring-offset-2 inline-block';

  const variantClasses = {
    primary: 'bg-gradient-to-r from-purple-600 to-pink-600 text-white hover:shadow-lg hover:shadow-purple-500/50 focus:ring-purple-500',
    secondary: 'bg-slate-800 text-white hover:bg-slate-700 focus:ring-slate-500',
    outline: 'border-2 border-purple-500 text-purple-400 hover:bg-purple-500 hover:text-white focus:ring-purple-500',
  };

  const sizeClasses = {
    sm: 'px-4 py-2 text-sm',
    md: 'px-6 py-3 text-base',
    lg: 'px-8 py-4 text-lg',
  };

  const classes = cn(baseClasses, variantClasses[variant], sizeClasses[size], className);

  const content = (
    <>
      <span className="relative z-10">{children}</span>
      <motion.div
        className="absolute inset-0 bg-white/20"
        initial={{ x: '-100%', opacity: 0 }}
        whileHover={{ x: '100%', opacity: 1 }}
        transition={{ duration: 0.5 }}
      />
    </>
  );

  if (href) {
    return (
      <Link to={href} className={classes} data-testid={dataTestId}>
        {content}
      </Link>
    );
  }

  return (
    <motion.button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={classes}
      whileTap={{ scale: 0.95 }}
      data-testid={dataTestId}
    >
      {content}
    </motion.button>
  );
};

export default FuturisticButton;
