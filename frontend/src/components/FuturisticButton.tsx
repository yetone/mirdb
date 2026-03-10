/**
 * Futuristic Button Component
 * Owner: Scenario 12 - Visual Effects and Animations
 *
 * A modern button component with smooth hover animations, glow effects,
 * and futuristic styling. Respects reduced motion preferences.
 */

import { type ReactNode, type MouseEventHandler } from 'react';
import { motion } from 'framer-motion';
import { useReducedMotion } from '../hooks/useReducedMotion';

type ButtonVariant = 'primary' | 'secondary' | 'outline' | 'ghost';
type ButtonSize = 'sm' | 'md' | 'lg';

interface FuturisticButtonProps {
  children: ReactNode;
  variant?: ButtonVariant;
  size?: ButtonSize;
  className?: string;
  isLoading?: boolean;
  leftIcon?: ReactNode;
  rightIcon?: ReactNode;
  disabled?: boolean;
  type?: 'button' | 'submit' | 'reset';
  onClick?: MouseEventHandler<HTMLButtonElement>;
}

const variantClasses: Record<ButtonVariant, string> = {
  primary:
    'bg-primary text-primary-content hover:bg-primary/90 border-primary shadow-lg shadow-primary/25',
  secondary:
    'bg-secondary text-secondary-content hover:bg-secondary/90 border-secondary shadow-lg shadow-secondary/25',
  outline:
    'bg-transparent border-2 border-primary text-primary hover:bg-primary/10',
  ghost: 'bg-transparent text-base-content hover:bg-base-content/10 border-transparent',
};

const sizeClasses: Record<ButtonSize, string> = {
  sm: 'px-3 py-1.5 text-sm min-h-8',
  md: 'px-5 py-2.5 text-base min-h-10',
  lg: 'px-7 py-3.5 text-lg min-h-12',
};

/**
 * FuturisticButton with animated hover effects.
 * Falls back to CSS transitions when reduced motion is preferred.
 */
export function FuturisticButton({
  children,
  variant = 'primary',
  size = 'md',
  className = '',
  isLoading = false,
  leftIcon,
  rightIcon,
  disabled,
  type = 'button',
  onClick,
}: FuturisticButtonProps) {
  const prefersReducedMotion = useReducedMotion();

  const baseClasses = `
    relative inline-flex items-center justify-center gap-2
    font-medium rounded-lg border
    transition-colors duration-200
    focus:outline-none focus:ring-2 focus:ring-primary/50 focus:ring-offset-2 focus:ring-offset-base-100
    disabled:opacity-50 disabled:cursor-not-allowed
  `;

  // Animation variants for hover effects
  const buttonVariants = {
    initial: {
      scale: 1,
      boxShadow: '0 4px 15px rgba(0, 0, 0, 0.1)',
    },
    hover: prefersReducedMotion
      ? {}
      : {
          scale: 1.02,
          boxShadow: '0 8px 25px rgba(0, 0, 0, 0.2)',
        },
    tap: prefersReducedMotion
      ? {}
      : {
          scale: 0.98,
        },
  };

  return (
    <motion.button
      type={type}
      className={`${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`}
      data-testid="futuristic-button"
      data-variant={variant}
      data-size={size}
      data-reduced-motion={prefersReducedMotion ? 'true' : 'false'}
      variants={buttonVariants}
      initial="initial"
      whileHover={!disabled && !isLoading ? 'hover' : undefined}
      whileTap={!disabled && !isLoading ? 'tap' : undefined}
      transition={{
        type: 'spring',
        stiffness: 400,
        damping: 20,
      }}
      disabled={disabled || isLoading}
      onClick={onClick}
    >
      {/* Glow effect overlay */}
      <motion.span
        className="absolute inset-0 rounded-lg bg-gradient-to-r from-transparent via-white/10 to-transparent"
        initial={{ x: '-100%', opacity: 0 }}
        whileHover={
          !prefersReducedMotion && !disabled && !isLoading
            ? {
                x: '100%',
                opacity: 1,
                transition: {
                  duration: 0.5,
                  ease: 'easeInOut',
                },
              }
            : {}
        }
        aria-hidden="true"
      />

      {/* Loading spinner */}
      {isLoading && (
        <span
          className="absolute inset-0 flex items-center justify-center"
          data-testid="button-loading"
        >
          <svg
            className="animate-spin h-5 w-5"
            xmlns="http://www.w3.org/2000/svg"
            fill="none"
            viewBox="0 0 24 24"
            aria-hidden="true"
          >
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
            />
          </svg>
        </span>
      )}

      {/* Button content */}
      <span
        className={`relative z-10 flex items-center gap-2 ${isLoading ? 'opacity-0' : ''}`}
      >
        {leftIcon && <span className="flex-shrink-0">{leftIcon}</span>}
        {children}
        {rightIcon && <span className="flex-shrink-0">{rightIcon}</span>}
      </span>
    </motion.button>
  );
}

export default FuturisticButton;
