import { motion } from 'framer-motion'
import { clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

interface FuturisticButtonProps {
  children: React.ReactNode
  onClick?: () => void
  type?: 'button' | 'submit' | 'reset'
  variant?: 'primary' | 'secondary' | 'outline'
  size?: 'sm' | 'md' | 'lg'
  disabled?: boolean
  className?: string
  'data-testid'?: string
}

/**
 * FuturisticButton component with glassmorphism and animation effects
 * Implements the design system for the URL Shortening Service
 */
export function FuturisticButton({
  children,
  onClick,
  type = 'button',
  variant = 'primary',
  size = 'md',
  disabled = false,
  className,
  'data-testid': testId,
}: FuturisticButtonProps) {
  const baseStyles =
    'relative overflow-hidden font-semibold rounded-lg transition-all duration-300 focus:outline-none focus:ring-2 focus:ring-primary/50'

  const variantStyles = {
    primary:
      'bg-gradient-to-r from-primary to-secondary text-primary-content hover:shadow-lg hover:shadow-primary/25',
    secondary:
      'bg-gradient-to-r from-secondary to-accent text-secondary-content hover:shadow-lg hover:shadow-secondary/25',
    outline:
      'border-2 border-primary bg-transparent text-primary hover:bg-primary/10',
  }

  const sizeStyles = {
    sm: 'px-4 py-2 text-sm',
    md: 'px-6 py-3 text-base',
    lg: 'px-8 py-4 text-lg',
  }

  const disabledStyles = disabled
    ? 'opacity-50 cursor-not-allowed'
    : 'cursor-pointer'

  return (
    <motion.button
      type={type}
      onClick={disabled ? undefined : onClick}
      disabled={disabled}
      data-testid={testId}
      className={twMerge(
        clsx(
          baseStyles,
          variantStyles[variant],
          sizeStyles[size],
          disabledStyles,
          className
        )
      )}
      whileHover={disabled ? {} : { scale: 1.02 }}
      whileTap={disabled ? {} : { scale: 0.98 }}
    >
      {/* Glow effect */}
      <motion.div
        className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0"
        initial={{ x: '-100%' }}
        animate={disabled ? {} : { x: '200%' }}
        transition={{
          repeat: Infinity,
          repeatDelay: 3,
          duration: 1,
          ease: 'easeInOut',
        }}
      />
      <span className="relative z-10">{children}</span>
    </motion.button>
  )
}

export default FuturisticButton
