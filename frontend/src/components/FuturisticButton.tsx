import { motion, type HTMLMotionProps } from 'framer-motion'
import { forwardRef } from 'react'

export interface FuturisticButtonProps
  extends Omit<HTMLMotionProps<'button'>, 'children'> {
  children: React.ReactNode
  variant?: 'primary' | 'secondary' | 'ghost'
  size?: 'sm' | 'md' | 'lg'
  isLoading?: boolean
}

export const FuturisticButton = forwardRef<
  HTMLButtonElement,
  FuturisticButtonProps
>(
  (
    {
      children,
      variant = 'primary',
      size = 'md',
      isLoading = false,
      className = '',
      disabled,
      ...props
    },
    ref
  ) => {
    const baseClasses = 'btn font-semibold transition-all duration-300'

    const variantClasses = {
      primary: 'btn-primary text-primary-content',
      secondary: 'btn-secondary text-secondary-content',
      ghost: 'btn-ghost',
    }

    const sizeClasses = {
      sm: 'btn-sm',
      md: 'btn-md',
      lg: 'btn-lg text-lg px-8',
    }

    return (
      <motion.button
        ref={ref}
        className={`${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`}
        disabled={disabled || isLoading}
        whileHover={{ scale: 1.05 }}
        whileTap={{ scale: 0.95 }}
        {...props}
      >
        {isLoading ? (
          <span className="loading loading-spinner loading-sm"></span>
        ) : (
          children
        )}
      </motion.button>
    )
  }
)

FuturisticButton.displayName = 'FuturisticButton'
