/**
 * FuturisticButton Component
 *
 * A styled button component with futuristic visual styling.
 * Used for CTAs throughout the application.
 */
import { motion } from 'framer-motion'
import { ComponentPropsWithoutRef, forwardRef } from 'react'

export interface FuturisticButtonProps extends ComponentPropsWithoutRef<'button'> {
  variant?: 'primary' | 'secondary' | 'outline'
  size?: 'sm' | 'md' | 'lg'
  href?: string
}

export const FuturisticButton = forwardRef<HTMLButtonElement, FuturisticButtonProps>(
  ({ children, variant = 'primary', size = 'md', className = '', href, onClick, ...props }, ref) => {
    const baseStyles = 'btn font-bold tracking-wider transition-all duration-300 ease-out'

    const variantStyles = {
      primary: 'btn-primary bg-gradient-to-r from-primary to-secondary text-primary-content hover:shadow-lg hover:shadow-primary/50',
      secondary: 'btn-secondary',
      outline: 'btn-outline border-2 hover:border-primary hover:bg-primary/10',
    }

    const sizeStyles = {
      sm: 'btn-sm text-sm px-4 py-2',
      md: 'btn-md text-base px-6 py-3',
      lg: 'btn-lg text-lg px-8 py-4',
    }

    const combinedClassName = `${baseStyles} ${variantStyles[variant]} ${sizeStyles[size]} ${className}`

    const handleClick = (e: React.MouseEvent<HTMLButtonElement>) => {
      if (href) {
        window.location.href = href
      }
      onClick?.(e)
    }

    return (
      <motion.button
        ref={ref}
        className={combinedClassName}
        onClick={handleClick}
        whileHover={{ scale: 1.05 }}
        whileTap={{ scale: 0.95 }}
        {...props}
      >
        {children}
      </motion.button>
    )
  }
)

FuturisticButton.displayName = 'FuturisticButton'
