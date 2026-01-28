import { ReactNode, ButtonHTMLAttributes } from 'react'
import { motion } from 'framer-motion'

interface FuturisticButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode
  variant?: 'primary' | 'secondary' | 'ghost'
  size?: 'sm' | 'md' | 'lg'
}

export function FuturisticButton({
  children,
  variant = 'primary',
  size = 'md',
  className = '',
  ...props
}: FuturisticButtonProps) {
  const baseClasses = 'btn relative overflow-hidden transition-all duration-300'

  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary',
    ghost: 'btn-ghost',
  }

  const sizeClasses = {
    sm: 'btn-sm',
    md: 'btn-md',
    lg: 'btn-lg',
  }

  return (
    <motion.button
      className={`${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`}
      whileHover={{ scale: 1.02 }}
      whileTap={{ scale: 0.98 }}
      {...props}
    >
      {children}
    </motion.button>
  )
}
