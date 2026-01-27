import { ButtonHTMLAttributes, ReactNode } from 'react'
import { motion } from 'framer-motion'
import clsx from 'clsx'

interface FuturisticButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode
  variant?: 'primary' | 'secondary' | 'outline'
  size?: 'sm' | 'md' | 'lg'
}

export default function FuturisticButton({
  children,
  variant = 'primary',
  size = 'md',
  className,
  ...props
}: FuturisticButtonProps) {
  const baseClasses = 'btn font-semibold transition-all duration-300'

  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary',
    outline: 'btn-outline',
  }

  const sizeClasses = {
    sm: 'btn-sm',
    md: 'btn-md',
    lg: 'btn-lg',
  }

  return (
    <motion.button
      whileHover={{ scale: 1.05 }}
      whileTap={{ scale: 0.95 }}
      className={clsx(baseClasses, variantClasses[variant], sizeClasses[size], className)}
      {...props}
    >
      {children}
    </motion.button>
  )
}
