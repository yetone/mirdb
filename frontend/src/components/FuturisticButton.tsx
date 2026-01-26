import { ReactNode, MouseEventHandler } from 'react'
import { motion, HTMLMotionProps } from 'framer-motion'

interface FuturisticButtonProps extends Omit<HTMLMotionProps<'button'>, 'children'> {
  children: ReactNode
  variant?: 'primary' | 'secondary' | 'outline'
  size?: 'sm' | 'md' | 'lg'
  onClick?: MouseEventHandler<HTMLButtonElement>
}

export function FuturisticButton({
  children,
  variant = 'primary',
  size = 'md',
  className = '',
  onClick,
  ...props
}: FuturisticButtonProps) {
  const baseClasses = 'btn font-semibold transition-all duration-300'

  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary',
    outline: 'btn-outline',
  }

  const sizeClasses = {
    sm: 'btn-sm min-h-[44px] min-w-[44px]',
    md: 'btn-md min-h-[44px] min-w-[44px]',
    lg: 'btn-lg min-h-[44px] min-w-[44px]',
  }

  return (
    <motion.button
      whileHover={{ scale: 1.05 }}
      whileTap={{ scale: 0.95 }}
      className={`${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`}
      onClick={onClick}
      {...props}
    >
      {children}
    </motion.button>
  )
}
