import { ReactNode } from 'react'
import { motion } from 'framer-motion'

interface FuturisticButtonProps {
  children: ReactNode
  onClick?: () => void
  variant?: 'primary' | 'secondary' | 'ghost'
  className?: string
  type?: 'button' | 'submit' | 'reset'
}

export function FuturisticButton({
  children,
  onClick,
  variant = 'primary',
  className = '',
  type = 'button',
}: FuturisticButtonProps) {
  const baseStyles = 'btn font-semibold transition-all duration-300'
  const variantStyles = {
    primary: 'btn-primary',
    secondary: 'btn-secondary',
    ghost: 'btn-ghost',
  }

  return (
    <motion.button
      type={type}
      onClick={onClick}
      className={`${baseStyles} ${variantStyles[variant]} ${className}`}
      whileHover={{ scale: 1.05 }}
      whileTap={{ scale: 0.95 }}
    >
      {children}
    </motion.button>
  )
}

export default FuturisticButton
