import { motion } from 'framer-motion'
import { ReactNode } from 'react'

interface FuturisticButtonProps {
  children: ReactNode
  onClick?: () => void
  variant?: 'primary' | 'secondary' | 'outline'
  className?: string
  type?: 'button' | 'submit' | 'reset'
  disabled?: boolean
}

export default function FuturisticButton({
  children,
  onClick,
  variant = 'primary',
  className = '',
  type = 'button',
  disabled = false,
}: FuturisticButtonProps) {
  const baseStyles = 'btn relative overflow-hidden font-semibold px-6 py-3 rounded-lg transition-all duration-300'

  const variantStyles = {
    primary: 'btn-primary bg-gradient-to-r from-primary to-secondary hover:shadow-lg hover:shadow-primary/50',
    secondary: 'btn-secondary bg-gradient-to-r from-secondary to-accent hover:shadow-lg hover:shadow-secondary/50',
    outline: 'btn-outline border-2 hover:bg-base-content/10',
  }

  return (
    <motion.button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={`${baseStyles} ${variantStyles[variant]} ${className}`}
      whileHover={{ scale: 1.05 }}
      whileTap={{ scale: 0.95 }}
    >
      {children}
    </motion.button>
  )
}
