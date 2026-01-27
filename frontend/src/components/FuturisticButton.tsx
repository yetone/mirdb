import { ReactNode, CSSProperties } from 'react'
import { motion } from 'framer-motion'

interface FuturisticButtonProps {
  children: ReactNode
  onClick?: () => void
  variant?: 'primary' | 'secondary' | 'ghost'
  className?: string
  type?: 'button' | 'submit' | 'reset'
}

/**
 * Accessible button styles that meet WCAG 2.1 AA color contrast requirements (4.5:1 minimum)
 * These inline styles override DaisyUI's default button colors which don't meet accessibility standards
 */
const accessibleStyles: Record<string, CSSProperties> = {
  primary: {
    backgroundColor: '#5b21b6', // Deep purple - provides 7.5:1 contrast with white
    color: '#ffffff',
    borderColor: '#5b21b6',
  },
  secondary: {
    backgroundColor: '#0f766e', // Teal - provides 4.5:1+ contrast with white
    color: '#ffffff',
    borderColor: '#0f766e',
  },
  ghost: {
    backgroundColor: 'transparent',
    color: '#1f2937', // Dark gray - provides 12.6:1 contrast with white background
    borderColor: 'transparent',
  },
}

export function FuturisticButton({
  children,
  onClick,
  variant = 'primary',
  className = '',
  type = 'button',
}: FuturisticButtonProps) {
  const baseStyles = 'btn font-semibold transition-all duration-300 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary'
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
      style={accessibleStyles[variant]}
      whileHover={{ scale: 1.05 }}
      whileTap={{ scale: 0.95 }}
    >
      {children}
    </motion.button>
  )
}

export default FuturisticButton
