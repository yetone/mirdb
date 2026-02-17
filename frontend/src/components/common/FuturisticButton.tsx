/**
 * Styled button component with futuristic design.
 * Owner: Scenario 13 - Component Integration
 *
 * Features:
 * - Hover state animations
 * - Multiple variants (primary, secondary, outline)
 * - Loading state support
 * - Keyboard accessibility
 */

import React from 'react'
import { motion } from 'framer-motion'

export interface FuturisticButtonProps {
  variant?: 'primary' | 'secondary' | 'outline'
  loading?: boolean
  disabled?: boolean
  onClick?: () => void
  children: React.ReactNode
  className?: string
  type?: 'button' | 'submit' | 'reset'
  'aria-label'?: string
  'data-testid'?: string
}

export function FuturisticButton({
  variant = 'primary',
  loading = false,
  disabled = false,
  onClick,
  children,
  className = '',
  type = 'button',
  'aria-label': ariaLabel,
  'data-testid': testId,
}: FuturisticButtonProps) {
  const baseClasses = 'btn font-semibold transition-all duration-200 min-h-[44px] min-w-[44px]'

  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary',
    outline: 'btn-outline',
  }

  return (
    <motion.button
      type={type}
      onClick={onClick}
      disabled={disabled || loading}
      className={`${baseClasses} ${variantClasses[variant]} ${className}`}
      whileHover={{ scale: disabled ? 1 : 1.02 }}
      whileTap={{ scale: disabled ? 1 : 0.98 }}
      aria-label={ariaLabel}
      aria-busy={loading}
      data-testid={testId}
    >
      {loading ? (
        <span className="loading loading-spinner loading-sm" aria-hidden="true"></span>
      ) : (
        children
      )}
    </motion.button>
  )
}
