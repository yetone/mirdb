/**
 * FuturisticButton Component
 * Styled button component with futuristic appearance.
 * This is an existing component used by multiple scenarios.
 */
import React from 'react'

interface FuturisticButtonProps {
  children: React.ReactNode
  onClick?: () => void
  type?: 'button' | 'submit' | 'reset'
  variant?: 'primary' | 'secondary' | 'ghost'
  disabled?: boolean
  loading?: boolean
  className?: string
  'aria-label'?: string
}

const FuturisticButton: React.FC<FuturisticButtonProps> = ({
  children,
  onClick,
  type = 'button',
  variant = 'primary',
  disabled = false,
  loading = false,
  className = '',
  'aria-label': ariaLabel,
}) => {
  const baseClasses = 'btn relative overflow-hidden transition-all duration-300'

  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary btn-outline',
    ghost: 'btn-ghost',
  }

  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled || loading}
      className={`${baseClasses} ${variantClasses[variant]} ${className}`}
      aria-label={ariaLabel}
    >
      {loading ? (
        <span className="loading loading-spinner loading-sm" aria-hidden="true" />
      ) : (
        children
      )}
    </button>
  )
}

export default FuturisticButton
