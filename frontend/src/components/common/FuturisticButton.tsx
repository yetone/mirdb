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

import React, { ReactNode, ButtonHTMLAttributes } from 'react'

export interface FuturisticButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'outline'
  loading?: boolean
  children: ReactNode
}

export function FuturisticButton({
  variant = 'primary',
  loading = false,
  disabled,
  children,
  className = '',
  ...props
}: FuturisticButtonProps) {
  const baseStyles = `
    relative
    px-6 py-3
    font-semibold
    rounded-lg
    transition-all
    duration-300
    transform
    hover:scale-105
    active:scale-95
    disabled:opacity-50
    disabled:cursor-not-allowed
    disabled:transform-none
    focus:outline-none
    focus:ring-2
    focus:ring-offset-2
  `

  const variantStyles = {
    primary: `
      bg-primary
      text-primary-content
      hover:bg-primary-focus
      focus:ring-primary
    `,
    secondary: `
      bg-secondary
      text-secondary-content
      hover:bg-secondary-focus
      focus:ring-secondary
    `,
    outline: `
      border-2
      border-primary
      text-primary
      hover:bg-primary
      hover:text-primary-content
      focus:ring-primary
    `,
  }

  return (
    <button
      className={`${baseStyles} ${variantStyles[variant]} ${className}`}
      disabled={disabled || loading}
      {...props}
    >
      {loading ? (
        <span className="flex items-center gap-2">
          <span className="loading loading-spinner loading-sm" />
          Loading...
        </span>
      ) : (
        children
      )}
    </button>
  )
}
