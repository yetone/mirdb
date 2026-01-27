import React from 'react'

/**
 * FuturisticButton component.
 * A styled button with hover effects for call-to-action elements.
 */
interface FuturisticButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  children: React.ReactNode
  variant?: 'primary' | 'secondary' | 'outline'
  size?: 'sm' | 'md' | 'lg'
}

export function FuturisticButton({
  children,
  variant = 'primary',
  size = 'md',
  className = '',
  ...props
}: FuturisticButtonProps) {
  const baseClasses = 'btn transition-all duration-300 hover:scale-105'

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
    <button
      className={`${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`}
      {...props}
    >
      {children}
    </button>
  )
}
