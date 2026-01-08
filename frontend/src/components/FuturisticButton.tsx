import React from 'react'
import clsx from 'clsx'

interface FuturisticButtonProps {
  children: React.ReactNode
  variant?: 'primary' | 'secondary' | 'outline'
  size?: 'sm' | 'md' | 'lg'
  onClick?: () => void
  type?: 'button' | 'submit' | 'reset'
  disabled?: boolean
  className?: string
}

export default function FuturisticButton({
  children,
  variant = 'primary',
  size = 'md',
  onClick,
  type = 'button',
  disabled = false,
  className,
}: FuturisticButtonProps) {
  const baseClasses = 'btn transition-all duration-300 font-semibold'

  const variantClasses = {
    primary: 'btn-primary hover:brightness-110',
    secondary: 'btn-secondary hover:brightness-110',
    outline: 'btn-outline hover:brightness-110',
  }

  const sizeClasses = {
    sm: 'btn-sm',
    md: 'btn-md',
    lg: 'btn-lg',
  }

  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={clsx(
        baseClasses,
        variantClasses[variant],
        sizeClasses[size],
        disabled && 'btn-disabled',
        className
      )}
    >
      {children}
    </button>
  )
}
