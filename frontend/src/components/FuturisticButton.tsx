import { ButtonHTMLAttributes, ReactNode } from 'react'
import { clsx } from 'clsx'

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
  const baseClasses = 'btn transition-all duration-300 hover:scale-105'

  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary',
    outline: 'btn-outline',
  }

  const sizeClasses = {
    sm: 'btn-sm',
    md: '',
    lg: 'btn-lg',
  }

  return (
    <button
      className={clsx(
        baseClasses,
        variantClasses[variant],
        sizeClasses[size],
        className
      )}
      {...props}
    >
      {children}
    </button>
  )
}
