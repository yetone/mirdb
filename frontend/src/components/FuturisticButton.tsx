import { ReactNode } from 'react'
import { Link } from 'react-router-dom'

interface FuturisticButtonProps {
  children: ReactNode
  to?: string
  onClick?: () => void
  variant?: 'primary' | 'secondary' | 'ghost' | 'outline'
  size?: 'sm' | 'md' | 'lg'
  className?: string
  'data-testid'?: string
}

export default function FuturisticButton({
  children,
  to,
  onClick,
  variant = 'primary',
  size = 'md',
  className = '',
  'data-testid': testId
}: FuturisticButtonProps) {
  const baseClasses = 'btn relative overflow-hidden transition-all duration-300 hover:scale-105 hover:shadow-lg'
  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary',
    ghost: 'btn-ghost',
    outline: 'btn-outline'
  }
  const sizeClasses = {
    sm: 'btn-sm',
    md: 'btn-md',
    lg: 'btn-lg'
  }

  const classes = `${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`

  if (to) {
    return (
      <Link to={to} className={classes} data-testid={testId}>
        {children}
      </Link>
    )
  }

  return (
    <button onClick={onClick} className={classes} data-testid={testId}>
      {children}
    </button>
  )
}
