import { ReactNode } from 'react'
import { Link } from 'react-router-dom'

interface FuturisticButtonProps {
  children: ReactNode
  to?: string
  onClick?: () => void
  variant?: 'primary' | 'secondary' | 'ghost'
  className?: string
}

export default function FuturisticButton({
  children,
  to,
  onClick,
  variant = 'primary',
  className = ''
}: FuturisticButtonProps) {
  const baseClasses = 'btn relative overflow-hidden transition-all duration-300 hover:scale-105 hover:shadow-lg'
  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary',
    ghost: 'btn-ghost'
  }

  const classes = `${baseClasses} ${variantClasses[variant]} ${className}`

  if (to) {
    return (
      <Link to={to} className={classes}>
        {children}
      </Link>
    )
  }

  return (
    <button onClick={onClick} className={classes}>
      {children}
    </button>
  )
}
