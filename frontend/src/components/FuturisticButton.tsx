import React, { ButtonHTMLAttributes, ReactNode, AnchorHTMLAttributes } from 'react'
import { Link } from 'react-router-dom'

type BaseProps = {
  children: ReactNode
  variant?: 'primary' | 'secondary' | 'ghost'
  size?: 'sm' | 'md' | 'lg'
  className?: string
}

type ButtonProps = BaseProps & {
  href?: never
} & ButtonHTMLAttributes<HTMLButtonElement>

type LinkProps = BaseProps & {
  href: string
} & Omit<AnchorHTMLAttributes<HTMLAnchorElement>, 'href'>

type FuturisticButtonProps = ButtonProps | LinkProps

export function FuturisticButton({
  children,
  variant = 'primary',
  size = 'md',
  className = '',
  ...props
}: FuturisticButtonProps) {
  const baseClasses = 'btn transition-all duration-300 transform hover:scale-105'

  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary btn-outline',
    ghost: 'btn-ghost',
  }

  const sizeClasses = {
    sm: 'btn-sm',
    md: 'btn-md',
    lg: 'btn-lg',
  }

  const combinedClasses = `${baseClasses} ${variantClasses[variant]} ${sizeClasses[size]} ${className}`

  if ('href' in props && props.href) {
    const { href, ...linkProps } = props as LinkProps
    return (
      <Link to={href} className={combinedClasses} {...linkProps}>
        {children}
      </Link>
    )
  }

  const buttonProps = props as ButtonProps
  return (
    <button className={combinedClasses} {...buttonProps}>
      {children}
    </button>
  )
}

export default FuturisticButton
