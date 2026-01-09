import type { ButtonHTMLAttributes } from 'react'
import { forwardRef } from 'react'
import type { LinkProps } from 'react-router-dom'
import { Link } from 'react-router-dom'

interface BaseButtonProps {
  variant?: 'primary' | 'secondary' | 'outline'
  size?: 'sm' | 'md' | 'lg'
  children: React.ReactNode
  className?: string
}

type ButtonAsButton = BaseButtonProps &
  Omit<ButtonHTMLAttributes<HTMLButtonElement>, keyof BaseButtonProps> & {
    as?: 'button'
    to?: never
  }

type ButtonAsLink = BaseButtonProps &
  Omit<LinkProps, keyof BaseButtonProps> & {
    as: 'link'
    to: string
  }

export type FuturisticButtonProps = ButtonAsButton | ButtonAsLink

const getVariantClasses = (variant: BaseButtonProps['variant'] = 'primary') => {
  const variants = {
    primary:
      'btn-primary bg-gradient-to-r from-primary to-secondary text-primary-content hover:shadow-lg hover:shadow-primary/50 transition-all duration-300',
    secondary:
      'btn-secondary bg-gradient-to-r from-secondary to-accent text-secondary-content hover:shadow-lg hover:shadow-secondary/50 transition-all duration-300',
    outline:
      'btn-outline border-2 border-primary text-primary hover:bg-primary hover:text-primary-content transition-all duration-300',
  }
  return variants[variant]
}

const getSizeClasses = (size: BaseButtonProps['size'] = 'md') => {
  const sizes = {
    sm: 'btn-sm text-sm px-4 py-2',
    md: 'btn-md text-base px-6 py-3',
    lg: 'btn-lg text-lg px-8 py-4',
  }
  return sizes[size]
}

export const FuturisticButton = forwardRef<
  HTMLButtonElement | HTMLAnchorElement,
  FuturisticButtonProps
>((props, ref) => {
  const { variant = 'primary', size = 'md', children, className = '', ...rest } = props

  const baseClasses = 'btn font-semibold rounded-lg transform hover:scale-105'
  const variantClasses = getVariantClasses(variant)
  const sizeClasses = getSizeClasses(size)
  const combinedClasses = `${baseClasses} ${variantClasses} ${sizeClasses} ${className}`

  if (props.as === 'link') {
    const { as, to, ...linkRest } = rest as ButtonAsLink
    return (
      <Link
        ref={ref as React.Ref<HTMLAnchorElement>}
        to={to}
        className={combinedClasses}
        {...linkRest}
      >
        {children}
      </Link>
    )
  }

  const { as, ...buttonRest } = rest as ButtonAsButton
  return (
    <button
      ref={ref as React.Ref<HTMLButtonElement>}
      className={combinedClasses}
      {...buttonRest}
    >
      {children}
    </button>
  )
})

FuturisticButton.displayName = 'FuturisticButton'

export default FuturisticButton
