import { ReactNode, ButtonHTMLAttributes } from 'react'
import { Link, LinkProps } from 'react-router-dom'

type ButtonVariant = 'primary' | 'secondary' | 'outline' | 'ghost'
type ButtonSize = 'sm' | 'md' | 'lg'

interface BaseButtonProps {
  variant?: ButtonVariant
  size?: ButtonSize
  children: ReactNode
  className?: string
  'data-testid'?: string
}

interface ButtonAsButtonProps
  extends BaseButtonProps,
    Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'children' | 'className'> {
  as?: 'button'
  to?: never
}

interface ButtonAsLinkProps extends BaseButtonProps, Omit<LinkProps, 'children' | 'className'> {
  as: 'link'
}

type FuturisticButtonProps = ButtonAsButtonProps | ButtonAsLinkProps

const variantClasses: Record<ButtonVariant, string> = {
  primary: 'btn-primary',
  secondary: 'btn-secondary',
  outline: 'btn-outline',
  ghost: 'btn-ghost',
}

const sizeClasses: Record<ButtonSize, string> = {
  sm: 'btn-sm',
  md: '',
  lg: 'btn-lg',
}

/**
 * FuturisticButton - A consistent button component for the homepage
 * NFR-4 compliant design consistency component
 * Supports both button and Link variants
 */
const FuturisticButton = (props: FuturisticButtonProps) => {
  const {
    variant = 'primary',
    size = 'md',
    children,
    className = '',
    'data-testid': testId,
  } = props

  const baseClasses = `
    btn
    ${variantClasses[variant]}
    ${sizeClasses[size]}
    transition-all
    duration-200
    ${className}
  `.replace(/\s+/g, ' ').trim()

  if (props.as === 'link') {
    const { as: _, variant: __, size: ___, ...linkProps } = props
    return (
      <Link
        {...linkProps}
        className={baseClasses}
        data-testid={testId}
      >
        {children}
      </Link>
    )
  }

  const { as: _, variant: __, size: ___, ...buttonProps } = props
  return (
    <button
      {...buttonProps}
      className={baseClasses}
      data-testid={testId}
    >
      {children}
    </button>
  )
}

export default FuturisticButton
