import type { ReactNode } from 'react'
import { Link } from 'react-router-dom'

interface FuturisticButtonBaseProps {
  children: ReactNode
  variant?: 'primary' | 'secondary' | 'ghost'
  size?: 'sm' | 'md' | 'lg'
  className?: string
  'data-testid'?: string
}

// Button element props (for onClick handlers)
interface FuturisticButtonAsButton extends FuturisticButtonBaseProps {
  as?: 'button'
  to?: never
  onClick?: () => void
  type?: 'button' | 'submit' | 'reset'
  disabled?: boolean
}

// Link element props (for navigation)
interface FuturisticButtonAsLink extends FuturisticButtonBaseProps {
  as: 'link'
  to: string
  onClick?: never
  type?: never
  disabled?: never
}

type FuturisticButtonProps = FuturisticButtonAsButton | FuturisticButtonAsLink

const sizeClasses = {
  sm: 'btn-sm px-4 py-2 text-sm min-h-[36px]',
  md: 'px-6 py-3 text-base min-h-[44px]',
  lg: 'btn-lg px-8 py-4 text-lg min-h-[52px]',
}

const variantClasses = {
  primary: `
    bg-gradient-to-r from-primary to-secondary
    text-primary-content
    border-2 border-primary/50
    shadow-lg shadow-primary/25
    hover:shadow-xl hover:shadow-primary/40
    hover:border-primary
    hover:scale-105
    active:scale-95
  `,
  secondary: `
    bg-base-100/80
    text-base-content
    border-2 border-base-content/20
    shadow-md
    hover:bg-base-200
    hover:border-primary/50
    hover:shadow-lg
    hover:scale-105
    active:scale-95
  `,
  ghost: `
    bg-transparent
    text-base-content
    border-2 border-transparent
    hover:bg-base-content/10
    hover:border-base-content/20
    hover:scale-105
    active:scale-95
  `,
}

const baseClasses = `
  btn
  relative
  overflow-hidden
  font-bold
  rounded-xl
  transition-all
  duration-300
  ease-out
  backdrop-blur-sm
  focus:outline-none
  focus-visible:ring-2
  focus-visible:ring-primary
  focus-visible:ring-offset-2
  focus-visible:ring-offset-base-100
  motion-reduce:transition-none
  motion-reduce:hover:scale-100
  motion-reduce:active:scale-100
`

function FuturisticButton({
  children,
  variant = 'primary',
  size = 'md',
  className = '',
  as = 'button',
  ...props
}: FuturisticButtonProps) {
  const combinedClasses = `
    ${baseClasses}
    ${sizeClasses[size]}
    ${variantClasses[variant]}
    ${className}
  `.replace(/\s+/g, ' ').trim()

  if (as === 'link') {
    const { to, 'data-testid': dataTestId } = props as FuturisticButtonAsLink
    return (
      <Link
        to={to}
        className={combinedClasses}
        data-testid={dataTestId}
      >
        <span className="relative z-10">{children}</span>
        <span
          className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700 motion-reduce:hidden"
          aria-hidden="true"
        />
      </Link>
    )
  }

  const { onClick, type = 'button', disabled, 'data-testid': dataTestId } = props as FuturisticButtonAsButton
  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={`${combinedClasses} ${disabled ? 'opacity-50 cursor-not-allowed' : ''}`}
      data-testid={dataTestId}
    >
      <span className="relative z-10">{children}</span>
      <span
        className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700 motion-reduce:hidden"
        aria-hidden="true"
      />
    </button>
  )
}

export default FuturisticButton
