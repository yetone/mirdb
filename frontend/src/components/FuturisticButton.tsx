import { Link } from 'react-router-dom'
import { clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'
import { motion } from 'framer-motion'
import { ReactNode } from 'react'

type ButtonVariant = 'primary' | 'secondary' | 'outline' | 'ghost'
type ButtonSize = 'sm' | 'md' | 'lg'

interface BaseButtonProps {
  variant?: ButtonVariant
  size?: ButtonSize
  children: ReactNode
  className?: string
  'data-testid'?: string
}

interface ButtonAsButton extends BaseButtonProps {
  as?: 'button'
  type?: 'button' | 'submit' | 'reset'
  onClick?: () => void
  disabled?: boolean
}

interface ButtonAsLink extends BaseButtonProps {
  as: 'link'
  to: string
}

type FuturisticButtonProps = ButtonAsButton | ButtonAsLink

const variantStyles: Record<ButtonVariant, string> = {
  primary:
    'bg-primary text-primary-content hover:bg-primary/80 shadow-lg hover:shadow-primary/50 border-transparent',
  secondary:
    'bg-secondary text-secondary-content hover:bg-secondary/80 shadow-lg hover:shadow-secondary/50 border-transparent',
  outline:
    'bg-transparent text-primary border-primary hover:bg-primary hover:text-primary-content',
  ghost:
    'bg-transparent text-base-content hover:bg-base-200 border-transparent',
}

const sizeStyles: Record<ButtonSize, string> = {
  sm: 'px-4 py-2 text-sm',
  md: 'px-6 py-3 text-base',
  lg: 'px-8 py-4 text-lg',
}

export default function FuturisticButton(props: FuturisticButtonProps) {
  const {
    variant = 'primary',
    size = 'md',
    children,
    className,
    'data-testid': testId,
  } = props

  const baseStyles = twMerge(
    clsx(
      'inline-flex items-center justify-center font-semibold rounded-lg border-2 transition-all duration-300 focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-2',
      variantStyles[variant],
      sizeStyles[size],
      className
    )
  )

  if (props.as === 'link') {
    return (
      <motion.div
        whileHover={{ scale: 1.02 }}
        whileTap={{ scale: 0.98 }}
      >
        <Link
          to={props.to}
          className={baseStyles}
          data-testid={testId}
        >
          {children}
        </Link>
      </motion.div>
    )
  }

  const { type = 'button', onClick, disabled } = props as ButtonAsButton

  return (
    <motion.button
      whileHover={{ scale: disabled ? 1 : 1.02 }}
      whileTap={{ scale: disabled ? 1 : 0.98 }}
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={twMerge(baseStyles, disabled && 'opacity-50 cursor-not-allowed')}
      data-testid={testId}
    >
      {children}
    </motion.button>
  )
}
