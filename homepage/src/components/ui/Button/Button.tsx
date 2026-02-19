/**
 * Button Component
 * Owner: Scenario 1 - Hero Section & Value Proposition
 *
 * Reusable button component with primary and secondary variants.
 * Supports both button and link (anchor) elements.
 */
import { cn } from '@/utils/cn'
import type { ButtonProps } from '@/types'
import styles from './Button.module.css'

export function Button({
  variant = 'primary',
  children,
  href,
  onClick,
  target,
  rel,
  className,
  'aria-label': ariaLabel,
}: ButtonProps) {
  const combinedClassName = cn(
    styles.button,
    styles[variant],
    className
  )

  if (href) {
    return (
      <a
        href={href}
        className={combinedClassName}
        target={target}
        rel={rel || (target === '_blank' ? 'noopener noreferrer' : undefined)}
        aria-label={ariaLabel}
      >
        {children}
      </a>
    )
  }

  return (
    <button
      type="button"
      className={combinedClassName}
      onClick={onClick}
      aria-label={ariaLabel}
    >
      {children}
    </button>
  )
}
