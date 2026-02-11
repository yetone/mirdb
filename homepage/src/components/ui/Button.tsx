/**
 * Reusable button component with variants.
 */

import type { ButtonProps } from '../../types'
import { classNames } from '../../utils'
import './Button.css'

export function Button({
  variant = 'primary',
  size = 'md',
  href,
  external,
  onClick,
  children,
  className,
}: ButtonProps) {
  const classes = classNames(
    'button',
    `button--${variant}`,
    `button--${size}`,
    className
  )

  if (href) {
    return (
      <a
        href={href}
        className={classes}
        target={external ? '_blank' : undefined}
        rel={external ? 'noopener noreferrer' : undefined}
        onClick={onClick}
      >
        {children}
      </a>
    )
  }

  return (
    <button className={classes} onClick={onClick} type="button">
      {children}
    </button>
  )
}
