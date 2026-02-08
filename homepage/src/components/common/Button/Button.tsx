/**
 * Reusable Button Component
 * Owner: Scenario 5 - Navigation and Links
 *
 * Features:
 * - Primary and secondary variants
 * - Link button (as anchor tag)
 * - Hover and focus states
 * - Accessible (proper ARIA attributes)
 */

import React from 'react'
import './Button.css'

export interface ButtonProps {
  /** The content to display inside the button */
  children: React.ReactNode
  /** Visual variant of the button */
  variant?: 'primary' | 'secondary'
  /** If provided, renders as an anchor tag */
  href?: string
  /** Target attribute for anchor links */
  target?: '_blank' | '_self' | '_parent' | '_top'
  /** Rel attribute for anchor links (auto-set for external links) */
  rel?: string
  /** Click handler for button variant */
  onClick?: (event: React.MouseEvent) => void
  /** Disabled state */
  disabled?: boolean
  /** Additional CSS class names */
  className?: string
  /** Accessible label */
  'aria-label'?: string
  /** Type attribute for button element */
  type?: 'button' | 'submit' | 'reset'
}

export function Button({
  children,
  variant = 'primary',
  href,
  target,
  rel,
  onClick,
  disabled = false,
  className = '',
  'aria-label': ariaLabel,
  type = 'button',
}: ButtonProps) {
  const baseClass = 'btn'
  const variantClass = `btn--${variant}`
  const combinedClassName = `${baseClass} ${variantClass} ${className}`.trim()

  // For external links, ensure proper security attributes
  const computedRel =
    rel ?? (target === '_blank' ? 'noopener noreferrer' : undefined)

  // Render as anchor if href is provided
  if (href) {
    return (
      <a
        href={href}
        className={combinedClassName}
        target={target}
        rel={computedRel}
        aria-label={ariaLabel}
        aria-disabled={disabled || undefined}
        tabIndex={disabled ? -1 : undefined}
        onClick={disabled ? (e) => e.preventDefault() : onClick}
      >
        {children}
      </a>
    )
  }

  // Render as button
  return (
    <button
      type={type}
      className={combinedClassName}
      onClick={onClick}
      disabled={disabled}
      aria-label={ariaLabel}
    >
      {children}
    </button>
  )
}

export default Button
