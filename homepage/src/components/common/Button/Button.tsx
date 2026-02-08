/**
 * Reusable Button Component
 * Owner: Scenario 5 - Navigation and Links
 *
 * Expected exports:
 * - Button: React.FC<ButtonProps> - Reusable button component
 * - ButtonProps: interface
 *
 * Features:
 * - Primary and secondary variants
 * - Link button (as anchor tag)
 * - Hover and focus states
 * - Accessible (proper ARIA attributes)
 */

export interface ButtonProps {
  children: React.ReactNode
  variant?: 'primary' | 'secondary'
  href?: string
  onClick?: () => void
  className?: string
}

export function Button({ children, variant = 'primary', href, onClick, className }: ButtonProps) {
  // Implementation by Scenario 5
  if (href) {
    return (
      <a href={href} className={className}>
        {children}
      </a>
    )
  }
  return (
    <button onClick={onClick} className={className}>
      {children}
    </button>
  )
}
