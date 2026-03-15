/**
 * External Link Component with security attributes.
 * Owner: Scenario 6 - GitHub Repository Integration
 *
 * Props:
 * - href: string - External URL
 * - children: ReactNode - Link content
 * - showIcon?: boolean - Show external link indicator (default: true)
 * - className?: string - Additional CSS classes
 * - aria-label?: string - Accessible label for the link
 *
 * Features:
 * - Automatically adds target="_blank"
 * - Adds rel="noopener noreferrer" for security
 * - Optional external link icon indicator
 *
 * Requirements:
 * - REQ-5: GitHub repository links
 * - US-3: External links open in new tab with indicator
 */

import { ExternalLink as ExternalLinkIcon } from 'lucide-react'
import type { ReactNode } from 'react'

export interface ExternalLinkProps {
  href: string
  children: ReactNode
  showIcon?: boolean
  className?: string
  'aria-label'?: string
}

/**
 * A link component that opens in a new tab with proper security attributes.
 * Includes an optional external link indicator icon.
 */
export function ExternalLink({
  href,
  children,
  showIcon = true,
  className = '',
  'aria-label': ariaLabel,
}: ExternalLinkProps) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className={`inline-flex items-center gap-1 ${className}`}
      aria-label={ariaLabel}
    >
      {children}
      {showIcon && (
        <ExternalLinkIcon
          size={14}
          className="inline-block flex-shrink-0"
          aria-hidden="true"
          data-testid="external-link-icon"
        />
      )}
    </a>
  )
}

export default ExternalLink
