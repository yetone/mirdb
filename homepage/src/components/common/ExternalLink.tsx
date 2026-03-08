/**
 * External Link component for MirDB homepage.
 * Owner: Scenario 6 - External Links and Resources
 *
 * This component renders external links with proper security
 * and accessibility attributes:
 * - Opens in new tab (target="_blank")
 * - Prevents security vulnerabilities (rel="noopener noreferrer")
 * - Accessible link text
 */

import React from 'react'
import type { ExternalLink as ExternalLinkType } from '../../types'

export interface ExternalLinkProps {
  /** The link data */
  link: ExternalLinkType
  /** Additional CSS classes */
  className?: string
  /** Optional children to override label */
  children?: React.ReactNode
  /** Show an external link icon */
  showIcon?: boolean
}

/**
 * ExternalLink component that renders links with proper security attributes.
 * All external links open in a new tab and include rel="noopener noreferrer"
 * for security.
 */
export const ExternalLink: React.FC<ExternalLinkProps> = ({
  link,
  className = '',
  children,
  showIcon = false,
}) => {
  return (
    <a
      href={link.url}
      target="_blank"
      rel="noopener noreferrer"
      className={`inline-flex items-center text-blue-400 hover:text-blue-300 transition-colors ${className}`}
      aria-label={`${link.label} (opens in new tab)`}
    >
      {children || link.label}
      {showIcon && (
        <svg
          className="ml-1 w-4 h-4"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
          />
        </svg>
      )}
    </a>
  )
}

/**
 * Props for simple external link without ExternalLink type
 */
export interface SimpleExternalLinkProps {
  /** The link URL */
  href: string
  /** Link text/label */
  children: React.ReactNode
  /** Additional CSS classes */
  className?: string
  /** Show an external link icon */
  showIcon?: boolean
}

/**
 * SimpleExternalLink for cases where you just need a URL and label
 * without the full ExternalLink type.
 */
export const SimpleExternalLink: React.FC<SimpleExternalLinkProps> = ({
  href,
  children,
  className = '',
  showIcon = false,
}) => {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className={`inline-flex items-center text-blue-400 hover:text-blue-300 transition-colors ${className}`}
    >
      {children}
      {showIcon && (
        <svg
          className="ml-1 w-4 h-4"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
          />
        </svg>
      )}
    </a>
  )
}
