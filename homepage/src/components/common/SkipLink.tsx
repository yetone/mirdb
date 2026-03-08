import React from 'react'

/**
 * Skip Link component for accessibility.
 *
 * This component provides a "Skip to main content" link
 * that becomes visible on keyboard focus for screen reader users.
 */
export const SkipLink: React.FC = () => {
  return (
    <a
      href="#main-content"
      className="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50 focus:px-4 focus:py-2 focus:bg-blue-600 focus:text-white focus:rounded-lg"
    >
      Skip to main content
    </a>
  )
}
