/**
 * GlassMorphismCard Component
 * Owner: Shared resource
 *
 * A reusable card component with glass morphism visual effect.
 * Used throughout the application for consistent card styling.
 */

import { ReactNode } from 'react'

export interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
  'data-testid'?: string
}

export function GlassMorphismCard({
  children,
  className = '',
  'data-testid': testId,
}: GlassMorphismCardProps) {
  return (
    <div
      className={`backdrop-blur-md bg-base-100/30 border border-base-content/10 rounded-xl shadow-lg ${className}`}
      data-testid={testId}
    >
      {children}
    </div>
  )
}

export default GlassMorphismCard
