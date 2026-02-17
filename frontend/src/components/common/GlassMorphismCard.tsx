/**
 * Glass-style card container component.
 * Owner: Scenario 13 - Component Integration
 *
 * Features:
 * - Frosted glass visual effect
 * - Theme-aware styling
 * - Flexible content container
 */

import React, { ReactNode } from 'react'

export interface GlassMorphismCardProps {
  className?: string
  children: ReactNode
  testId?: string
}

export function GlassMorphismCard({ className = '', children, testId }: GlassMorphismCardProps) {
  return (
    <div
      data-testid={testId}
      className={`
        backdrop-blur-md
        bg-base-100/70
        border border-base-content/10
        rounded-2xl
        shadow-xl
        p-6
        transition-all
        duration-300
        hover:shadow-2xl
        hover:bg-base-100/80
        ${className}
      `}
    >
      {children}
    </div>
  )
}
