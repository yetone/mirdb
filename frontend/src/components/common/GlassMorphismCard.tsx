/**
 * Glass-style card container component.
 * Owner: Scenario 13 - Component Integration
 *
 * Features:
 * - Frosted glass visual effect
 * - Theme-aware styling
 * - Flexible content container
 */

import React from 'react'

export interface GlassMorphismCardProps {
  className?: string
  children: React.ReactNode
}

export function GlassMorphismCard({ className = '', children }: GlassMorphismCardProps) {
  return (
    <div
      className={`card bg-base-200/50 backdrop-blur-md border border-base-content/10 shadow-xl ${className}`}
    >
      <div className="card-body">{children}</div>
    </div>
  )
}
