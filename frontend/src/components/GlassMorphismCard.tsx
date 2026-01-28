/**
 * GlassMorphismCard Component
 *
 * Card with glass effect styling for features and content.
 */

import React, { ReactNode } from 'react'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
}

export function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <div
      className={`card bg-base-100/80 backdrop-blur-lg shadow-xl border border-base-300 ${className}`}
    >
      {children}
    </div>
  )
}
