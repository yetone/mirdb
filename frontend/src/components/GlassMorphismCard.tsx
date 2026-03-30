/**
 * GlassMorphism Card component with blur effect
 */

import React, { ReactNode } from 'react'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
}

const GlassMorphismCard: React.FC<GlassMorphismCardProps> = ({ children, className = '' }) => {
  return (
    <div
      className={`backdrop-blur-lg bg-base-100/30 border border-base-content/10 rounded-xl p-6 shadow-xl ${className}`}
    >
      {children}
    </div>
  )
}

export default GlassMorphismCard
