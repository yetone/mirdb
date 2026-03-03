/**
 * GlassMorphismCard Component
 * Glass-like card component with blur effect.
 * This is an existing component used by multiple scenarios.
 */
import React from 'react'

interface GlassMorphismCardProps {
  children: React.ReactNode
  className?: string
}

const GlassMorphismCard: React.FC<GlassMorphismCardProps> = ({
  children,
  className = '',
}) => {
  return (
    <div
      className={`card bg-base-100/80 backdrop-blur-md border border-base-300 shadow-xl ${className}`}
    >
      <div className="card-body">
        {children}
      </div>
    </div>
  )
}

export default GlassMorphismCard
