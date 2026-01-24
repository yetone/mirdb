import React, { ReactNode } from 'react'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
}

export function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <div className={`card bg-base-200/50 backdrop-blur-lg border border-base-300 shadow-xl ${className}`}>
      <div className="card-body">
        {children}
      </div>
    </div>
  )
}

export default GlassMorphismCard
