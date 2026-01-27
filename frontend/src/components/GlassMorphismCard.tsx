import React from 'react'

interface GlassMorphismCardProps {
  children: React.ReactNode
  className?: string
}

export const GlassMorphismCard: React.FC<GlassMorphismCardProps> = ({
  children,
  className = '',
}) => {
  return (
    <div
      className={`backdrop-blur-md bg-base-100/70 border border-base-content/10 rounded-xl shadow-xl transition-all duration-300 hover:shadow-2xl hover:bg-base-100/80 ${className}`}
    >
      {children}
    </div>
  )
}
