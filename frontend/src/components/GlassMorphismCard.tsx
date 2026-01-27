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
      className={`backdrop-blur-md bg-white/10 dark:bg-gray-900/30 border border-white/20 dark:border-gray-700/30 rounded-xl shadow-lg ${className}`}
      data-testid="glassmorphism-card"
    >
      {children}
    </div>
  )
}

export default GlassMorphismCard
