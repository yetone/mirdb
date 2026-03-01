import React from 'react'
import { clsx } from 'clsx'

interface GlassMorphismCardProps {
  children: React.ReactNode
  className?: string
}

/**
 * GlassMorphismCard Component
 *
 * A glass morphism styled card component with blur effect and transparency.
 * Used throughout the application for consistent card styling.
 */
export const GlassMorphismCard: React.FC<GlassMorphismCardProps> = ({
  children,
  className
}) => {
  return (
    <div
      className={clsx(
        'glass-card',
        'backdrop-blur-md bg-white/10 dark:bg-gray-900/30',
        'border border-white/20 dark:border-gray-700/30',
        'rounded-xl shadow-lg',
        'transition-all duration-300',
        'hover:bg-white/20 dark:hover:bg-gray-900/40',
        'hover:shadow-xl',
        className
      )}
      data-testid="glass-card"
    >
      {children}
    </div>
  )
}

export default GlassMorphismCard
