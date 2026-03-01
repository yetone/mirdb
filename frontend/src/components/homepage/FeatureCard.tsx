import React from 'react'
import { GlassMorphismCard } from '../GlassMorphismCard'
import { clsx } from 'clsx'

export interface FeatureCardProps {
  icon: React.ReactNode
  title: string
  description: string
  className?: string
}

/**
 * Feature Card Component
 * Owner: Scenario 2 - Feature Highlights Section
 *
 * Individual feature card for the features section.
 * Displays a feature with icon, title, and description using GlassMorphismCard styling.
 *
 * Requirements:
 * - Uses GlassMorphismCard wrapper
 * - Hover effects for interactivity
 * - Accessible with proper ARIA labels
 */
export const FeatureCard: React.FC<FeatureCardProps> = ({
  icon,
  title,
  description,
  className
}) => {
  return (
    <GlassMorphismCard
      className={clsx(
        'p-6 flex flex-col items-center text-center',
        'transform transition-transform duration-300',
        'hover:scale-105',
        className
      )}
    >
      <div
        className="feature-icon mb-4 text-4xl text-primary"
        aria-hidden="true"
        data-testid="feature-icon"
      >
        {icon}
      </div>
      <h3
        className="text-xl font-semibold mb-2 text-base-content"
        data-testid="feature-title"
      >
        {title}
      </h3>
      <p
        className="text-base-content/70"
        data-testid="feature-description"
      >
        {description}
      </p>
    </GlassMorphismCard>
  )
}

export default FeatureCard
