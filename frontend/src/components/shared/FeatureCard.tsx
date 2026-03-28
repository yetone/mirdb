/**
 * Feature Card component.
 * Owner: Scenario 2 - Feature Showcase Section
 *
 * A reusable card component for displaying product features.
 * Uses DaisyUI card styling with theme-aware design.
 */
import React from 'react'

export interface FeatureCardProps {
  icon: React.ReactNode | string
  title: string
  description: string
}

export const FeatureCard: React.FC<FeatureCardProps> = ({ icon, title, description }) => {
  return (
    <div
      className="card bg-base-100 shadow-md hover:shadow-xl transition-all duration-300 hover:-translate-y-1"
      data-testid="feature-card"
    >
      <div className="card-body items-center text-center">
        <div
          className="text-primary text-4xl mb-4"
          data-testid="feature-icon"
          aria-hidden="true"
        >
          {icon}
        </div>
        <h3
          className="card-title text-base-content text-lg font-semibold"
          data-testid="feature-title"
        >
          {title}
        </h3>
        <p
          className="text-base-content/70 text-sm leading-relaxed"
          data-testid="feature-description"
        >
          {description}
        </p>
      </div>
    </div>
  )
}

export default FeatureCard
