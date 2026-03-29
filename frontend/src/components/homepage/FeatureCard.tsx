/**
 * Feature Card Component
 * Owner: Scenario 2 - Features Section with Three Core Features
 *
 * Individual feature card with:
 * - Icon or illustration
 * - Feature name/title
 * - 1-2 sentence description
 * - Optional "Learn more" link
 * - Hover effects (elevation/border)
 *
 * Requirements: REQ-2
 */
import { ReactNode } from 'react'

export interface FeatureCardProps {
  icon: ReactNode
  title: string
  description: string
  learnMoreLink?: string
}

export function FeatureCard({ icon, title, description, learnMoreLink }: FeatureCardProps) {
  return (
    <div
      className="card bg-base-200 shadow-md hover:shadow-lg transition-shadow duration-300 hover:border-primary border border-transparent"
      data-testid="feature-card"
    >
      <div className="card-body items-center text-center">
        <div className="text-primary mb-4" data-testid="feature-icon">
          {icon}
        </div>
        <h3 className="card-title text-lg" data-testid="feature-title">
          {title}
        </h3>
        <p className="text-base-content/70" data-testid="feature-description">
          {description}
        </p>
        {learnMoreLink && (
          <div className="card-actions mt-4">
            <a
              href={learnMoreLink}
              className="link link-primary text-sm"
              data-testid="feature-learn-more"
            >
              Learn more
            </a>
          </div>
        )}
      </div>
    </div>
  )
}
