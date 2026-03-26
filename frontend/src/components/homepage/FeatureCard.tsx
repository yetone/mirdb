/**
 * Feature Card Component
 * Owner: Scenario 5 - Features Section Display
 *
 * Displays a single feature card with:
 * - Icon or illustration
 * - Feature title
 * - 1-2 sentence description
 * - Hover lift effect
 *
 * Requirements: REQ-4
 */

import type { FeatureItem } from '../../types/homepage'

export interface FeatureCardProps extends FeatureItem {
  className?: string
}

export function FeatureCard({ icon, title, description, className = '' }: FeatureCardProps) {
  return (
    <article
      data-testid="feature-card"
      className={`card bg-base-100 shadow-md hover:shadow-xl transition-all duration-300 hover:-translate-y-1 ${className}`}
    >
      <div className="card-body items-center text-center">
        <div
          data-testid="feature-icon"
          className="text-4xl mb-4 text-primary"
          role="img"
          aria-label={`${title} icon`}
        >
          {icon}
        </div>
        <h3
          data-testid="feature-title"
          className="card-title text-lg font-semibold"
        >
          {title}
        </h3>
        <p
          data-testid="feature-description"
          className="text-base-content/70 text-sm"
        >
          {description}
        </p>
      </div>
    </article>
  )
}

export default FeatureCard
