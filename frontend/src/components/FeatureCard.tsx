import { ReactNode } from 'react'

/**
 * Feature highlight card component.
 * Owner: Scenario 1 - Homepage Hero Section
 *
 * Displays feature with icon, title, and description.
 * Supports dark/light themes via DaisyUI.
 * Accessible with proper ARIA attributes.
 */
interface FeatureCardProps {
  icon: ReactNode
  title: string
  description: string
}

export function FeatureCard({ icon, title, description }: FeatureCardProps) {
  return (
    <article
      className="card bg-base-200 shadow-lg hover:shadow-xl transition-shadow duration-300"
      aria-labelledby={`feature-title-${title.toLowerCase().replace(/\s+/g, '-')}`}
    >
      <div className="card-body items-center text-center">
        <div
          className="mb-4 p-3 rounded-full bg-base-300"
          data-testid="feature-icon"
        >
          {icon}
        </div>
        <h3
          id={`feature-title-${title.toLowerCase().replace(/\s+/g, '-')}`}
          className="card-title text-lg font-semibold"
        >
          {title}
        </h3>
        <p className="text-base-content/70">{description}</p>
      </div>
    </article>
  )
}

export default FeatureCard
