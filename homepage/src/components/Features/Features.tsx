import { FEATURES } from '../../utils/constants'
import type { Feature } from '../../types'
import './Features.css'

interface FeatureCardProps {
  feature: Feature
}

function FeatureCard({ feature }: FeatureCardProps) {
  return (
    <article className="feature-card" data-testid={`feature-${feature.id}`}>
      {feature.icon && <span className="feature-icon" aria-hidden="true">{feature.icon}</span>}
      <h3 className="feature-title">{feature.title}</h3>
      <p className="feature-description">{feature.description}</p>
    </article>
  )
}

export function Features() {
  return (
    <div className="features container" data-testid="features-section">
      <h2 className="section-title">Key Features</h2>
      <div className="features-grid">
        {FEATURES.map((feature) => (
          <FeatureCard key={feature.id} feature={feature} />
        ))}
      </div>
    </div>
  )
}
