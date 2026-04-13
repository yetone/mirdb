/**
 * FeatureCard Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Displays a single feature with title, description, and optional icon.
 */
import type { Feature } from '../../types';
import './Features.css';

interface FeatureCardProps {
  feature: Feature;
}

export function FeatureCard({ feature }: FeatureCardProps) {
  return (
    <article className="feature-card" data-testid="feature-card">
      {feature.icon && (
        <span className="feature-icon" aria-hidden="true">
          {feature.icon}
        </span>
      )}
      <h3 className="feature-title">{feature.title}</h3>
      <p className="feature-description">{feature.description}</p>
    </article>
  );
}
