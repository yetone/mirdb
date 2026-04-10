/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Displays the key features in a grid layout:
 * - LSM-tree architecture
 * - Rust implementation
 * - Crash recovery
 * - Memcached compatibility
 *
 * Each feature has icon, heading, and description.
 * Grid: 2 columns on desktop, 1 on mobile.
 */

import { features } from '../../data/features';
import { Card } from '../common';
import './Features.css';

/**
 * Icon components for each feature
 */
const FeatureIcon = ({ type }: { type: string }) => {
  switch (type) {
    case 'tree':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M12 2L8 8h8L12 2z" />
          <path d="M12 8L6 16h12L12 8z" />
          <path d="M12 16v6" />
          <path d="M9 22h6" />
        </svg>
      );
    case 'rust':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="12" cy="12" r="9" />
          <path d="M12 3v2" />
          <path d="M12 19v2" />
          <path d="M3 12h2" />
          <path d="M19 12h2" />
          <path d="M9 9l1.5 1.5" />
          <path d="M13.5 13.5L15 15" />
          <path d="M9 15l1.5-1.5" />
          <path d="M13.5 10.5L15 9" />
        </svg>
      );
    case 'shield':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M12 2l8 4v6c0 5.5-3.5 10-8 11-4.5-1-8-5.5-8-11V6l8-4z" />
          <path d="M9 12l2 2 4-4" />
        </svg>
      );
    case 'plug':
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M12 2v6" />
          <path d="M8 2v6" />
          <path d="M16 2v6" />
          <rect x="4" y="8" width="16" height="8" rx="2" />
          <path d="M10 16v4" />
          <path d="M14 16v4" />
        </svg>
      );
    default:
      return null;
  }
};

export interface FeatureCardProps {
  id: string;
  title: string;
  description: string;
  icon?: string;
}

export function FeatureCard({ id, title, description, icon }: FeatureCardProps) {
  return (
    <Card
      title={title}
      description={description}
      icon={icon ? <FeatureIcon type={icon} /> : undefined}
      className="feature-card"
      testId={`feature-card-${id}`}
    />
  );
}

export function Features() {
  return (
    <section className="features" id="features" aria-labelledby="features-heading">
      <div className="container">
        <h2 id="features-heading" className="features__title">
          Key Features
        </h2>
        <p className="features__subtitle">
          Everything you need for high-performance key-value storage
        </p>
        <div className="features__grid" data-testid="features-grid">
          {features.map((feature) => (
            <FeatureCard
              key={feature.id}
              id={feature.id}
              title={feature.title}
              description={feature.description}
              icon={feature.icon}
            />
          ))}
        </div>
      </div>
    </section>
  );
}
