/**
 * Planned Features component.
 * Owner: Scenario 3 - Features and Roadmap
 *
 * Highlights upcoming capabilities with distinct visual styling.
 * Covers REQ-9.
 */

export interface PlannedFeature {
  id: string;
  title: string;
  description: string;
  icon: React.ReactNode;
}

export const PLANNED_FEATURES: PlannedFeature[] = [
  {
    id: 'raft-consensus',
    title: 'Raft Consensus',
    description:
      'Distributed consensus for multi-node replication and automatic failover. Build highly available MirDB clusters with strong consistency guarantees.',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} data-testid="planned-feature-icon-raft">
        <circle cx="12" cy="5" r="3" />
        <circle cx="5" cy="19" r="3" />
        <circle cx="19" cy="19" r="3" />
        <line x1="12" y1="8" x2="5" y2="16" />
        <line x1="12" y1="8" x2="19" y2="16" />
      </svg>
    ),
  },
];

function PlannedFeatureCard({ feature }: { feature: PlannedFeature }) {
  return (
    <div
      className="planned-feature-card"
      data-testid={`planned-feature-card-${feature.id}`}
      tabIndex={0}
      role="listitem"
      aria-label={`${feature.title} - Coming Soon`}
    >
      <div className="planned-feature-card__badge" data-testid="coming-soon-badge">
        Coming Soon
      </div>
      <div
        className="planned-feature-card__icon"
        data-testid={`planned-feature-icon-wrapper-${feature.id}`}
        aria-hidden="true"
      >
        {feature.icon}
      </div>
      <h3
        className="planned-feature-card__title"
        data-testid={`planned-feature-title-${feature.id}`}
      >
        {feature.title}
      </h3>
      <p
        className="planned-feature-card__description"
        data-testid={`planned-feature-description-${feature.id}`}
      >
        {feature.description}
      </p>
    </div>
  );
}

export default function PlannedFeatures() {
  return (
    <section
      className="planned-features-section"
      aria-label="Planned Features"
      data-testid="planned-features-section"
    >
      <div className="planned-features-section__container">
        <h2 className="planned-features-section__heading" data-testid="planned-features-heading">
          Roadmap
        </h2>
        <p
          className="planned-features-section__subheading"
          data-testid="planned-features-subheading"
        >
          Exciting capabilities on the horizon.
        </p>
        <div
          className="planned-features-section__grid"
          role="list"
          data-testid="planned-features-grid"
        >
          {PLANNED_FEATURES.map((feature) => (
            <PlannedFeatureCard key={feature.id} feature={feature} />
          ))}
        </div>
      </div>
    </section>
  );
}
