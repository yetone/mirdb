/**
 * Features Section component.
 * Owner: Scenario 3 - Features and Roadmap
 *
 * Lists implemented MirDB capabilities with icons and descriptions.
 * Covers REQ-8.
 */

export interface Feature {
  id: string;
  title: string;
  description: string;
  icon: React.ReactNode;
  prominent?: boolean;
}

export const FEATURES: Feature[] = [
  {
    id: 'memcached-protocol',
    title: 'Memcached Protocol',
    description:
      'Wire-compatible with the Memcached protocol. Drop-in replacement for existing memcached deployments with zero client changes.',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} data-testid="feature-icon-memcached">
        <rect x="2" y="4" width="20" height="6" rx="1" />
        <rect x="2" y="14" width="20" height="6" rx="1" />
        <line x1="6" y1="7" x2="6" y2="7" />
        <line x1="6" y1="17" x2="6" y2="17" />
        <line x1="10" y1="7" x2="18" y2="7" />
        <line x1="10" y1="17" x2="18" y2="17" />
      </svg>
    ),
    prominent: true,
  },
  {
    id: 'tokio-async',
    title: 'Tokio Async Runtime',
    description:
      'Built on Tokio for high-performance asynchronous I/O. Handles thousands of concurrent connections with minimal overhead.',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} data-testid="feature-icon-tokio">
        <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
      </svg>
    ),
  },
  {
    id: 'skip-list-memtable',
    title: 'Skip-list Memtable',
    description:
      'In-memory skip-list data structure for fast ordered writes and range scans. Efficient O(log n) insertions and lookups.',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} data-testid="feature-icon-skiplist">
        <line x1="4" y1="20" x2="20" y2="20" />
        <line x1="4" y1="16" x2="16" y2="16" />
        <line x1="4" y1="12" x2="12" y2="12" />
        <line x1="4" y1="8" x2="8" y2="8" />
        <line x1="4" y1="4" x2="6" y2="4" />
      </svg>
    ),
  },
  {
    id: 'lsm-tree',
    title: 'LSM Tree Storage',
    description:
      'Log-Structured Merge Tree architecture for write-optimized persistent storage. Efficient sequential writes with tiered compaction.',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} data-testid="feature-icon-lsm">
        <rect x="8" y="2" width="8" height="4" rx="1" />
        <rect x="6" y="8" width="12" height="4" rx="1" />
        <rect x="4" y="14" width="16" height="4" rx="1" />
        <rect x="2" y="20" width="20" height="2" rx="1" />
      </svg>
    ),
  },
  {
    id: 'compaction',
    title: 'Minor/Major Compaction',
    description:
      'Automatic background compaction to merge SSTable files. Minor compaction flushes memtables; major compaction reclaims space and optimizes reads.',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} data-testid="feature-icon-compaction">
        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
        <polyline points="17 8 12 3 7 8" />
        <line x1="12" y1="3" x2="12" y2="15" />
      </svg>
    ),
  },
  {
    id: 'persistence',
    title: 'Persistence',
    description:
      'Durable write-ahead logging ensures data survives crashes and restarts. Automatic recovery replays logs on startup.',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} data-testid="feature-icon-persistence">
        <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
        <polyline points="9 12 11 14 15 10" />
      </svg>
    ),
  },
];

function FeatureCard({ feature }: { feature: Feature }) {
  return (
    <div
      className={`feature-card${feature.prominent ? ' feature-card--prominent' : ''}`}
      data-testid={`feature-card-${feature.id}`}
      tabIndex={0}
      role="listitem"
      aria-label={feature.title}
    >
      <div
        className={`feature-card__icon${feature.prominent ? ' feature-card__icon--prominent' : ''}`}
        data-testid={`feature-icon-wrapper-${feature.id}`}
        aria-hidden="true"
      >
        {feature.icon}
      </div>
      <h3 className="feature-card__title" data-testid={`feature-title-${feature.id}`}>
        {feature.title}
      </h3>
      <p className="feature-card__description" data-testid={`feature-description-${feature.id}`}>
        {feature.description}
      </p>
    </div>
  );
}

export default function FeaturesSection() {
  return (
    <section
      className="features-section"
      aria-label="Features"
      data-testid="features-section"
    >
      <div className="features-section__container">
        <h2 className="features-section__heading" data-testid="features-heading">
          Features
        </h2>
        <p className="features-section__subheading" data-testid="features-subheading">
          Built for speed, durability, and compatibility.
        </p>
        <div className="features-section__grid" role="list" data-testid="features-grid">
          {FEATURES.map((feature) => (
            <FeatureCard key={feature.id} feature={feature} />
          ))}
        </div>
      </div>
    </section>
  );
}
