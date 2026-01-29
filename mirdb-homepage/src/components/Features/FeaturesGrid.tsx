/**
 * React version of FeaturesGrid for testing purposes.
 * This mirrors the FeaturesGrid.astro component structure.
 */

import type { Feature } from '../../types/index';

interface IconProps {
  name: string;
  size?: 'sm' | 'md' | 'lg';
  className?: string;
}

const sizeMap = {
  sm: 'w-4 h-4',
  md: 'w-6 h-6',
  lg: 'w-8 h-8',
};

const iconPaths: Record<string, string> = {
  'memcached': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />',
  'storage': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />',
  'performance': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" />',
  'config': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" /><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />',
  'compaction': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />',
  'async': '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />',
};

export function Icon({ name, size = 'md', className = '' }: IconProps) {
  const sizeClass = sizeMap[size];
  const iconPath = iconPaths[name] || iconPaths['memcached'];

  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      className={`${sizeClass} stroke-current fill-none ${className}`}
      viewBox="0 0 24 24"
      aria-hidden="true"
      dangerouslySetInnerHTML={{ __html: iconPath }}
      data-testid={`icon-${name}`}
    />
  );
}

interface FeatureCardProps {
  icon: string;
  title: string;
  description: string;
}

export function FeatureCard({ icon, title, description }: FeatureCardProps) {
  return (
    <article
      className="feature-card bg-surface border border-border rounded-lg p-6 hover:border-accent transition-colors duration-200"
      data-testid="feature-card"
    >
      <div className="flex items-center gap-3 mb-4">
        <div className="text-accent">
          <Icon name={icon} size="lg" />
        </div>
        <h3 className="text-lg font-mono font-semibold text-text-primary" data-testid="feature-title">
          {title}
        </h3>
      </div>
      <p className="text-text-secondary text-sm leading-relaxed" data-testid="feature-description">
        {description}
      </p>
    </article>
  );
}

interface FeaturesGridProps {
  features?: Feature[];
}

// Default features matching the content/features.json
const defaultFeatures: Feature[] = [
  {
    id: 'memcached-compatible',
    title: 'Memcached Compatible',
    description: 'Drop-in replacement for Memcached. Supports standard protocol including get, set, add, replace, append, prepend, and delete commands.',
    icon: 'memcached',
  },
  {
    id: 'persistent-storage',
    title: 'Persistent Storage',
    description: 'Data survives restarts via LSM-tree architecture. Write-ahead logging ensures durability with configurable sync policies.',
    icon: 'storage',
  },
  {
    id: 'high-performance',
    title: 'High Performance',
    description: 'Skip-list memtable for fast in-memory operations. Compressed SSTables reduce storage footprint while maintaining read performance.',
    icon: 'performance',
  },
  {
    id: 'configurable',
    title: 'Configurable',
    description: 'TOML-based configuration with sensible defaults. Fine-tune memory limits, compaction thresholds, and I/O parameters.',
    icon: 'config',
  },
  {
    id: 'compaction',
    title: 'Compaction',
    description: 'Automatic minor and major compaction for storage efficiency. Background processes merge SSTables and reclaim space from deleted keys.',
    icon: 'compaction',
  },
  {
    id: 'async-io',
    title: 'Async I/O',
    description: 'Built on Tokio for high concurrency. Asynchronous I/O operations maximize throughput on modern multi-core systems.',
    icon: 'async',
  },
];

export function FeaturesGrid({ features = defaultFeatures }: FeaturesGridProps) {
  return (
    <section className="features-grid max-w-6xl mx-auto" data-testid="features-grid">
      <h2 className="text-3xl font-mono font-bold text-center mb-12 text-text-primary">
        Why MirDB?
      </h2>
      <div
        className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
        data-testid="features-container"
      >
        {features.map((feature) => (
          <FeatureCard
            key={feature.id}
            icon={feature.icon}
            title={feature.title}
            description={feature.description}
          />
        ))}
      </div>
    </section>
  );
}

export default FeaturesGrid;
