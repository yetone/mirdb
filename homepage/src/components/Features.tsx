import React from 'react';
import { FeatureCard } from './FeatureCard';
import { MemcachedIcon, PersistenceIcon, LsmTreeIcon } from './Icons';

export interface Feature {
  id: string;
  title: string;
  description: string;
  icon: React.ReactNode;
}

export const featuresData: Feature[] = [
  {
    id: 'memcached',
    title: 'Memcached Protocol Compatible',
    description:
      'Drop-in replacement for memcached clients. Use your existing memcached client libraries without any code changes. Full support for GET, SET, DELETE, and other standard commands.',
    icon: <MemcachedIcon />,
  },
  {
    id: 'persistence',
    title: 'Durable Persistence',
    description:
      'Unlike memcached, your data survives restarts. MirDB persists all data to disk using SSTables, ensuring your cached data is never lost due to process restarts or system reboots.',
    icon: <PersistenceIcon />,
  },
  {
    id: 'lsm-tree',
    title: 'LSM-Tree Architecture',
    description:
      'Built on a Log-Structured Merge-tree (LSM-tree) storage engine with SSTable compaction. Optimized for write-heavy workloads with efficient background compaction across multiple levels.',
    icon: <LsmTreeIcon />,
  },
];

export const Features: React.FC = () => {
  return (
    <section id="features" className="features-section" data-testid="features-section">
      <div className="container">
        <h2 className="section-title">Key Features</h2>
        <div className="features-grid" data-testid="features-grid">
          {featuresData.map((feature) => (
            <FeatureCard
              key={feature.id}
              title={feature.title}
              description={feature.description}
              icon={feature.icon}
              testId={`feature-${feature.id}`}
            />
          ))}
        </div>
      </div>
    </section>
  );
};

export default Features;
