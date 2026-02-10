/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Displays a grid of feature cards showcasing MirDB capabilities:
 * - Tokio with Memcached Protocol
 * - Memtable with Skiplist
 * - Minor Compaction
 * - Major Compaction
 */

import React from 'react';
import { FeatureCard } from '../ui/FeatureCard';
import { getFeaturesWithIcons } from '../../data/features';

const sectionStyles: React.CSSProperties = {
  padding: 'var(--spacing-3xl) 0',
};

const containerStyles: React.CSSProperties = {
  maxWidth: 'var(--container-max-width)',
  margin: '0 auto',
  padding: '0 var(--container-padding)',
};

const headerStyles: React.CSSProperties = {
  textAlign: 'center',
  marginBottom: 'var(--spacing-2xl)',
};

const titleStyles: React.CSSProperties = {
  fontSize: 'var(--font-size-3xl)',
  fontWeight: 700,
  color: 'var(--text-primary)',
  marginBottom: 'var(--spacing-md)',
};

const subtitleStyles: React.CSSProperties = {
  fontSize: 'var(--font-size-lg)',
  color: 'var(--text-secondary)',
  maxWidth: '600px',
  margin: '0 auto',
};

const gridStyles: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
  gap: 'var(--spacing-xl)',
};

export const Features: React.FC = () => {
  const features = getFeaturesWithIcons();

  return (
    <section id="features" style={sectionStyles} aria-labelledby="features-title">
      <div style={containerStyles}>
        <header style={headerStyles}>
          <h2 id="features-title" style={titleStyles}>
            Core Features
          </h2>
          <p style={subtitleStyles}>
            MirDB combines the simplicity of memcached protocol with the durability of persistent
            storage through LSM-tree architecture.
          </p>
        </header>
        <div
          style={gridStyles}
          className="features-grid"
          data-testid="features-grid"
          role="list"
        >
          {features.map((feature) => (
            <div key={feature.id} role="listitem">
              <FeatureCard feature={feature} />
            </div>
          ))}
        </div>
      </div>
    </section>
  );
};

export default Features;
