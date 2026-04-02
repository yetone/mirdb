/**
 * Features section component.
 * Owner: Scenario 3 - Features Section Display
 *
 * Displays MirDB's core capabilities:
 * - Memcached Protocol support
 * - Persistence with SSTables
 * - LSM Tree architecture
 */
import React from 'react';
import { Container } from '@/components/common/Container';
import { FeatureCard } from '@/components/ui/FeatureCard';
import { features } from '@/data/features';
import { SECTION_IDS } from '@/utils/constants';
import styles from './Features.module.css';

export function Features() {
  return (
    <section id={SECTION_IDS.features} className={styles.section} aria-labelledby="features-heading">
      <Container>
        <h2 id="features-heading" className={styles.heading}>
          Features
        </h2>
        <div className={styles.grid} data-testid="features-grid">
          {features.map((feature) => (
            <FeatureCard
              key={feature.title}
              icon={feature.icon}
              title={feature.title}
              description={feature.description}
            />
          ))}
        </div>
      </Container>
    </section>
  );
}
