/**
 * Features Section Component
 * Owner: Scenario 3 - Features Section
 *
 * Displays key MirDB features in a responsive grid:
 * - LSM-Tree Storage Engine
 * - Memcached Protocol Compatibility
 * - TTL Support
 * - Write-Ahead Log Durability
 */

import { features } from '@/data/features'
import { FeatureCard } from './FeatureCard'
import styles from './Features.module.css'

export function Features() {
  return (
    <section id="features" className={styles.features} aria-labelledby="features-heading">
      <div className={styles.container}>
        <h2 id="features-heading" className={styles.heading}>
          Features
        </h2>
        <div className={styles.grid} data-testid="features-grid">
          {features.map((feature) => (
            <FeatureCard
              key={feature.id}
              title={feature.title}
              description={feature.description}
              icon={feature.icon}
            />
          ))}
        </div>
      </div>
    </section>
  )
}
