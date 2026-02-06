/**
 * Features section component.
 * Owner: Scenario 3 - Features Section
 *
 * Displays key features/benefits in a responsive grid layout.
 * Requirements covered: REQ-6
 */

import { FeatureCard } from './FeatureCard'
import { FEATURE_LIST } from '../../utils/constants'
import type { Feature } from '../../types'
import styles from './Features.module.css'

export interface FeaturesProps {
  features?: Feature[]
  title?: string
}

export function Features({
  features = FEATURE_LIST,
  title = 'Our Features',
}: FeaturesProps) {
  return (
    <section
      id="features"
      className={styles.features}
      aria-labelledby="features-heading"
    >
      <div className={styles.container}>
        <h2 id="features-heading" className={styles.heading}>
          {title}
        </h2>
        <div className={styles.grid} data-testid="features-grid">
          {features.map((feature, index) => (
            <FeatureCard
              key={`${feature.title}-${index}`}
              icon={feature.icon}
              title={feature.title}
              description={feature.description}
            />
          ))}
        </div>
      </div>
    </section>
  )
}
