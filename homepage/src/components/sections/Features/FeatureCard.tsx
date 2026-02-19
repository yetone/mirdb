/**
 * FeatureCard Component
 * Owner: Scenario 3 - Features Section
 *
 * Displays a single feature with icon, title, and description.
 */

import type { Feature } from '@/types'
import { Icon } from '@/components/ui/Icon/Icon'
import styles from './Features.module.css'

interface FeatureCardProps {
  title: string
  description: string
  icon: string
}

export function FeatureCard({ title, description, icon }: FeatureCardProps) {
  return (
    <article className={styles.card} data-testid="feature-card">
      <div className={styles.iconWrapper}>
        <Icon name={icon} size={24} />
      </div>
      <h3 className={styles.cardTitle}>{title}</h3>
      <p className={styles.cardDescription}>{description}</p>
    </article>
  )
}
