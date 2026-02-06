/**
 * Individual feature card component.
 * Owner: Scenario 3 - Features Section
 *
 * Displays a single feature with icon, title, and description.
 */

import styles from './Features.module.css'

export interface FeatureCardProps {
  icon: string
  title: string
  description: string
}

const iconMap: Record<string, string> = {
  rocket: '🚀',
  shield: '🛡️',
  chart: '📈',
  globe: '🌐',
  code: '💻',
  support: '🎧',
}

export function FeatureCard({ icon, title, description }: FeatureCardProps) {
  const iconEmoji = iconMap[icon] || '⭐'

  return (
    <article className={styles.card} data-testid="feature-card">
      <div
        className={styles.iconWrapper}
        aria-hidden="true"
        data-testid="feature-icon"
      >
        <span className={styles.icon} role="img" aria-label={`${title} icon`}>
          {iconEmoji}
        </span>
      </div>
      <h3 className={styles.cardTitle} data-testid="feature-title">
        {title}
      </h3>
      <p className={styles.cardDescription} data-testid="feature-description">
        {description}
      </p>
    </article>
  )
}
