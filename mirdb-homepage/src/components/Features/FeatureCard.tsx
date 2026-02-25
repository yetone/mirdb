import type { Feature } from '../../types';
import styles from './Features.module.css';

interface FeatureCardProps {
  feature: Feature;
}

export function FeatureCard({ feature }: FeatureCardProps) {
  const { title, description, icon, status } = feature;

  return (
    <article className={styles.card}>
      <span className={styles.icon} aria-hidden="true">
        {icon}
      </span>
      <h3 className={styles.cardTitle}>{title}</h3>
      <p className={styles.cardDescription}>{description}</p>
      {status === 'planned' && (
        <span className={styles.plannedBadge}>Planned</span>
      )}
    </article>
  );
}
