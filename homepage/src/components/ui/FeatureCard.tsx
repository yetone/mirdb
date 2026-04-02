/**
 * Feature card component for displaying individual features.
 * Owner: Scenario 3 - Features Section Display
 */
import React from 'react';
import { FeatureCardProps } from '@/types';
import styles from './FeatureCard.module.css';

export function FeatureCard({ icon, title, description }: FeatureCardProps) {
  return (
    <article className={styles.card} data-testid="feature-card">
      <div className={styles.icon} aria-hidden="true">
        {icon}
      </div>
      <h3 className={styles.title}>{title}</h3>
      <p className={styles.description}>{description}</p>
    </article>
  );
}
