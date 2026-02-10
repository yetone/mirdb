/**
 * Feature Card Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Displays a single feature card with:
 * - Icon
 * - Title
 * - Description
 * - Hover effects
 */

import React from 'react';
import type { Feature } from '../../types';

export interface FeatureCardProps {
  feature: Feature;
}

const cardStyles: React.CSSProperties = {
  backgroundColor: 'var(--bg-secondary)',
  borderRadius: 'var(--radius-lg)',
  padding: 'var(--spacing-xl)',
  border: '1px solid var(--border-color)',
  transition: 'transform 0.2s ease, box-shadow 0.2s ease',
  cursor: 'default',
};

const iconContainerStyles: React.CSSProperties = {
  width: '48px',
  height: '48px',
  borderRadius: 'var(--radius-md)',
  backgroundColor: 'var(--accent-primary)',
  color: '#ffffff',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  marginBottom: 'var(--spacing-md)',
};

const titleStyles: React.CSSProperties = {
  fontSize: 'var(--font-size-lg)',
  fontWeight: 600,
  color: 'var(--text-primary)',
  marginBottom: 'var(--spacing-sm)',
};

const descriptionStyles: React.CSSProperties = {
  fontSize: 'var(--font-size-base)',
  color: 'var(--text-secondary)',
  lineHeight: 1.6,
};

export const FeatureCard: React.FC<FeatureCardProps> = ({ feature }) => {
  const [isHovered, setIsHovered] = React.useState(false);

  const dynamicCardStyles: React.CSSProperties = {
    ...cardStyles,
    transform: isHovered ? 'translateY(-4px)' : 'translateY(0)',
    boxShadow: isHovered ? 'var(--shadow-lg)' : 'var(--shadow-sm)',
  };

  return (
    <article
      className="feature-card"
      style={dynamicCardStyles}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      data-testid={`feature-card-${feature.id}`}
    >
      <div style={iconContainerStyles} aria-hidden="true">
        {feature.icon}
      </div>
      <h3 style={titleStyles}>{feature.title}</h3>
      <p style={descriptionStyles}>{feature.description}</p>
    </article>
  );
};

export default FeatureCard;
