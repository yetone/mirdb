/**
 * MetricCard component
 *
 * Owner: Scenario 2 - Real-time System Metrics Dashboard
 *
 * Displays a single metric with a label and formatted value.
 */

import React from 'react';

export interface MetricCardProps {
  /** Label describing the metric */
  label: string;
  /** The primary value to display */
  value: string;
  /** Optional secondary value (e.g., for memory: "/ 2GB") */
  secondaryValue?: string;
  /** Optional icon or emoji to display */
  icon?: string;
  /** Custom CSS class name */
  className?: string;
  /** Test ID for testing purposes */
  testId?: string;
}

/**
 * A card component that displays a single metric with its value.
 */
export function MetricCard({
  label,
  value,
  secondaryValue,
  icon,
  className = '',
  testId,
}: MetricCardProps): React.ReactElement {
  return (
    <div
      className={`metric-card ${className}`}
      data-testid={testId}
      style={{
        padding: '1.5rem',
        backgroundColor: 'var(--bg-secondary, #f5f5f5)',
        borderRadius: '8px',
        display: 'flex',
        flexDirection: 'column',
        gap: '0.5rem',
      }}
    >
      <div
        className="metric-card__header"
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '0.5rem',
          color: 'var(--text-secondary, #666)',
          fontSize: '0.875rem',
        }}
      >
        {icon && <span className="metric-card__icon">{icon}</span>}
        <span className="metric-card__label">{label}</span>
      </div>
      <div
        className="metric-card__value"
        style={{
          fontSize: '1.75rem',
          fontWeight: 600,
          color: 'var(--text-primary, #1a1a1a)',
        }}
      >
        {value}
        {secondaryValue && (
          <span
            className="metric-card__secondary"
            style={{
              fontSize: '1rem',
              fontWeight: 400,
              color: 'var(--text-secondary, #666)',
              marginLeft: '0.25rem',
            }}
          >
            {secondaryValue}
          </span>
        )}
      </div>
    </div>
  );
}

export default MetricCard;
