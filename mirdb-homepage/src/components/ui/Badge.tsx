import React from 'react';
import { CRATES_URL } from '../../utils/constants';
import './Badge.css';

/**
 * Badge UI Component
 * Owner: Scenario 8 - Rust Crate Badge Display
 *
 * Displays crates.io badge with version:
 * - Links to crates.io page
 * - Shows current version number
 * - Accessible link with proper attributes
 */

export interface BadgeProps {
  label: string;
  value: string;
  href?: string;
  variant?: 'default' | 'crates' | 'success' | 'info';
  className?: string;
  testId?: string;
}

/**
 * Generic Badge component for displaying label-value pairs
 */
export const Badge: React.FC<BadgeProps> = ({
  label,
  value,
  href,
  variant = 'default',
  className = '',
  testId,
}) => {
  const badgeContent = (
    <span
      className={`badge badge--${variant} ${className}`.trim()}
      data-testid={testId}
    >
      <span className="badge__label" data-testid={testId ? `${testId}-label` : undefined}>
        {label}
      </span>
      <span className="badge__value" data-testid={testId ? `${testId}-value` : undefined}>
        {value}
      </span>
    </span>
  );

  if (href) {
    return (
      <a
        href={href}
        className="badge__link"
        target="_blank"
        rel="noopener noreferrer"
        aria-label={`${label}: ${value}`}
        data-testid={testId ? `${testId}-link` : undefined}
      >
        {badgeContent}
      </a>
    );
  }

  return badgeContent;
};

export interface CratesBadgeProps {
  version: string;
  crateName?: string;
  className?: string;
}

/**
 * CratesBadge - Specific crates.io badge component
 * Displays crate version and links to crates.io page
 */
export const CratesBadge: React.FC<CratesBadgeProps> = ({
  version,
  crateName = 'mirdb',
  className = '',
}) => {
  const cratesUrl = crateName === 'mirdb' ? CRATES_URL : `https://crates.io/crates/${crateName}`;

  return (
    <Badge
      label="crates.io"
      value={`v${version.replace(/^v/, '')}`}
      href={cratesUrl}
      variant="crates"
      className={className}
      testId="crates-badge"
    />
  );
};

export default Badge;
