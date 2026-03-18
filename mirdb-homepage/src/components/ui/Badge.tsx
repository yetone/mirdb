/**
 * Status Badge Component.
 * Owner: Scenario 5 - Status Badges and Project Health
 *
 * Props:
 * - imageUrl: string - Badge image URL
 * - linkUrl: string - Link to CI/service
 * - alt: string - Alt text for accessibility
 *
 * Expected exports:
 * - Badge: React.FC<BadgeProps>
 */

import type { BadgeProps } from '../../types';

export function Badge({ imageUrl, linkUrl, alt }: BadgeProps) {
  return (
    <a
      href={linkUrl}
      target="_blank"
      rel="noopener noreferrer"
      className="inline-block transition-opacity hover:opacity-80"
      data-testid="status-badge-link"
    >
      <img
        src={imageUrl}
        alt={alt}
        className="h-5 md:h-6"
        data-testid="status-badge-image"
        loading="lazy"
      />
    </a>
  );
}
