/**
 * Status Badge Component
 * Owner: Scenario 5 - CI/CD Status Badges
 *
 * Displays CI/CD status badges:
 * - Badge image with alt text
 * - Link to CI dashboard
 */
import type { StatusBadgeProps } from '../../types';
import './Badges.css';

export function StatusBadge({ src, alt, href }: StatusBadgeProps) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="status-badge-link"
      data-testid="status-badge"
    >
      <img
        src={src}
        alt={alt}
        className="status-badge-image"
        data-testid="status-badge-image"
      />
    </a>
  );
}
