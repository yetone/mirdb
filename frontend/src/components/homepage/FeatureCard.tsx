import { Feature } from '../../types/homepage';

/**
 * Single feature card.
 * Owner: Scenario 3 - Feature Showcase Section.
 *
 * Renders a decorative icon (aria-hidden), a 2-5 word title, and a short
 * description. The icon is paired with a visible text label, so per WCAG 2.1 AA
 * we hide it from assistive technology to avoid duplicate announcements.
 */

export interface FeatureCardProps {
  feature: Feature;
}

function renderIcon(icon: Feature['icon']) {
  if (typeof icon !== 'string') {
    return icon;
  }

  switch (icon) {
    case 'link':
      return (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth={2}
          strokeLinecap="round"
          strokeLinejoin="round"
          width={32}
          height={32}
          data-testid="feature-icon-link"
        >
          <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />
          <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" />
        </svg>
      );
    case 'chart':
      return (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth={2}
          strokeLinecap="round"
          strokeLinejoin="round"
          width={32}
          height={32}
          data-testid="feature-icon-chart"
        >
          <line x1="12" y1="20" x2="12" y2="10" />
          <line x1="18" y1="20" x2="18" y2="4" />
          <line x1="6" y1="20" x2="6" y2="16" />
        </svg>
      );
    case 'dashboard':
      return (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth={2}
          strokeLinecap="round"
          strokeLinejoin="round"
          width={32}
          height={32}
          data-testid="feature-icon-dashboard"
        >
          <rect x="3" y="3" width="7" height="9" />
          <rect x="14" y="3" width="7" height="5" />
          <rect x="14" y="12" width="7" height="9" />
          <rect x="3" y="16" width="7" height="5" />
        </svg>
      );
    case 'share':
      return (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth={2}
          strokeLinecap="round"
          strokeLinejoin="round"
          width={32}
          height={32}
          data-testid="feature-icon-share"
        >
          <circle cx="18" cy="5" r="3" />
          <circle cx="6" cy="12" r="3" />
          <circle cx="18" cy="19" r="3" />
          <line x1="8.59" y1="13.51" x2="15.42" y2="17.49" />
          <line x1="15.41" y1="6.51" x2="8.59" y2="10.49" />
        </svg>
      );
    default:
      return (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth={2}
          strokeLinecap="round"
          strokeLinejoin="round"
          width={32}
          height={32}
          data-testid="feature-icon-default"
        >
          <circle cx="12" cy="12" r="10" />
        </svg>
      );
  }
}

export default function FeatureCard({ feature }: FeatureCardProps) {
  const titleId = `feature-${feature.id}-title`;

  return (
    <article
      className="card bg-base-200 shadow-md p-6 flex flex-col items-start gap-3"
      aria-labelledby={titleId}
      data-testid={`feature-card-${feature.id}`}
    >
      <span
        className="text-primary"
        aria-hidden="true"
        data-testid={`feature-card-icon-${feature.id}`}
      >
        {renderIcon(feature.icon)}
      </span>
      <h3
        id={titleId}
        className="text-xl font-semibold"
        data-testid={`feature-card-title-${feature.id}`}
      >
        {feature.title}
      </h3>
      <p
        className="text-base opacity-80"
        data-testid={`feature-card-description-${feature.id}`}
      >
        {feature.description}
      </p>
    </article>
  );
}
