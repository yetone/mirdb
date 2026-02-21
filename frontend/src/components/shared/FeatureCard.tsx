/**
 * Feature Card Component.
 * Owner: Scenario 3 - Features Section
 *
 * Reusable card component for displaying features.
 *
 * Props:
 * - icon: string (icon name: 'link', 'chart', 'shield', 'share')
 * - title: string
 * - description: string
 * - className?: string
 */

interface FeatureCardProps {
  icon: string;
  title: string;
  description: string;
  className?: string;
}

function FeatureIcon({ icon }: { icon: string }) {
  const iconMap: Record<string, JSX.Element> = {
    link: (
      <svg
        className="w-8 h-8"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
        />
      </svg>
    ),
    chart: (
      <svg
        className="w-8 h-8"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
        />
      </svg>
    ),
    shield: (
      <svg
        className="w-8 h-8"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"
        />
      </svg>
    ),
    share: (
      <svg
        className="w-8 h-8"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.368 2.684 3 3 0 00-5.368-2.684z"
        />
      </svg>
    ),
  };

  return (
    <div
      className="w-12 h-12 rounded-lg bg-primary/10 flex items-center justify-center text-primary"
      data-testid="feature-card-icon"
    >
      {iconMap[icon] || iconMap.link}
    </div>
  );
}

function FeatureCard({ icon, title, description, className = '' }: FeatureCardProps) {
  return (
    <div
      className={`card bg-base-100 shadow-md hover:shadow-lg transition-shadow p-6 ${className}`}
      data-testid="feature-card"
    >
      <FeatureIcon icon={icon} />
      <h3
        className="text-lg font-semibold text-base-content mt-4"
        data-testid="feature-card-title"
      >
        {title}
      </h3>
      <p
        className="text-base-content/70 mt-2"
        data-testid="feature-card-description"
      >
        {description}
      </p>
    </div>
  );
}

export default FeatureCard;
