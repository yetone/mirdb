/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Requirements covered:
 * - REQ-2: Present features section showcasing 3 key product capabilities
 * - REQ-8: Render glassmorphism card components for feature presentation
 *
 * Expected exports:
 * - FeaturesSection: React.FC - Features showcase component
 *
 * Features to display:
 * 1. URL Shortening - Link icon - "Create short, memorable URLs"
 * 2. Analytics Dashboard - Chart icon - "Track clicks, locations, referrers"
 * 3. Link Management - Grid icon - "Organize and manage all your URLs"
 *
 * Layout:
 * - 3-column grid on desktop
 * - Single column on mobile
 * - Glass-effect cards with staggered entrance animations
 */

interface Feature {
  id: string;
  title: string;
  description: string;
  icon: React.ReactNode;
}

// Link icon SVG
const LinkIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    className="h-8 w-8"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
    />
  </svg>
);

// Chart icon SVG
const ChartIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    className="h-8 w-8"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
    />
  </svg>
);

// Grid icon SVG
const GridIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    className="h-8 w-8"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"
    />
  </svg>
);

const features: Feature[] = [
  {
    id: 'url-shortening',
    title: 'URL Shortening',
    description: 'Create short, memorable URLs from long links instantly. Share them anywhere with ease.',
    icon: <LinkIcon />,
  },
  {
    id: 'analytics-dashboard',
    title: 'Analytics Dashboard',
    description: 'Track clicks, locations, referrers, and browser statistics with detailed insights.',
    icon: <ChartIcon />,
  },
  {
    id: 'link-management',
    title: 'Link Management',
    description: 'Organize and manage all your shortened URLs in one place with powerful tools.',
    icon: <GridIcon />,
  },
];

export function FeaturesSection() {
  return (
    <section
      className="py-16 md:py-24 px-4"
      aria-labelledby="features-heading"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
        >
          Powerful Features
        </h2>

        {/* Grid: 1 column mobile, 3 columns desktop */}
        <div
          className="grid grid-cols-1 md:grid-cols-3 gap-6 md:gap-8"
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <article
              key={feature.id}
              className="backdrop-blur-md bg-base-100/30 bg-opacity-30 rounded-2xl p-6 md:p-8 border border-base-content/10 shadow-lg"
              data-testid="feature-card"
            >
              <div className="flex flex-col items-center text-center space-y-4">
                {/* Icon */}
                <div className="text-primary" data-testid="feature-icon">
                  {feature.icon}
                </div>

                {/* Title */}
                <h3 className="text-xl font-semibold">{feature.title}</h3>

                {/* Description */}
                <p className="text-base-content/70">{feature.description}</p>
              </div>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}
