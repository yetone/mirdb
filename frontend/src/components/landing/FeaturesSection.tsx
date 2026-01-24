/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Displays 3-4 feature cards highlighting product capabilities:
 * - URL Shortening: Create short, memorable links
 * - Click Analytics: Track clicks with detailed analytics
 * - Share Stats: Generate public share links
 * - Dashboard Management: Manage all links in one place
 *
 * Requirements: REQ-2
 */

import { GlassMorphismCard } from '../GlassMorphismCard';
import type { Feature } from '../../types/landing';

const defaultFeatures: Feature[] = [
  {
    icon: 'link',
    title: 'URL Shortening',
    description: 'Create short, memorable links instantly. Transform long URLs into clean, shareable links.',
  },
  {
    icon: 'chart',
    title: 'Click Analytics',
    description: 'Track clicks with detailed analytics including referrers, browsers, and locations.',
  },
  {
    icon: 'share',
    title: 'Share Stats',
    description: 'Generate public share links for your stats. Let others see your link performance.',
  },
  {
    icon: 'dashboard',
    title: 'Dashboard Management',
    description: 'Manage all your links in one place. View, edit, and organize your shortened URLs.',
  },
];

interface FeaturesSectionProps {
  features?: Feature[];
}

function FeatureIcon({ icon }: { icon: string }) {
  const iconPaths: Record<string, JSX.Element> = {
    link: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-8 h-8"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M13.19 8.688a4.5 4.5 0 0 1 1.242 7.244l-4.5 4.5a4.5 4.5 0 1 1-6.364-6.364l1.757-1.757m13.35-.622 1.757-1.757a4.5 4.5 0 0 0-6.364-6.364l-4.5 4.5a4.5 4.5 0 0 0 1.242 7.244"
        />
      </svg>
    ),
    chart: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-8 h-8"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75ZM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625ZM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z"
        />
      </svg>
    ),
    share: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-8 h-8"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M7.217 10.907a2.25 2.25 0 1 0 0 2.186m0-2.186c.18.324.283.696.283 1.093s-.103.77-.283 1.093m0-2.186 9.566-5.314m-9.566 7.5 9.566 5.314m0 0a2.25 2.25 0 1 0 3.935 2.186 2.25 2.25 0 0 0-3.935-2.186Zm0-12.814a2.25 2.25 0 1 0 3.933-2.185 2.25 2.25 0 0 0-3.933 2.185Z"
        />
      </svg>
    ),
    dashboard: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-8 h-8"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M3.75 6A2.25 2.25 0 0 1 6 3.75h2.25A2.25 2.25 0 0 1 10.5 6v2.25a2.25 2.25 0 0 1-2.25 2.25H6a2.25 2.25 0 0 1-2.25-2.25V6ZM3.75 15.75A2.25 2.25 0 0 1 6 13.5h2.25a2.25 2.25 0 0 1 2.25 2.25V18a2.25 2.25 0 0 1-2.25 2.25H6A2.25 2.25 0 0 1 3.75 18v-2.25ZM13.5 6a2.25 2.25 0 0 1 2.25-2.25H18A2.25 2.25 0 0 1 20.25 6v2.25A2.25 2.25 0 0 1 18 10.5h-2.25a2.25 2.25 0 0 1-2.25-2.25V6ZM13.5 15.75a2.25 2.25 0 0 1 2.25-2.25H18a2.25 2.25 0 0 1 2.25 2.25V18A2.25 2.25 0 0 1 18 20.25h-2.25A2.25 2.25 0 0 1 13.5 18v-2.25Z"
        />
      </svg>
    ),
  };

  return (
    <div className="text-primary" data-testid={`feature-icon-${icon}`}>
      {iconPaths[icon] || iconPaths.link}
    </div>
  );
}

export function FeaturesSection({ features = defaultFeatures }: FeaturesSectionProps) {
  return (
    <section
      id="features"
      className="py-16 px-4 md:px-8 lg:px-16"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12 text-base-content"
        >
          Powerful Features
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {features.map((feature) => (
            <GlassMorphismCard
              key={feature.title}
              className="hover:scale-105 transition-transform duration-300"
            >
              <article
                className="flex flex-col items-center text-center"
                data-testid={`feature-card-${feature.title.toLowerCase().replace(/\s+/g, '-')}`}
              >
                <div className="mb-4">
                  <FeatureIcon icon={feature.icon} />
                </div>
                <h3 className="text-xl font-semibold mb-2 text-base-content">
                  {feature.title}
                </h3>
                <p className="text-base-content/70">
                  {feature.description}
                </p>
              </article>
            </GlassMorphismCard>
          ))}
        </div>
      </div>
    </section>
  );
}
