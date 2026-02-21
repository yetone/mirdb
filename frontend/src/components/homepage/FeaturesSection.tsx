/**
 * Features Section Component.
 * Owner: Scenario 3 - Features Section
 *
 * Expected behavior:
 * - Display 4 feature cards in 2x2 grid (desktop)
 * - Single column stack on mobile
 * - Each card: icon, title, brief description
 * - Features: URL Shortening, Analytics Dashboard, Secure & Reliable, Share Insights
 * - Consistent spacing and alignment
 *
 * Uses: FeatureCard component
 */

import FeatureCard from '../shared/FeatureCard';
import { FEATURES } from '../../utils/constants';

function getFeatureTestId(title: string): string {
  return `feature-card-${title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/, '')}`;
}

function FeaturesSection() {
  return (
    <section
      id="features"
      className="py-16 lg:py-24 px-4 bg-base-200"
      data-testid="features-section"
    >
      <div className="container mx-auto max-w-6xl">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold text-base-content mb-4">
            Powerful Features
          </h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Everything you need to create, manage, and analyze your short links.
          </p>
        </div>

        <div
          className="grid grid-cols-1 lg:grid-cols-2 gap-6 lg:gap-8"
          data-testid="features-grid"
        >
          {FEATURES.map((feature) => (
            <div
              key={feature.title}
              data-testid={getFeatureTestId(feature.title)}
            >
              <FeatureCard
                icon={feature.icon}
                title={feature.title}
                description={feature.description}
              />
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

export default FeaturesSection;
