import { Feature } from '../../types/homepage';
import { FEATURES } from '../../utils/constants';
import FeatureCard from './FeatureCard';

/**
 * Features grid section.
 * Owner: Scenario 3 - Feature Showcase Section.
 *
 * Reads FEATURES from constants by default and renders a semantic <section>
 * with an accessible heading. Each feature is rendered as a <FeatureCard />.
 * When an empty features array is supplied, an empty-state placeholder is
 * shown instead of crashing.
 */

export interface FeaturesSectionProps {
  features?: Feature[];
}

const SECTION_HEADING_ID = 'features-section-heading';

export default function FeaturesSection({ features = FEATURES }: FeaturesSectionProps) {
  const hasFeatures = features.length > 0;

  return (
    <section
      id="features"
      className="features py-16 px-4"
      aria-labelledby={SECTION_HEADING_ID}
      data-testid="features-section"
    >
      <div className="max-w-6xl mx-auto">
        <header className="text-center mb-10">
          <h2
            id={SECTION_HEADING_ID}
            className="text-3xl md:text-4xl font-bold mb-2"
            data-testid="features-section-heading"
          >
            Features
          </h2>
          <p className="text-base opacity-80 max-w-2xl mx-auto">
            Everything you need to shorten links, understand your audience, and
            manage your URLs from one dashboard.
          </p>
        </header>

        {hasFeatures ? (
          <div
            className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6"
            data-testid="features-grid"
            role="list"
          >
            {features.map((feature) => (
              <div role="listitem" key={feature.id}>
                <FeatureCard feature={feature} />
              </div>
            ))}
          </div>
        ) : (
          <p
            className="text-center opacity-70"
            data-testid="features-empty-state"
          >
            No features available yet. Check back soon.
          </p>
        )}
      </div>
    </section>
  );
}
