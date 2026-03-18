/**
 * Features Showcase Section.
 * Owner: Scenario 2 - Features Showcase Section
 *
 * Displays a grid of feature cards showcasing MirDB's key capabilities:
 * - Tokio async runtime
 * - Memtable with skiplist
 * - Minor compaction
 * - Major compaction
 */

import { FeatureCard } from '../ui/FeatureCard';
import { features } from '../../data/features';
import type { Feature } from '../../types';

export interface FeaturesProps {
  featureData?: Feature[];
}

export function Features({ featureData = features }: FeaturesProps) {
  return (
    <section
      id="features"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-gray-900"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="features-heading"
          className="text-3xl font-bold text-center text-white mb-12"
        >
          Key Features
        </h2>
        <div
          className="grid grid-cols-1 md:grid-cols-2 gap-6"
          data-testid="features-grid"
        >
          {featureData.map((feature, index) => (
            <FeatureCard
              key={feature.title}
              {...feature}
              testId={`feature-card-${index}`}
            />
          ))}
        </div>
      </div>
    </section>
  );
}
