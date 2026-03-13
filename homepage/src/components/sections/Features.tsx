/**
 * Features Section Component
 * Owner: Scenario 4 - Features Section Display
 *
 * Displays key MirDB features in a grid of cards.
 * Implements REQ-3: Display key features including persistent storage,
 * Memcached protocol, LSM-tree, WAL, and atomic compaction.
 */

import { Card } from '../ui/Card';
import { FEATURES } from '../../utils/constants';

export function Features() {
  return (
    <section
      id="features"
      data-testid="features"
      aria-labelledby="features-heading"
      className="py-20 bg-gray-50 dark:bg-gray-800"
    >
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <h2
          id="features-heading"
          className="text-3xl font-bold text-center text-gray-900 dark:text-white mb-12"
        >
          Features
        </h2>
        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
          {FEATURES.map((feature) => (
            <Card
              key={feature.id}
              title={feature.title}
              description={feature.description}
            />
          ))}
        </div>
      </div>
    </section>
  );
}
