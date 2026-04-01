/**
 * Features Section Component.
 * Owner: Scenario 3 - Features Section
 */

import { features } from '../../data/features'
import { FeatureCard } from './FeatureCard'

export function Features() {
  return (
    <section
      id="features"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-gray-50 dark:bg-gray-900"
      aria-labelledby="features-heading"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-12">
          <h2
            id="features-heading"
            className="text-3xl font-bold text-gray-900 dark:text-white mb-4"
          >
            Features
          </h2>
          <p className="text-gray-600 dark:text-gray-300 max-w-2xl mx-auto">
            MirDB provides a robust set of features for high-performance key-value storage
          </p>
        </div>
        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <FeatureCard
              key={feature.id}
              icon={feature.icon}
              title={feature.title}
              description={feature.description}
              status={feature.status}
            />
          ))}
        </div>
      </div>
    </section>
  )
}
