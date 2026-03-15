import React from 'react'
import FeatureCard from '../ui/FeatureCard'
import { FEATURES } from '../../utils/constants'

export default function Features() {
  return (
    <section
      id="features"
      className="py-16 sm:py-24 bg-gray-50 dark:bg-gray-900"
      aria-labelledby="features-heading"
    >
      <div className="section-container">
        <div className="text-center max-w-3xl mx-auto mb-12">
          <h2
            id="features-heading"
            className="section-heading"
          >
            Powerful Features
          </h2>
          <p className="section-description">
            MirDB combines the simplicity of Memcached with the durability of persistent storage,
            all built with Rust for maximum performance and safety.
          </p>
        </div>

        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
          data-testid="features-grid"
        >
          {FEATURES.map((feature) => (
            <FeatureCard
              key={feature.title}
              title={feature.title}
              description={feature.description}
              icon={feature.icon}
            />
          ))}
        </div>
      </div>
    </section>
  )
}
