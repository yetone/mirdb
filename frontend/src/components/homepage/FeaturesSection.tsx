/**
 * Features Section Component.
 * Owner: Scenario 2 - Features Section Display
 *
 * Showcases key capabilities with feature cards:
 * - URL Shortening: Create short, memorable links
 * - Click Analytics: Track clicks with detailed metrics
 * - Geographic Insights: See where your audience is located
 * - Share Statistics: Generate public share links for stats
 *
 * Uses GlassMorphismCard component for consistent styling.
 *
 * Requirements:
 * - REQ-3: Showcase key features in a features section
 */

import React from 'react'
import type { Feature, FeaturesSectionProps } from '@/types/homepage'
import { GlassMorphismCard } from '@/components/GlassMorphismCard'

interface FeatureWithTestId extends Feature {
  testId: string
}

const defaultFeatures: FeatureWithTestId[] = [
  {
    icon: '🔗',
    title: 'URL Shortening',
    description: 'Create short, memorable links instantly from any long URL.',
    testId: 'feature-card-url-shortening',
  },
  {
    icon: '📊',
    title: 'Click Analytics',
    description: 'Track clicks with detailed metrics and real-time updates.',
    testId: 'feature-card-click-analytics',
  },
  {
    icon: '🌍',
    title: 'Geographic Insights',
    description: 'See where your audience is located around the world.',
    testId: 'feature-card-geographic-insights',
  },
  {
    icon: '📤',
    title: 'Share Statistics',
    description: 'Generate public share links for stats to showcase your performance.',
    testId: 'feature-card-share-statistics',
  },
]

export function FeaturesSection({ features }: FeaturesSectionProps) {
  const displayFeatures = features || defaultFeatures

  return (
    <section
      data-testid="features-section"
      className="py-16 px-4 bg-base-100"
      aria-labelledby="features-title"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="features-title"
          className="text-3xl font-bold text-center mb-12"
        >
          Powerful Features
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {displayFeatures.map((feature, index) => {
            const testId = 'testId' in feature
              ? (feature as FeatureWithTestId).testId
              : `feature-card-${index}`
            return (
              <div
                key={index}
                data-testid={testId}
                className="backdrop-blur-md bg-base-100/30 border border-base-content/10 rounded-2xl shadow-xl"
              >
                <GlassMorphismCard className="p-6 h-full">
                  <div
                    data-testid="feature-icon"
                    className="text-4xl mb-4"
                  >
                    {feature.icon}
                  </div>
                  <h3 className="text-xl font-semibold mb-2">{feature.title}</h3>
                  <p className="text-base-content/70">{feature.description}</p>
                </GlassMorphismCard>
              </div>
            )
          })}
        </div>
      </div>
    </section>
  )
}

export default FeaturesSection
