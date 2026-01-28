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

const defaultFeatures: Feature[] = [
  {
    icon: '🔗',
    title: 'URL Shortening',
    description: 'Create short, memorable links instantly from any long URL.',
  },
  {
    icon: '📊',
    title: 'Click Analytics',
    description: 'Track clicks with detailed metrics and real-time updates.',
  },
  {
    icon: '🌍',
    title: 'Geographic Insights',
    description: 'See where your audience is located around the world.',
  },
  {
    icon: '📤',
    title: 'Share Statistics',
    description: 'Generate public share links to showcase your link performance.',
  },
]

export function FeaturesSection({ features = defaultFeatures }: FeaturesSectionProps) {
  return (
    <section className="py-16 px-4 bg-base-100" aria-labelledby="features-title">
      <div className="max-w-6xl mx-auto">
        <h2 id="features-title" className="text-3xl font-bold text-center mb-12">
          Powerful Features
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {features.map((feature, index) => (
            <GlassMorphismCard key={index} className="p-6">
              <div className="text-4xl mb-4">{feature.icon}</div>
              <h3 className="text-xl font-semibold mb-2">{feature.title}</h3>
              <p className="text-base-content/70">{feature.description}</p>
            </GlassMorphismCard>
          ))}
        </div>
      </div>
    </section>
  )
}
