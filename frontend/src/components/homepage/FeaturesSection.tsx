/**
 * Features Section Component
 * Owner: Scenario 3 - Features Section
 *
 * Displays feature cards using GlassMorphismCard:
 * - URL Shortening feature with icon and description
 * - Analytics Dashboard feature with icon and description
 * - Click Tracking feature with icon and description
 *
 * Layout:
 * - Three-column for desktop (lg:grid-cols-3)
 * - Single column for mobile
 */

import { GlassMorphismCard } from '../GlassMorphismCard'
import type { Feature } from '../../types/homepage'

export interface FeaturesSectionProps {
  features?: Feature[]
}

const defaultFeatures: Feature[] = [
  {
    icon: '🔗',
    title: 'URL Shortening',
    description:
      'Transform long, unwieldy URLs into clean, shareable short links in seconds. Perfect for social media, emails, and marketing campaigns.',
  },
  {
    icon: '📊',
    title: 'Analytics Dashboard',
    description:
      'Gain deep insights into your link performance with our comprehensive analytics dashboard. Track clicks, geographic data, and referral sources.',
  },
  {
    icon: '📈',
    title: 'Click Tracking',
    description:
      'Monitor every click in real-time. Understand when and where your audience engages with your links to optimize your campaigns.',
  },
]

export function FeaturesSection({ features = defaultFeatures }: FeaturesSectionProps) {
  return (
    <section
      id="features"
      aria-label="Features"
      className="py-16 sm:py-24 px-4 sm:px-6 lg:px-8"
    >
      <div className="max-w-6xl mx-auto">
        <h2 className="text-3xl sm:text-4xl font-bold text-center mb-12">
          Powerful Features
        </h2>

        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 lg:gap-8"
          data-testid="features-grid"
        >
          {features.map((feature, index) => (
            <GlassMorphismCard key={index} className="h-full">
              <div className="flex flex-col items-center text-center h-full">
                <span
                  className="text-4xl mb-4"
                  role="img"
                  aria-label={`${feature.title} icon`}
                >
                  {feature.icon}
                </span>
                <h3 className="text-xl font-semibold mb-3">{feature.title}</h3>
                <p className="text-base-content/70">{feature.description}</p>
              </div>
            </GlassMorphismCard>
          ))}
        </div>
      </div>
    </section>
  )
}

export default FeaturesSection
