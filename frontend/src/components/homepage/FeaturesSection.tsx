/**
 * Features Section Component
 * Owner: Scenario 5 - Features Section Display
 *
 * Displays grid of feature cards:
 * - 3-5 core features (URL shortening, analytics, dashboard, etc.)
 * - Responsive grid (3 cols desktop, 2 tablet, 1 mobile)
 * - Analytics feature as differentiator
 *
 * Requirements: REQ-4, US-3
 */

import { FeatureCard } from './FeatureCard'
import type { FeatureItem } from '../../types/homepage'

/**
 * Default features for the URL shortening service.
 * Includes analytics as a key differentiator (US-3).
 */
const defaultFeatures: FeatureItem[] = [
  {
    icon: '🔗',
    title: 'Lightning Fast URL Shortening',
    description: 'Transform long URLs into short, memorable links instantly. No sign-up required to get started.',
  },
  {
    icon: '📊',
    title: 'Powerful Analytics',
    description: 'Track clicks, geographic data, and referral sources. Gain insights into how your links perform.',
  },
  {
    icon: '📱',
    title: 'QR Code Generation',
    description: 'Generate QR codes for any shortened URL. Perfect for print materials and offline marketing.',
  },
  {
    icon: '⚡',
    title: 'Custom Aliases',
    description: 'Create branded, memorable links with custom aliases. Make your URLs stand out.',
  },
  {
    icon: '🎯',
    title: 'Dashboard Control',
    description: 'Manage all your links from a single dashboard. Edit, delete, and organize with ease.',
  },
]

export interface FeaturesSectionProps {
  features?: FeatureItem[]
  className?: string
}

export function FeaturesSection({ features = defaultFeatures, className = '' }: FeaturesSectionProps) {
  return (
    <section
      id="features-section"
      data-testid="features-section"
      className={`py-16 bg-base-200 ${className}`}
    >
      <div className="container mx-auto px-4">
        <div className="text-center mb-12">
          <h2
            data-testid="features-heading"
            className="text-3xl md:text-4xl font-bold mb-4"
          >
            Everything You Need
          </h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Powerful tools to shorten, track, and optimize your links. Start free, upgrade when you need more.
          </p>
        </div>

        <div
          data-testid="features-grid"
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
        >
          {features.map((feature, index) => (
            <FeatureCard
              key={index}
              icon={feature.icon}
              title={feature.title}
              description={feature.description}
            />
          ))}
        </div>
      </div>
    </section>
  )
}

export default FeaturesSection
