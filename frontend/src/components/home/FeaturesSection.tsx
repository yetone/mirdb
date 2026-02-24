/**
 * Features Section Component
 * Owner: Scenario 6 - Features Section Display
 *
 * Displays feature cards in a grid layout highlighting key capabilities.
 *
 * Expected exports:
 * - FeaturesSection: React.FC
 *
 * Features to display:
 * - Analytics Tracking - Track clicks and analyze performance
 * - Dashboard Management - Manage all shortened URLs
 * - Share Tokens - Share analytics publicly
 * - Dark Mode - Multiple theme support
 *
 * Uses GlassMorphismCard component for card styling.
 */

import { GlassMorphismCard } from '../ui/GlassMorphismCard'
import type { Feature } from '../../types/home'

const features: Feature[] = [
  {
    icon: '📊',
    title: 'Analytics',
    description:
      'Track clicks and analyze performance with detailed analytics. Monitor your link engagement in real-time.',
  },
  {
    icon: '🎛️',
    title: 'Dashboard',
    description:
      'Manage all your shortened URLs from a centralized dashboard. Organize, edit, and delete links with ease.',
  },
  {
    icon: '🔗',
    title: 'Share Tokens',
    description:
      'Share your link analytics publicly with secure share tokens. Collaborate with your team effortlessly.',
  },
  {
    icon: '🌙',
    title: 'Dark Mode',
    description:
      'Enjoy multiple theme options including dark mode. Customize the interface to match your preferences.',
  },
]

export interface FeatureCardProps {
  feature: Feature
}

export function FeatureCard({ feature }: FeatureCardProps) {
  return (
    <GlassMorphismCard
      className="p-6 h-full transition-transform hover:scale-105"
      data-testid="feature-card"
    >
      <div className="flex flex-col items-center text-center space-y-4">
        <span
          className="text-4xl"
          role="img"
          aria-label={`${feature.title} icon`}
        >
          {feature.icon}
        </span>
        <h3 className="text-xl font-semibold text-base-content">
          {feature.title}
        </h3>
        <p className="text-base-content/70">{feature.description}</p>
      </div>
    </GlassMorphismCard>
  )
}

export function FeaturesSection() {
  return (
    <section
      className="py-16 px-4"
      aria-label="Features section"
      data-testid="features-section"
    >
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold mb-4 text-base-content">
            Powerful Features
          </h2>
          <p className="text-lg text-base-content/70 max-w-2xl mx-auto">
            Everything you need to create, manage, and track your shortened URLs
            effectively.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {features.map((feature) => (
            <FeatureCard key={feature.title} feature={feature} />
          ))}
        </div>
      </div>
    </section>
  )
}

export default FeaturesSection
