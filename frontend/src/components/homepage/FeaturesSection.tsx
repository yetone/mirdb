import React from 'react'
import { FeatureCard } from './FeatureCard'
import { Link2, BarChart3, Shield, LayoutDashboard } from 'lucide-react'

/**
 * Features Section Component
 * Owner: Scenario 2 - Feature Highlights Section
 *
 * Displays a grid of feature cards highlighting core capabilities.
 *
 * Features to highlight:
 * - URL shortening
 * - Analytics and click tracking
 * - Secure authentication
 * - Dashboard management
 *
 * Requirements:
 * - Grid layout (responsive)
 * - Each feature has icon and description
 * - Uses GlassMorphismCard for cards (via FeatureCard)
 */

interface Feature {
  id: string
  icon: React.ReactNode
  title: string
  description: string
}

const features: Feature[] = [
  {
    id: 'url-shortening',
    icon: <Link2 className="w-10 h-10" />,
    title: 'URL Shortening',
    description: 'Transform long URLs into short, memorable links that are easy to share and track.'
  },
  {
    id: 'analytics',
    icon: <BarChart3 className="w-10 h-10" />,
    title: 'Analytics & Click Tracking',
    description: 'Monitor your link performance with detailed analytics, click tracking, and geographic data.'
  },
  {
    id: 'authentication',
    icon: <Shield className="w-10 h-10" />,
    title: 'Secure Authentication',
    description: 'Keep your links safe with robust authentication and user account management.'
  },
  {
    id: 'dashboard',
    icon: <LayoutDashboard className="w-10 h-10" />,
    title: 'Dashboard Management',
    description: 'Manage all your shortened URLs from a centralized, intuitive dashboard interface.'
  }
]

export const FeaturesSection: React.FC = () => {
  return (
    <section
      className="features-section py-16 px-4"
      data-testid="features-section"
      aria-labelledby="features-heading"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12 text-base-content"
        >
          Core Features
        </h2>

        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <FeatureCard
              key={feature.id}
              icon={feature.icon}
              title={feature.title}
              description={feature.description}
              data-testid={`feature-card-${feature.id}`}
            />
          ))}
        </div>
      </div>
    </section>
  )
}

export default FeaturesSection
