/**
 * Features section component for homepage.
 * Owner: Scenario 3 - Features Section Display
 *
 * Displays feature highlights in a grid layout:
 * - URL Shortening
 * - Analytics Dashboard
 * - Click Tracking
 * - Secure & Reliable
 *
 * Uses GlassMorphismCard for styling via FeatureCard component.
 */

import React from 'react'
import { Link2, BarChart3, MousePointerClick, Shield } from 'lucide-react'
import FeatureCard from './FeatureCard'
import { Feature } from '@/types/homepage'

const features: Feature[] = [
  {
    id: 'url-shortening',
    icon: <Link2 className="w-8 h-8" aria-hidden="true" />,
    title: 'URL Shortening',
    description:
      'Transform long, unwieldy URLs into short, memorable links in seconds. Share them anywhere with ease and track their performance over time.',
  },
  {
    id: 'analytics-dashboard',
    icon: <BarChart3 className="w-8 h-8" aria-hidden="true" />,
    title: 'Analytics Dashboard',
    description:
      'Gain deep insights into your link performance with our comprehensive analytics dashboard. View detailed statistics, geographic data, and referral sources.',
  },
  {
    id: 'click-tracking',
    icon: <MousePointerClick className="w-8 h-8" aria-hidden="true" />,
    title: 'Click Tracking',
    description:
      'Monitor every click on your shortened links in real-time. Understand when and where your audience is engaging with your content.',
  },
  {
    id: 'secure-reliable',
    icon: <Shield className="w-8 h-8" aria-hidden="true" />,
    title: 'Secure & Reliable',
    description:
      'Your links are protected with enterprise-grade security. Our infrastructure ensures 99.9% uptime so your links are always accessible.',
  },
]

const FeaturesSection: React.FC = () => {
  return (
    <section
      className="py-20 px-4"
      data-testid="features-section"
      aria-label="Features"
    >
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold mb-4">
            Powerful Features
          </h2>
          <p className="text-lg text-base-content/80 max-w-2xl mx-auto">
            Everything you need to shorten, share, and track your links effectively.
          </p>
        </div>
        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <div key={feature.id} data-testid={`feature-card-${feature.id}`}>
              <FeatureCard
                icon={feature.icon}
                title={feature.title}
                description={feature.description}
              />
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

export default FeaturesSection
