/**
 * Features Section component.
 * Owner: Scenario 2 - Feature Showcase Section
 *
 * Displays 5 key product features in a responsive grid layout.
 * Features: URL Shortening, Click Analytics, Dashboard, Real-time Tracking, QR Codes
 * Supports both light and dark themes with proper contrast.
 */
import React from 'react'
import { Link2, BarChart3, LayoutDashboard, Activity, QrCode } from 'lucide-react'
import { FeatureCard } from '../shared/FeatureCard'
import { Feature } from '../../types/homepage'

const features: Feature[] = [
  {
    icon: <Link2 size={40} />,
    title: 'URL Shortening',
    description:
      'Transform long, unwieldy URLs into clean, memorable short links. Share them anywhere with ease and track their performance.',
  },
  {
    icon: <BarChart3 size={40} />,
    title: 'Click Analytics',
    description:
      'Gain deep insights into your link performance. Track clicks, geographic data, and referrer information in real-time.',
  },
  {
    icon: <LayoutDashboard size={40} />,
    title: 'Dashboard',
    description:
      'Manage all your shortened links from one intuitive dashboard. Organize, edit, and monitor your entire link portfolio.',
  },
  {
    icon: <Activity size={40} />,
    title: 'Real-time Tracking',
    description:
      'Watch your links perform in real-time. Get instant notifications and live updates on click activity as it happens.',
  },
  {
    icon: <QrCode size={40} />,
    title: 'QR Codes',
    description:
      'Generate QR codes for any shortened link. Perfect for print materials, presentations, and offline marketing campaigns.',
  },
]

export const FeaturesSection: React.FC = () => {
  return (
    <section
      id="features"
      className="py-16 px-4 bg-base-200"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="container mx-auto max-w-6xl">
        <div className="text-center mb-12">
          <h2
            id="features-heading"
            className="text-3xl md:text-4xl font-bold text-base-content mb-4"
            data-testid="features-heading"
          >
            Powerful Features
          </h2>
          <p className="text-base-content/70 text-lg max-w-2xl mx-auto">
            Everything you need to create, manage, and track your shortened URLs in one place.
          </p>
        </div>

        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
          data-testid="features-grid"
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
