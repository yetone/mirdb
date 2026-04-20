/**
 * Features section component displaying service capabilities.
 * Owner: Scenario 4 - Features Section Display
 *
 * Renders a grid of feature cards showcasing key features:
 * - URL Shortening
 * - Click Analytics
 * - Geo-location Tracking
 * - User Dashboard
 */

import { motion } from 'framer-motion'
import FeatureCard from './FeatureCard'
import { LinkIcon, AnalyticsIcon, GeoIcon, DashboardIcon } from '../icons'

const features = [
  {
    id: 'url-shortening',
    icon: <LinkIcon size={48} />,
    title: 'URL Shortening',
    description: 'Transform long, unwieldy URLs into clean, memorable short links that are easy to share anywhere.',
  },
  {
    id: 'click-analytics',
    icon: <AnalyticsIcon size={48} />,
    title: 'Click Analytics',
    description: 'Track every click with detailed analytics. See how your links perform with real-time statistics.',
  },
  {
    id: 'geo-tracking',
    icon: <GeoIcon size={48} />,
    title: 'Geo-location Tracking',
    description: 'Understand your audience with geographic insights. See where your visitors are clicking from.',
  },
  {
    id: 'dashboard',
    icon: <DashboardIcon size={48} />,
    title: 'User Dashboard',
    description: 'Manage all your links in one place. A powerful dashboard to organize and monitor your URLs.',
  },
]

export default function FeaturesSection() {
  return (
    <section
      className="py-16 px-4 md:px-8 bg-base-100"
      aria-label="Features section"
      data-testid="features-section"
    >
      <div className="container mx-auto max-w-6xl">
        <motion.div
          className="text-center mb-12"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2 className="text-3xl md:text-4xl font-bold mb-4">
            Powerful Features
          </h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Everything you need to manage, track, and optimize your links
          </p>
        </motion.div>

        <div
          className="grid grid-cols-1 md:grid-cols-2 gap-6 lg:gap-8"
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <FeatureCard
              key={feature.id}
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
