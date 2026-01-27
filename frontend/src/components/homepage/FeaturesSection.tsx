/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section Implementation
 *
 * Grid layout displaying 4 key product features:
 * 1. URL Shortening - Create short, memorable links
 * 2. Click Analytics - Track engagement in real-time
 * 3. Geographic Insights - See where your audience is located
 * 4. Share Stats - Generate public links to share analytics
 *
 * Layout: 1 col mobile, 2 cols tablet, 4 cols desktop
 * Each feature uses GlassMorphismCard with icon, title, description
 *
 * Requirements covered: REQ-2
 */

import React from 'react'
import { Link2, BarChart3, Globe, Share2 } from 'lucide-react'
import { GlassMorphismCard } from '../GlassMorphismCard'

interface Feature {
  id: string
  icon: React.ReactNode
  title: string
  description: string
}

const features: Feature[] = [
  {
    id: 'url-shortening',
    icon: <Link2 className="w-8 h-8 text-primary" aria-hidden="true" />,
    title: 'URL Shortening',
    description: 'Create short, memorable links',
  },
  {
    id: 'click-analytics',
    icon: <BarChart3 className="w-8 h-8 text-primary" aria-hidden="true" />,
    title: 'Click Analytics',
    description: 'Track engagement in real-time',
  },
  {
    id: 'geographic-insights',
    icon: <Globe className="w-8 h-8 text-primary" aria-hidden="true" />,
    title: 'Geographic Insights',
    description: 'See where your audience is located',
  },
  {
    id: 'share-stats',
    icon: <Share2 className="w-8 h-8 text-primary" aria-hidden="true" />,
    title: 'Share Stats',
    description: 'Generate public links to share analytics',
  },
]

export const FeaturesSection: React.FC = () => {
  return (
    <section className="py-16 px-4 md:px-8" aria-labelledby="features-heading">
      <div className="max-w-7xl mx-auto">
        <h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
        >
          Key Features
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {features.map((feature) => (
            <GlassMorphismCard
              key={feature.id}
              className="p-6 flex flex-col items-center text-center hover:scale-105 transition-transform duration-300"
            >
              <div className="mb-4" data-testid={`icon-${feature.id}`}>
                {feature.icon}
              </div>
              <h3 className="text-xl font-semibold mb-2">{feature.title}</h3>
              <p className="text-base text-opacity-80">{feature.description}</p>
            </GlassMorphismCard>
          ))}
        </div>
      </div>
    </section>
  )
}

export default FeaturesSection
