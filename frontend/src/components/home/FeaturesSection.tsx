/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Purpose: Showcase key product features with visual cards.
 */

import React from 'react'
import { FeatureCard } from '../shared/FeatureCard'
import {
  LinkIcon,
  ChartBarIcon,
  ArrowTrendingUpIcon,
  GlobeAltIcon,
} from '@heroicons/react/24/outline'
import type { Feature } from '../../types/home'

const features: Feature[] = [
  {
    icon: <LinkIcon className="w-10 h-10" />,
    title: 'URL Shortening',
    description: 'Create short, memorable links instantly. Transform long URLs into clean, shareable links.',
  },
  {
    icon: <ChartBarIcon className="w-10 h-10" />,
    title: 'Click Analytics',
    description: 'Track every click with detailed insights. Monitor performance in real-time.',
  },
  {
    icon: <ArrowTrendingUpIcon className="w-10 h-10" />,
    title: 'Referrer Tracking',
    description: 'Know where your traffic comes from. Understand your audience sources.',
  },
  {
    icon: <GlobeAltIcon className="w-10 h-10" />,
    title: 'GeoIP Location',
    description: 'See geographic distribution of clicks. Visualize your global reach.',
  },
]

export const FeaturesSection: React.FC = () => {
  return (
    <section className="py-16 px-4 md:px-8 lg:px-16">
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold text-base-content mb-4">
            Powerful Features
          </h2>
          <p className="text-lg text-base-content/70 max-w-2xl mx-auto">
            Everything you need to shorten, share, and track your links effectively.
          </p>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
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
