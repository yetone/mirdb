/**
 * Features Section Component
 * Owner: Scenario 3 - Features Section Display
 *
 * Displays a grid of feature cards highlighting key capabilities:
 * - URL Shortening
 * - Click Analytics
 * - Link Management
 * - Share Statistics
 *
 * Expected props:
 * - features: Feature[] (optional, uses defaults if not provided)
 *
 * Requirements: REQ-2, REQ-4, US-4
 */

import { Link2, BarChart3, FolderOpen, Share2 } from 'lucide-react';
import { FeatureCard } from './FeatureCard';
import type { Feature } from '../../types/homepage';

/** Default features for the URL shortening service */
const defaultFeatures: Feature[] = [
  {
    id: 'url-shortening',
    title: 'URL Shortening',
    description: 'Create short, memorable links instantly. Transform long URLs into clean, shareable links that are easy to remember and track.',
    icon: <Link2 className="w-8 h-8" />,
  },
  {
    id: 'analytics',
    title: 'Click Analytics',
    description: 'Track every click with detailed insights. Monitor referrers, browsers, devices, and geographic locations of your visitors.',
    icon: <BarChart3 className="w-8 h-8" />,
  },
  {
    id: 'link-management',
    title: 'Link Management',
    description: 'Organize and manage all your shortened URLs in one place. Keep your links organized with a powerful dashboard.',
    icon: <FolderOpen className="w-8 h-8" />,
  },
  {
    id: 'share-statistics',
    title: 'Share Statistics',
    description: 'Generate public share links for your analytics. Let others view your link performance with customizable share options.',
    icon: <Share2 className="w-8 h-8" />,
  },
];

export interface FeaturesSectionProps {
  features?: Feature[];
}

/**
 * FeaturesSection - Grid display of product feature cards
 * Showcases the core capabilities of the URL shortening service.
 */
export function FeaturesSection({ features = defaultFeatures }: FeaturesSectionProps) {
  return (
    <section
      data-testid="features-section"
      className="py-16 md:py-24"
      aria-labelledby="features-heading"
    >
      {/* Section header */}
      <div className="text-center mb-12 md:mb-16">
        <h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-white mb-4"
        >
          Powerful Features
        </h2>
        <p className="text-gray-300 text-lg max-w-2xl mx-auto">
          Everything you need to shorten, track, and manage your links in one place.
        </p>
      </div>

      {/* Feature cards grid */}
      <div
        data-testid="feature-cards-grid"
        className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 md:gap-8"
      >
        {features.map((feature) => (
          <FeatureCard
            key={feature.id}
            title={feature.title}
            description={feature.description}
            icon={feature.icon}
          />
        ))}
      </div>
    </section>
  );
}
