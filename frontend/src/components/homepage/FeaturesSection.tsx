/**
 * Features Section Component
 * Owner: Scenario 4 - Features Section Display
 *
 * Displays three key feature cards using GlassMorphismCard:
 * - URL Shortening
 * - Analytics Dashboard
 * - Click Tracking
 *
 * Each card includes:
 * - Icon (Lucide icons)
 * - Title
 * - Description
 */

import React from 'react';
import { Link2, BarChart3, MousePointerClick } from 'lucide-react';
import { GlassMorphismCard } from '../GlassMorphismCard';
import { Feature } from '../../types/homepage';

const features: Feature[] = [
  {
    icon: Link2,
    title: 'URL Shortening',
    description: 'Transform long, unwieldy URLs into short, memorable links that are easy to share across any platform.',
  },
  {
    icon: BarChart3,
    title: 'Analytics Dashboard',
    description: 'Gain insights into your link performance with detailed analytics, geographic data, and referrer tracking.',
  },
  {
    icon: MousePointerClick,
    title: 'Click Tracking',
    description: 'Monitor every click in real-time. Track engagement patterns and optimize your campaigns for better results.',
  },
];

export function FeaturesSection() {
  return (
    <section
      className="py-20 px-4"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
        >
          Powerful Features
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {features.map((feature, index) => {
            const IconComponent = feature.icon;
            return (
              <GlassMorphismCard
                key={feature.title}
                className="flex flex-col items-center text-center hover:scale-105 transition-transform duration-300"
                data-testid={`feature-card-${index}`}
              >
                <div
                  className="w-14 h-14 rounded-full bg-primary/20 flex items-center justify-center mb-4"
                  data-testid={`feature-icon-${index}`}
                >
                  <IconComponent
                    className="w-7 h-7 text-primary"
                    aria-hidden="true"
                  />
                </div>
                <h3
                  className="text-xl font-semibold mb-3 text-base-content"
                  data-testid={`feature-title-${index}`}
                >
                  {feature.title}
                </h3>
                <p
                  className="text-base-content/70"
                  data-testid={`feature-description-${index}`}
                >
                  {feature.description}
                </p>
              </GlassMorphismCard>
            );
          })}
        </div>
      </div>
    </section>
  );
}
