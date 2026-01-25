/**
 * Features Section Component
 * Owner: Scenario 5 - Features Section Display
 *
 * Displays a grid of feature cards showcasing key capabilities:
 * 1. URL Shortening - Transform long URLs into short links
 * 2. Click Analytics - Track clicks, referrers, engagement
 * 3. Geographic Insights - Location data for each click
 * 4. Shareable Stats - Generate public share tokens
 *
 * Layout:
 * - Desktop: 4-column horizontal row
 * - Tablet: 2x2 grid
 * - Mobile: Single column stack
 *
 * Uses: GlassMorphismCard component
 */

import React from 'react';
import { motion } from 'framer-motion';
import { GlassMorphismCard } from '../GlassMorphismCard';

interface Feature {
  id: string;
  icon: React.ReactNode;
  title: string;
  description: string;
}

const features: Feature[] = [
  {
    id: 'url-shortening',
    icon: (
      <svg
        className="w-10 h-10 text-primary"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
        data-testid="icon-url-shortening"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
        />
      </svg>
    ),
    title: 'URL Shortening',
    description:
      'Create clean, memorable short links in seconds. Share them anywhere and track their performance.',
  },
  {
    id: 'click-analytics',
    icon: (
      <svg
        className="w-10 h-10 text-primary"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
        data-testid="icon-click-analytics"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
        />
      </svg>
    ),
    title: 'Click Analytics',
    description:
      'Monitor every click in real-time. See referrers, browsers, and engagement patterns at a glance.',
  },
  {
    id: 'geographic-insights',
    icon: (
      <svg
        className="w-10 h-10 text-primary"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
        data-testid="icon-geographic-insights"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
        />
      </svg>
    ),
    title: 'Geographic Insights',
    description:
      'Discover where your audience lives. Get detailed location data for every click on your links.',
  },
  {
    id: 'shareable-stats',
    icon: (
      <svg
        className="w-10 h-10 text-primary"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
        data-testid="icon-shareable-stats"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.368 2.684 3 3 0 00-5.368-2.684z"
        />
      </svg>
    ),
    title: 'Shareable Stats',
    description:
      'Generate secure share tokens to show your analytics to others without exposing your account.',
  },
];

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.1,
    },
  },
};

const cardVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
    },
  },
};

export function FeaturesSection() {
  return (
    <section
      id="features"
      className="py-16 px-4 md:px-8"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="max-w-6xl mx-auto">
        <motion.h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
          initial={{ opacity: 0, y: -20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          Key Features
        </motion.h2>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <motion.div
              key={feature.id}
              variants={cardVariants}
              data-testid={`feature-card-${feature.id}`}
            >
              <GlassMorphismCard className="h-full flex flex-col items-center text-center">
                <div
                  className="mb-4"
                  data-testid={`feature-icon-${feature.id}`}
                >
                  {feature.icon}
                </div>
                <h3
                  className="text-xl font-semibold mb-2"
                  data-testid={`feature-title-${feature.id}`}
                >
                  {feature.title}
                </h3>
                <p
                  className="text-base-content/70"
                  data-testid={`feature-description-${feature.id}`}
                >
                  {feature.description}
                </p>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
}
