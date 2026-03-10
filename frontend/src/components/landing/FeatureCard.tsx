/**
 * Feature Card Component
 * Owner: Scenario 4 - Features Section Display
 *
 * Individual feature card with icon, title, and description.
 * Uses glassmorphism styling with Framer Motion hover animations.
 */

import { motion } from 'framer-motion';
import type { Feature } from '../../types/landing';

export interface FeatureCardProps {
  feature: Feature;
}

/**
 * FeatureCard displays a single feature with icon, title, and description.
 * Uses glassmorphism styling and smooth hover animations via Framer Motion.
 */
export function FeatureCard({ feature }: FeatureCardProps) {
  const IconComponent = feature.icon;

  return (
    <motion.article
      className="glass-card group relative overflow-hidden rounded-xl p-6 backdrop-blur-md
                 bg-base-200/50 border border-base-content/10
                 transition-colors duration-300"
      data-testid={`feature-card-${feature.id}`}
      whileHover={{
        scale: 1.02,
        boxShadow: '0 20px 40px rgba(0, 0, 0, 0.15)',
      }}
      transition={{
        type: 'spring',
        stiffness: 300,
        damping: 20,
      }}
    >
      {/* Highlight overlay on hover */}
      <motion.div
        className="absolute inset-0 bg-primary/5 opacity-0 transition-opacity duration-300
                   group-hover:opacity-100"
        aria-hidden="true"
      />

      <div className="relative z-10">
        {/* Icon */}
        <div
          className="mb-4 inline-flex items-center justify-center w-12 h-12 rounded-lg
                     bg-primary/10 text-primary"
          data-testid={`feature-icon-${feature.id}`}
        >
          <IconComponent />
        </div>

        {/* Title */}
        <h3
          className="text-lg font-semibold text-base-content mb-2"
          data-testid={`feature-title-${feature.id}`}
        >
          {feature.title}
        </h3>

        {/* Description */}
        <p
          className="text-sm text-base-content/70 leading-relaxed"
          data-testid={`feature-description-${feature.id}`}
        >
          {feature.description}
        </p>
      </div>
    </motion.article>
  );
}
