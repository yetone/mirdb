/**
 * Features Section Component
 * Owner: Scenario 4 - Features Section Display
 *
 * Displays 4 feature cards in a responsive grid layout.
 * Features: URL Shortening, Detailed Analytics, Shareable Stats, Secure Authentication
 *
 * Layout:
 * - 1 column on mobile (< 640px)
 * - 2 columns on tablet (>= 768px)
 * - 4 columns on desktop (>= 1024px)
 */

import { motion } from 'framer-motion';
import { HiLink, HiChartBar, HiShare, HiShieldCheck } from 'react-icons/hi';
import { FEATURES_CONTENT } from '../../constants/landingContent';
import { FeatureCard } from './FeatureCard';
import type { Feature } from '../../types/landing';

// Map feature IDs to their respective icons
const FEATURE_ICONS: Record<string, React.ComponentType> = {
  shorten: HiLink,
  analytics: HiChartBar,
  share: HiShare,
  secure: HiShieldCheck,
};

/**
 * Build features array with icons from content constants
 */
function getFeatures(): Feature[] {
  return FEATURES_CONTENT.items.map((item) => ({
    id: item.id,
    icon: FEATURE_ICONS[item.id] || HiLink,
    title: item.title,
    description: item.description,
  }));
}

// Animation variants for staggered card appearance
const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.1,
    },
  },
} as const;

const itemVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      type: 'spring' as const,
      stiffness: 100,
      damping: 12,
    },
  },
} as const;

/**
 * FeaturesSection displays the product features in a responsive grid.
 */
export function FeaturesSection() {
  const features = getFeatures();

  return (
    <section
      id="features"
      className="py-16 px-4 sm:px-6 lg:px-8"
      aria-labelledby="features-title"
      data-testid="features-section"
    >
      <div className="max-w-7xl mx-auto">
        {/* Section Title */}
        <h2
          id="features-title"
          className="text-3xl sm:text-4xl font-bold text-center text-base-content mb-12"
          data-testid="features-title"
        >
          {FEATURES_CONTENT.title}
        </h2>

        {/* Features Grid */}
        <motion.div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-100px' }}
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <motion.div key={feature.id} variants={itemVariants}>
              <FeatureCard feature={feature} />
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
}
