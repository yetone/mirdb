/**
 * Features Section Component.
 * Owner: Scenario 2 - Features Section Display
 *
 * Requirements:
 * - Grid layout (3 columns on desktop, 1 column on mobile)
 * - Three feature cards using GlassMorphismCard
 * - Features: Instant URL Shortening, Detailed Analytics, Share Statistics
 * - Each card: icon, title, description
 *
 * Expected exports:
 * - FeaturesSection: React.FC
 */

import React from 'react';
import { motion } from 'framer-motion';
import { Link2, BarChart3, Share2 } from 'lucide-react';
import GlassMorphismCard from '../GlassMorphismCard';

interface Feature {
  icon: React.ReactNode;
  title: string;
  description: string;
}

const features: Feature[] = [
  {
    icon: <Link2 className="w-12 h-12 text-primary" aria-hidden="true" />,
    title: 'Instant URL Shortening',
    description: 'Create short, memorable links in seconds. Transform long URLs into clean, shareable links instantly.',
  },
  {
    icon: <BarChart3 className="w-12 h-12 text-secondary" aria-hidden="true" />,
    title: 'Detailed Analytics',
    description: 'Track clicks, referrers, browsers, and locations. Gain deep insights into your audience.',
  },
  {
    icon: <Share2 className="w-12 h-12 text-accent" aria-hidden="true" />,
    title: 'Share Statistics',
    description: 'Generate public share links for your analytics. Showcase your link performance with ease.',
  },
];

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.2,
    },
  },
};

const itemVariants = {
  hidden: { opacity: 0, y: 30 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.6,
      ease: 'easeOut',
    },
  },
};

export const FeaturesSection: React.FC = () => {
  return (
    <section
      id="features"
      className="py-20 px-4 sm:px-6 lg:px-8"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="max-w-7xl mx-auto">
        <motion.div
          className="text-center mb-16"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2
            id="features-heading"
            className="text-3xl sm:text-4xl font-bold mb-4"
          >
            Powerful Features
          </h2>
          <p className="text-lg text-base-content/70 max-w-2xl mx-auto">
            Everything you need to create, manage, and analyze your shortened URLs
          </p>
        </motion.div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-3 gap-8"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          data-testid="features-grid"
        >
          {features.map((feature, index) => (
            <motion.div key={index} variants={itemVariants}>
              <GlassMorphismCard className="p-8 h-full">
                <div
                  className="flex flex-col items-center text-center"
                  data-testid={`feature-card-${index}`}
                >
                  <div className="mb-6" data-testid={`feature-icon-${index}`}>
                    {feature.icon}
                  </div>
                  <h3
                    className="text-xl font-semibold mb-3"
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
                </div>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
};

export default FeaturesSection;
