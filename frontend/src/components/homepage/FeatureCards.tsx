/**
 * Feature Cards Section Component
 * Owner: Scenario 4 - Feature Cards Section Display
 *
 * Displays 4-6 feature cards highlighting service capabilities
 *
 * Related Requirements: REQ-2, REQ-3
 */

import React from 'react';
import { motion } from 'framer-motion';
import GlassMorphismCard from '../GlassMorphismCard';

interface Feature {
  title: string;
  description: string;
  icon: string;
}

const features: Feature[] = [
  {
    title: 'URL Shortening',
    description: 'Create memorable, short links instantly from any long URL.',
    icon: '🔗',
  },
  {
    title: 'Click Analytics',
    description: 'Track every click with detailed statistics and insights.',
    icon: '📊',
  },
  {
    title: 'Geographic Insights',
    description: 'Know where your audience is located with GeoIP tracking.',
    icon: '🌍',
  },
  {
    title: 'Browser & Device Data',
    description: 'Understand how users access your links across platforms.',
    icon: '💻',
  },
  {
    title: 'Shareable Stats',
    description: 'Share analytics publicly with secure share tokens.',
    icon: '📤',
  },
  {
    title: 'Multi-Theme Support',
    description: 'Customize your experience with multiple theme options.',
    icon: '🎨',
  },
];

const FeatureCards: React.FC = () => {
  return (
    <section className="py-20 px-4">
      <div className="container mx-auto">
        <motion.h2
          className="text-3xl md:text-4xl font-bold text-center mb-12"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
        >
          Powerful Features
        </motion.h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {features.map((feature, index) => (
            <motion.div
              key={feature.title}
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: index * 0.1 }}
            >
              <GlassMorphismCard className="h-full">
                <div className="text-4xl mb-4">{feature.icon}</div>
                <h3 className="text-xl font-semibold mb-2">{feature.title}</h3>
                <p className="text-base-content/70">{feature.description}</p>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
};

export default FeatureCards;
