/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Requirements: REQ-3, REQ-4
 * User Stories: US-4
 *
 * Expected functionality:
 * - Grid layout: 3 columns desktop, 1 column mobile
 * - Feature cards using GlassMorphismCard style
 * - Each feature has: icon, title, description
 * - Required features:
 *   1. URL Shortening - create short, memorable links
 *   2. Click Analytics - track link performance
 *   3. Shareable Stats - share analytics with others
 * - Icons from Heroicons or Lucide
 * - Smooth scroll anchor: #features
 */

import { motion } from 'framer-motion';
import { Link2, BarChart3, Share2 } from 'lucide-react';
import { GlassMorphismCard } from '../GlassMorphismCard';

interface Feature {
  id: number;
  icon: React.ReactNode;
  title: string;
  description: string;
}

const features: Feature[] = [
  {
    id: 1,
    icon: <Link2 className="w-8 h-8" aria-hidden="true" />,
    title: 'URL Shortening',
    description:
      'Create short, memorable links in seconds. Transform long URLs into clean, shareable links that are easy to remember.',
  },
  {
    id: 2,
    icon: <BarChart3 className="w-8 h-8" aria-hidden="true" />,
    title: 'Click Analytics',
    description:
      'Track every click with detailed analytics. Monitor your link performance, see referrers, and analyze traffic in real-time.',
  },
  {
    id: 3,
    icon: <Share2 className="w-8 h-8" aria-hidden="true" />,
    title: 'Shareable Stats',
    description:
      'Share your link analytics with others. Generate public stats pages for complete transparency and collaboration.',
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
      aria-labelledby="features-heading"
      className="py-16 md:py-24 px-4 md:px-8"
    >
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-12">
          <h2
            id="features-heading"
            className="text-3xl md:text-4xl font-bold mb-4"
          >
            Powerful Features
          </h2>
          <p className="text-base-content/70 text-lg max-w-2xl mx-auto">
            Everything you need to manage, track, and share your links
            effectively.
          </p>
        </div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-3 gap-6 md:gap-8"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-100px' }}
        >
          {features.map((feature) => (
            <motion.div key={feature.id} variants={itemVariants}>
              <GlassMorphismCard
                className="p-6 h-full flex flex-col"
                data-testid="feature-card"
              >
                <div
                  className="w-14 h-14 rounded-lg bg-primary/10 flex items-center justify-center mb-4 text-primary"
                  data-testid="feature-icon"
                >
                  {feature.icon}
                </div>
                <h3 className="text-xl font-semibold mb-2">{feature.title}</h3>
                <p className="text-base-content/70 flex-grow">
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
