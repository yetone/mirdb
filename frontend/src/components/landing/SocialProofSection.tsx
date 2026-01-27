/**
 * Social Proof Section Component.
 * Owner: Scenario 5 - Social Proof Section
 *
 * Requirements:
 * - Stats counters (animated or static)
 * - Metrics: Links Created, Clicks Tracked, Happy Users
 * - Trust-building social proof
 *
 * Expected exports:
 * - SocialProofSection: React.FC
 */

import React from 'react';
import { motion } from 'framer-motion';
import { Link2, MousePointerClick, Users } from 'lucide-react';

interface Stat {
  icon: React.ReactNode;
  value: string;
  label: string;
}

const stats: Stat[] = [
  {
    icon: <Link2 className="w-8 h-8 text-primary" aria-hidden="true" />,
    value: '10M+',
    label: 'Links Created',
  },
  {
    icon: <MousePointerClick className="w-8 h-8 text-secondary" aria-hidden="true" />,
    value: '500M+',
    label: 'Clicks Tracked',
  },
  {
    icon: <Users className="w-8 h-8 text-accent" aria-hidden="true" />,
    value: '100K+',
    label: 'Happy Users',
  },
];

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.15,
    },
  },
};

const itemVariants = {
  hidden: { opacity: 0, scale: 0.8 },
  visible: {
    opacity: 1,
    scale: 1,
    transition: {
      duration: 0.5,
      ease: 'easeOut',
    },
  },
};

export const SocialProofSection: React.FC = () => {
  return (
    <section
      id="social-proof"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-base-200/50"
      aria-labelledby="social-proof-heading"
      data-testid="social-proof-section"
    >
      <div className="max-w-7xl mx-auto">
        <motion.div
          className="text-center mb-12"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2
            id="social-proof-heading"
            className="text-3xl sm:text-4xl font-bold mb-4"
          >
            Trusted by Thousands
          </h2>
          <p className="text-lg text-base-content/70 max-w-2xl mx-auto">
            Join the growing community of users who trust us with their link management
          </p>
        </motion.div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-3 gap-8"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          data-testid="stats-grid"
        >
          {stats.map((stat, index) => (
            <motion.div
              key={index}
              variants={itemVariants}
              className="flex flex-col items-center text-center p-8 rounded-xl bg-base-100/50 backdrop-blur-sm border border-base-content/5"
              data-testid={`stat-card-${index}`}
            >
              <div className="mb-4" data-testid={`stat-icon-${index}`}>
                {stat.icon}
              </div>
              <span
                className="text-4xl sm:text-5xl font-bold mb-2"
                data-testid={`stat-value-${index}`}
              >
                {stat.value}
              </span>
              <span
                className="text-base-content/70 text-lg"
                data-testid={`stat-label-${index}`}
              >
                {stat.label}
              </span>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
};

export default SocialProofSection;
