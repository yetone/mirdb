/**
 * Statistics/Trust Section Component
 * Owner: Scenario 6 - Statistics and Trust Indicators Section
 *
 * Displays aggregate metrics to build credibility
 *
 * Related Requirements: REQ-10
 */

import React from 'react';
import { motion } from 'framer-motion';

interface Stat {
  value: string;
  label: string;
}

const stats: Stat[] = [
  { value: '10M+', label: 'URLs Shortened' },
  { value: '500M+', label: 'Clicks Tracked' },
  { value: '150+', label: 'Countries Reached' },
];

const StatsSection: React.FC = () => {
  return (
    <section className="py-20 px-4">
      <div className="container mx-auto">
        <motion.h2
          className="text-3xl md:text-4xl font-bold text-center mb-12"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
        >
          Trusted by Millions
        </motion.h2>
        <div className="flex flex-col md:flex-row justify-center items-center gap-8 md:gap-16">
          {stats.map((stat, index) => (
            <motion.div
              key={stat.label}
              className="text-center"
              initial={{ opacity: 0, scale: 0.8 }}
              whileInView={{ opacity: 1, scale: 1 }}
              viewport={{ once: true }}
              transition={{ delay: index * 0.1 }}
            >
              <div className="text-4xl md:text-5xl font-bold text-primary mb-2">
                {stat.value}
              </div>
              <div className="text-base-content/70">{stat.label}</div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
};

export default StatsSection;
