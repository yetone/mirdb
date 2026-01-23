/**
 * How It Works Section Component
 * Owner: Scenario 5 - How It Works Section
 *
 * Three-step visual guide showing the URL shortening workflow
 *
 * Related Requirements: REQ-9
 */

import React from 'react';
import { motion } from 'framer-motion';

interface Step {
  number: number;
  title: string;
  description: string;
}

const steps: Step[] = [
  {
    number: 1,
    title: 'Paste your long URL',
    description: 'Simply paste your lengthy URL into our shortener tool.',
  },
  {
    number: 2,
    title: 'Get a short, memorable link',
    description: 'Instantly receive a compact, easy-to-share short link.',
  },
  {
    number: 3,
    title: 'Track clicks and gain insights',
    description: 'Monitor performance with detailed analytics and metrics.',
  },
];

const HowItWorks: React.FC = () => {
  return (
    <section className="py-20 px-4 bg-base-200">
      <div className="container mx-auto">
        <motion.h2
          className="text-3xl md:text-4xl font-bold text-center mb-12"
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
        >
          How It Works
        </motion.h2>
        <div className="flex flex-col md:flex-row justify-center items-center gap-8">
          {steps.map((step, index) => (
            <motion.div
              key={step.number}
              className="flex flex-col items-center text-center max-w-xs"
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: index * 0.2 }}
            >
              <div className="w-16 h-16 rounded-full bg-primary text-primary-content flex items-center justify-center text-2xl font-bold mb-4">
                {step.number}
              </div>
              <h3 className="text-xl font-semibold mb-2">{step.title}</h3>
              <p className="text-base-content/70">{step.description}</p>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
};

export default HowItWorks;
