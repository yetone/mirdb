/**
 * How It Works Section Component.
 * Owner: Scenario 3 - How It Works Section
 *
 * Requirements:
 * - Three-step visual flow
 * - Step 1: Create - Paste your long URL and get a short link instantly
 * - Step 2: Share - Share your shortened link anywhere
 * - Step 3: Track - Monitor clicks and analyze your audience
 *
 * Expected exports:
 * - HowItWorksSection: React.FC
 */

import React from 'react';
import { motion } from 'framer-motion';
import { Link2, Share2, BarChart3, ArrowRight } from 'lucide-react';

interface Step {
  number: number;
  title: string;
  description: string;
  icon: React.ReactNode;
}

const steps: Step[] = [
  {
    number: 1,
    title: 'Create',
    description: 'Paste your long URL and get a short link instantly',
    icon: <Link2 className="w-8 h-8" />,
  },
  {
    number: 2,
    title: 'Share',
    description: 'Share your shortened link anywhere',
    icon: <Share2 className="w-8 h-8" />,
  },
  {
    number: 3,
    title: 'Track',
    description: 'Monitor clicks and analyze your audience',
    icon: <BarChart3 className="w-8 h-8" />,
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

export const HowItWorksSection: React.FC = () => {
  return (
    <section
      id="how-it-works"
      className="py-20 px-4 bg-base-200/50"
      aria-labelledby="how-it-works-heading"
    >
      <div className="max-w-6xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-center mb-16"
        >
          <h2
            id="how-it-works-heading"
            className="text-3xl md:text-4xl font-bold mb-4"
          >
            How It Works
          </h2>
          <p className="text-base-content/70 text-lg max-w-2xl mx-auto">
            Get started in three simple steps
          </p>
        </motion.div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-3 gap-8 relative"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          data-testid="steps-container"
        >
          {/* Connection line for desktop */}
          <div className="hidden md:block absolute top-1/2 left-1/4 right-1/4 h-0.5 bg-gradient-to-r from-primary via-secondary to-accent -translate-y-1/2 z-0" />

          {steps.map((step, index) => (
            <motion.div
              key={step.number}
              className="relative z-10"
              variants={itemVariants}
              data-testid={`step-${step.number}`}
            >
              <div className="flex flex-col items-center text-center">
                {/* Step number badge */}
                <div className="relative mb-6">
                  <div className="w-20 h-20 rounded-full bg-gradient-to-br from-primary to-secondary flex items-center justify-center text-primary-content shadow-lg">
                    {step.icon}
                  </div>
                  <span className="absolute -top-2 -right-2 w-8 h-8 rounded-full bg-accent text-accent-content flex items-center justify-center font-bold text-sm">
                    {step.number}
                  </span>
                </div>

                {/* Step content */}
                <h3 className="text-xl font-semibold mb-3" data-testid={`step-${step.number}-title`}>
                  {step.title}
                </h3>
                <p className="text-base-content/70" data-testid={`step-${step.number}-description`}>
                  {step.description}
                </p>

                {/* Arrow between steps (mobile only) */}
                {index < steps.length - 1 && (
                  <div className="md:hidden mt-6">
                    <ArrowRight className="w-6 h-6 text-primary rotate-90" />
                  </div>
                )}
              </div>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
};

export default HowItWorksSection;
