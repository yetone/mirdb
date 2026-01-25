/**
 * How It Works Section Component
 * Owner: Scenario 6 - How It Works Section
 *
 * Displays a 3-step process explaining the service:
 * 1. Paste your long URL
 * 2. Share your short link anywhere
 * 3. Watch the analytics roll in
 *
 * Layout:
 * - Desktop: Horizontal flow with connecting lines
 * - Mobile: Vertical stack
 *
 * Requirements covered:
 * - REQ-5: How It Works section with 3 steps
 */

import React from 'react';
import { motion } from 'framer-motion';
import { GlassMorphismCard } from '../GlassMorphismCard';

interface Step {
  number: number;
  title: string;
  description: string;
  icon: React.ReactNode;
}

const steps: Step[] = [
  {
    number: 1,
    title: 'Paste Your URL',
    description: 'Enter your long, unwieldy URL into our shortener. Any valid URL works.',
    icon: (
      <svg
        className="w-8 h-8"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M9 5H7a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"
        />
      </svg>
    ),
  },
  {
    number: 2,
    title: 'Share Your Link',
    description: 'Get a clean, short link instantly. Share it anywhere—social media, emails, or messages.',
    icon: (
      <svg
        className="w-8 h-8"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.368 2.684 3 3 0 00-5.368-2.684z"
        />
      </svg>
    ),
  },
  {
    number: 3,
    title: 'Track Analytics',
    description: 'Watch the analytics roll in. See clicks, locations, and engagement in real-time.',
    icon: (
      <svg
        className="w-8 h-8"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
        />
      </svg>
    ),
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

export function HowItWorksSection() {
  return (
    <section
      className="py-20 px-4"
      data-testid="how-it-works-section"
      aria-labelledby="how-it-works-title"
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
            id="how-it-works-title"
            className="text-3xl md:text-4xl font-bold mb-4 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
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
        >
          {/* Connecting lines (desktop only) */}
          <div className="hidden md:block absolute top-1/2 left-1/4 right-1/4 h-0.5 bg-gradient-to-r from-primary/30 via-secondary/30 to-primary/30 -translate-y-1/2 z-0" />

          {steps.map((step) => (
            <motion.div
              key={step.number}
              variants={itemVariants}
              className="relative z-10"
              data-testid={`step-${step.number}`}
            >
              <GlassMorphismCard className="h-full text-center">
                <div className="flex flex-col items-center">
                  {/* Step number badge */}
                  <div
                    className="w-12 h-12 rounded-full bg-primary text-primary-content flex items-center justify-center text-xl font-bold mb-4"
                    aria-hidden="true"
                  >
                    {step.number}
                  </div>

                  {/* Icon */}
                  <div className="text-primary mb-4" aria-hidden="true">
                    {step.icon}
                  </div>

                  {/* Title */}
                  <h3 className="text-xl font-semibold mb-2">{step.title}</h3>

                  {/* Description */}
                  <p className="text-base-content/70">{step.description}</p>
                </div>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
}
