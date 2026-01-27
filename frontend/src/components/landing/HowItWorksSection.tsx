/**
 * How It Works Section Component
 * Owner: Scenario 3 - How It Works Section
 *
 * Displays step-by-step guide to using the service:
 * 1. Sign up for an account
 * 2. Paste your long URL
 * 3. Get your short link
 * 4. Track clicks and analytics
 *
 * Each step includes:
 * - Step number indicator
 * - Title
 * - Brief description
 * - Optional icon/illustration
 *
 * Uses:
 * - Tailwind CSS for layout
 * - Framer Motion for sequential animations
 *
 * Props:
 * - steps?: StepItem[] - Optional custom steps array
 *
 * Types:
 * - StepItem: { number: number, title: string, description: string, icon?: ReactNode }
 */

import { motion } from 'framer-motion';
import { ReactNode } from 'react';
import {
  UserPlusIcon,
  LinkIcon,
  ClipboardDocumentCheckIcon,
  ChartBarIcon,
} from '@heroicons/react/24/outline';

export interface StepItem {
  number: number;
  title: string;
  description: string;
  icon?: ReactNode;
}

const defaultSteps: StepItem[] = [
  {
    number: 1,
    title: 'Sign Up',
    description: 'Create your free account in seconds. No credit card required.',
    icon: <UserPlusIcon className="w-8 h-8" />,
  },
  {
    number: 2,
    title: 'Paste Your URL',
    description: 'Enter any long URL you want to shorten.',
    icon: <LinkIcon className="w-8 h-8" />,
  },
  {
    number: 3,
    title: 'Get Short Link',
    description: 'Receive your shortened link instantly, ready to share.',
    icon: <ClipboardDocumentCheckIcon className="w-8 h-8" />,
  },
  {
    number: 4,
    title: 'Track Analytics',
    description: 'Monitor clicks, locations, and performance in real-time.',
    icon: <ChartBarIcon className="w-8 h-8" />,
  },
];

interface HowItWorksSectionProps {
  steps?: StepItem[];
}

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.2,
    },
  },
};

const stepVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
    },
  },
};

export default function HowItWorksSection({ steps = defaultSteps }: HowItWorksSectionProps) {
  return (
    <section
      id="how-it-works"
      className="py-16 px-4 sm:px-6 lg:px-8"
      aria-labelledby="how-it-works-heading"
    >
      <div className="max-w-6xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-center mb-12"
        >
          <h2
            id="how-it-works-heading"
            className="text-3xl sm:text-4xl font-bold mb-4"
          >
            How It Works
          </h2>
          <p className="text-lg text-base-content/70 max-w-2xl mx-auto">
            Get started with URL shortening in just a few simple steps
          </p>
        </motion.div>

        <motion.div
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8"
        >
          {steps.map((step) => (
            <motion.div
              key={step.number}
              variants={stepVariants}
              className="relative"
              data-testid={`step-${step.number}`}
            >
              <div className="flex flex-col items-center text-center p-6 rounded-xl bg-base-200/50 hover:bg-base-200 transition-colors">
                {/* Step Number Indicator */}
                <div
                  className="w-12 h-12 rounded-full bg-primary text-primary-content flex items-center justify-center text-xl font-bold mb-4"
                  data-testid={`step-number-${step.number}`}
                  aria-label={`Step ${step.number}`}
                >
                  {step.number}
                </div>

                {/* Icon */}
                {step.icon && (
                  <div className="text-primary mb-4" aria-hidden="true">
                    {step.icon}
                  </div>
                )}

                {/* Title */}
                <h3 className="text-xl font-semibold mb-2">{step.title}</h3>

                {/* Description */}
                <p className="text-base-content/70">{step.description}</p>
              </div>

              {/* Connector Line (hidden on mobile and last item) */}
              {step.number < steps.length && (
                <div
                  className="hidden lg:block absolute top-1/2 -right-4 w-8 h-0.5 bg-primary/30"
                  aria-hidden="true"
                />
              )}
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  );
}

// Named export for convenience
export { HowItWorksSection };
