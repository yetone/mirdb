/**
 * How It Works Section Component.
 * Owner: Scenario 5 - How It Works Section
 *
 * Displays the three-step process:
 * - Section heading: "How It Works"
 * - Step 1: "Paste Your URL" with link icon
 * - Step 2: "Get Short Link" with scissors icon
 * - Step 3: "Track Analytics" with chart icon
 * - Connecting arrows between steps
 * - Fade-in animation on scroll
 *
 * Requirements: REQ-6
 * Min height: 400px
 * Mobile: Steps stack vertically
 */
import { useRef } from 'react';
import { motion, useInView } from 'framer-motion';
import { Link, Scissors, BarChart3, ArrowRight, ArrowDown } from 'lucide-react';
import { ProcessStep } from './ProcessStep';
import { ProcessStepData } from '../../types/home';

const steps: ProcessStepData[] = [
  {
    stepNumber: 1,
    title: 'Paste Your URL',
    description: 'Enter any long URL you want to shorten',
    icon: <Link className="w-8 h-8" data-testid="link-icon" aria-label="Link icon" />,
  },
  {
    stepNumber: 2,
    title: 'Get Short Link',
    description: 'Instantly receive a compact, shareable link',
    icon: <Scissors className="w-8 h-8" data-testid="scissors-icon" aria-label="Scissors icon" />,
  },
  {
    stepNumber: 3,
    title: 'Track Analytics',
    description: 'Monitor clicks and engagement in real-time',
    icon: <BarChart3 className="w-8 h-8" data-testid="chart-icon" aria-label="Chart icon" />,
  },
];

export function HowItWorksSection() {
  const sectionRef = useRef<HTMLElement>(null);
  const isInView = useInView(sectionRef, { once: true, margin: '-100px' });

  const containerVariants = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        staggerChildren: 0.2,
        delayChildren: 0.1,
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
        ease: 'easeOut',
      },
    },
  };

  return (
    <section
      ref={sectionRef}
      className="how-it-works-section py-16 md:py-24 px-4"
      style={{ minHeight: '400px' }}
      aria-labelledby="how-it-works-heading"
      data-testid="how-it-works-section"
    >
      <div className="max-w-6xl mx-auto">
        {/* Section Heading */}
        <motion.h2
          id="how-it-works-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12 md:mb-16"
          initial={{ opacity: 0, y: -20 }}
          animate={isInView ? { opacity: 1, y: 0 } : { opacity: 0, y: -20 }}
          transition={{ duration: 0.5 }}
          data-testid="how-it-works-heading"
        >
          How It Works
        </motion.h2>

        {/* Steps Container */}
        <motion.div
          className="steps-container flex flex-col md:flex-row items-center justify-center gap-8 md:gap-4 lg:gap-8"
          variants={containerVariants}
          initial="hidden"
          animate={isInView ? 'visible' : 'hidden'}
          data-testid="steps-container"
          data-animate={isInView ? 'true' : 'false'}
        >
          {steps.map((step, index) => (
            <div
              key={step.stepNumber}
              className="step-wrapper flex flex-col md:flex-row items-center"
            >
              {/* Process Step */}
              <motion.div variants={itemVariants}>
                <ProcessStep {...step} />
              </motion.div>

              {/* Connecting Arrow (not after last step) */}
              {index < steps.length - 1 && (
                <motion.div
                  className="arrow-connector mx-4 my-4 md:my-0 text-primary/50"
                  variants={itemVariants}
                  aria-hidden="true"
                  data-testid={`arrow-${index + 1}`}
                >
                  {/* Vertical arrow on mobile, horizontal on desktop */}
                  <ArrowDown className="block md:hidden w-6 h-6" />
                  <ArrowRight className="hidden md:block w-6 h-6" />
                </motion.div>
              )}
            </div>
          ))}
        </motion.div>
      </div>
    </section>
  );
}
