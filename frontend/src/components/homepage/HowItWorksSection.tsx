/**
 * How It Works Section Component
 * Owner: Scenario 5 - How It Works Section
 *
 * This component renders the three-step process explanation.
 *
 * Expected features:
 * - Three steps with visual progression
 * - Step 1: Create/Paste your long URL
 * - Step 2: Share your short link
 * - Step 3: Track performance with analytics
 * - Icons and step numbers
 * - Connecting visual elements between steps
 * - Section ID for anchor navigation (#how-it-works)
 */

import { motion } from 'framer-motion'
import { GlassMorphismCard } from '../GlassMorphismCard'
import type { Step } from '@/types/homepage'

const steps: Step[] = [
  {
    number: 1,
    title: 'Create',
    description: 'Paste your long URL and get a short, memorable link instantly',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-8 w-8"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
        />
      </svg>
    ),
  },
  {
    number: 2,
    title: 'Share',
    description: 'Share your short link anywhere - social media, emails, or messages',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-8 w-8"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
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
    title: 'Track',
    description: 'Monitor clicks, analyze traffic sources, and measure performance',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-8 w-8"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
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
]

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.2,
    },
  },
}

const itemVariants = {
  hidden: { opacity: 0, y: 30 },
  visible: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.5 },
  },
}

export function HowItWorksSection() {
  return (
    <section
      id="how-it-works"
      data-testid="how-it-works-section"
      className="py-20 px-4"
      aria-labelledby="how-it-works-heading"
    >
      <div className="container mx-auto max-w-6xl">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-center mb-16"
        >
          <h2
            id="how-it-works-heading"
            data-testid="how-it-works-heading"
            className="text-3xl md:text-4xl font-bold mb-4"
          >
            How It Works
          </h2>
          <p className="text-base-content/70 text-lg max-w-2xl mx-auto">
            Get started in three simple steps
          </p>
        </motion.div>

        <motion.div
          data-testid="how-it-works-steps"
          className="grid grid-cols-1 md:grid-cols-3 gap-8 relative"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
        >
          {/* Connecting line (hidden on mobile) */}
          <div
            className="hidden md:block absolute top-1/2 left-0 right-0 h-0.5 bg-gradient-to-r from-primary via-secondary to-accent -translate-y-1/2 z-0"
            aria-hidden="true"
          />

          {steps.map((step) => (
            <motion.div
              key={step.number}
              data-testid={`step-${step.number}`}
              variants={itemVariants}
              className="relative z-10"
            >
              <GlassMorphismCard className="p-6 h-full text-center">
                <div
                  data-testid={`step-${step.number}-icon`}
                  className="w-16 h-16 rounded-full bg-primary/20 flex items-center justify-center mx-auto mb-4 text-primary"
                  aria-hidden="true"
                >
                  {step.icon}
                </div>
                <div
                  data-testid={`step-${step.number}-number`}
                  className="inline-flex items-center justify-center w-8 h-8 rounded-full bg-primary text-primary-content text-sm font-bold mb-3"
                  aria-label={`Step ${step.number}`}
                >
                  {step.number}
                </div>
                <h3
                  data-testid={`step-${step.number}-title`}
                  className="text-xl font-semibold mb-2"
                >
                  {step.title}
                </h3>
                <p
                  data-testid={`step-${step.number}-description`}
                  className="text-base-content/70"
                >
                  {step.description}
                </p>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}
