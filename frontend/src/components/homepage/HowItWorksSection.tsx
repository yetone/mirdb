/**
 * How It Works Section Component
 * Owner: Scenario 4 - How It Works Section
 *
 * Displays step-by-step process:
 * - Step 1: Sign up for free
 * - Step 2: Paste long URL, get short link
 * - Step 3: Share and track performance
 *
 * Each step is numbered and visually sequenced.
 *
 * Requirements: REQ-4, US-5
 */

import { motion } from 'framer-motion'

export interface Step {
  id: string
  number: number
  title: string
  description: string
  icon: React.ReactNode
}

export interface HowItWorksSectionProps {
  steps?: Step[]
}

const defaultSteps: Step[] = [
  {
    id: 'signup',
    number: 1,
    title: 'Sign Up for Free',
    description:
      'Create your account in seconds. No credit card required. Get started with unlimited link shortening right away.',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-8 w-8"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        strokeWidth={2}
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M18 9v3m0 0v3m0-3h3m-3 0h-3m-2-5a4 4 0 11-8 0 4 4 0 018 0zM3 20a6 6 0 0112 0v1H3v-1z"
        />
      </svg>
    ),
  },
  {
    id: 'shorten',
    number: 2,
    title: 'Paste Your URL',
    description:
      'Simply paste your long URL and get a short, memorable link instantly. Customize your short links for better branding.',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-8 w-8"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        strokeWidth={2}
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
        />
      </svg>
    ),
  },
  {
    id: 'track',
    number: 3,
    title: 'Share & Track Analytics',
    description:
      'Share your shortened links everywhere. Track clicks, locations, referrers, and more with powerful real-time analytics.',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-8 w-8"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        strokeWidth={2}
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
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

const stepVariants = {
  hidden: { opacity: 0, y: 30 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
    },
  },
}

export function HowItWorksSection({ steps = defaultSteps }: HowItWorksSectionProps) {
  return (
    <section
      id="how-it-works"
      className="py-16 px-4 bg-base-200/30"
      aria-labelledby="how-it-works-heading"
      data-testid="how-it-works-section"
    >
      <div className="max-w-6xl mx-auto">
        <motion.div
          className="text-center mb-12"
          initial={{ opacity: 0, y: -20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2
            id="how-it-works-heading"
            className="text-3xl md:text-4xl font-bold mb-4 text-base-content"
            data-testid="how-it-works-heading"
          >
            How It Works
          </h2>
          <p className="text-lg text-base-content/70 max-w-2xl mx-auto">
            Get started in three simple steps and begin tracking your links today.
          </p>
        </motion.div>

        <motion.div
          className="relative"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          data-testid="steps-container"
        >
          {/* Connector line for visual flow */}
          <div
            className="hidden md:block absolute top-1/2 left-0 right-0 h-0.5 bg-gradient-to-r from-primary/20 via-primary to-primary/20 -translate-y-1/2 z-0"
            data-testid="step-connector"
            aria-hidden="true"
          />

          <div className="grid grid-cols-1 md:grid-cols-3 gap-8 relative z-10">
            {steps.map((step, index) => (
              <motion.div
                key={step.id}
                className="flex flex-col items-center text-center"
                variants={stepVariants}
                data-testid="step-item"
              >
                {/* Step number circle */}
                <div
                  className="w-16 h-16 rounded-full bg-primary text-primary-content flex items-center justify-center text-2xl font-bold mb-4 shadow-lg relative"
                  data-testid="step-number"
                  aria-label={`Step ${step.number}`}
                >
                  {step.number}
                </div>

                {/* Step card */}
                <div className="card bg-base-100 border border-base-300 shadow-md w-full">
                  <div className="card-body items-center">
                    {/* Icon */}
                    <div
                      className="text-primary mb-2"
                      data-testid="step-icon"
                      aria-hidden="true"
                    >
                      {step.icon}
                    </div>

                    {/* Title */}
                    <h3
                      className="card-title text-lg font-semibold text-base-content"
                      data-testid="step-title"
                    >
                      {step.title}
                    </h3>

                    {/* Description */}
                    <p
                      className="text-base-content/70 text-sm"
                      data-testid="step-description"
                    >
                      {step.description}
                    </p>
                  </div>
                </div>

                {/* Arrow connector for mobile */}
                {index < steps.length - 1 && (
                  <div
                    className="md:hidden my-4 text-primary"
                    data-testid="mobile-connector"
                    aria-hidden="true"
                  >
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      className="h-8 w-8"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      strokeWidth={2}
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        d="M19 14l-7 7m0 0l-7-7m7 7V3"
                      />
                    </svg>
                  </div>
                )}
              </motion.div>
            ))}
          </div>
        </motion.div>
      </div>
    </section>
  )
}

export default HowItWorksSection
