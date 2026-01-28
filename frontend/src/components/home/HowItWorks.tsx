/**
 * How It Works Section Component
 * Owner: Scenario 5 - How It Works Section
 *
 * Explains the three-step user flow:
 * 1. Paste your long URL
 * 2. Get a short, shareable link
 * 3. Track performance with detailed analytics
 *
 * Visual requirements:
 * - Numbered steps (1, 2, 3) or visual icons
 * - Clear visual hierarchy
 * - Responsive layout
 */
import { motion } from 'framer-motion'

interface Step {
  id: string
  number: number
  icon: React.ReactNode
  title: string
  description: string
}

const steps: Step[] = [
  {
    id: 'create-url',
    number: 1,
    icon: (
      <svg
        className="w-10 h-10 text-primary"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
        />
      </svg>
    ),
    title: 'Paste Your URL',
    description:
      'Simply paste your long URL into the input field. No sign-up required to get started.',
  },
  {
    id: 'get-short-link',
    number: 2,
    icon: (
      <svg
        className="w-10 h-10 text-primary"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
        />
      </svg>
    ),
    title: 'Get Your Short Link',
    description:
      'Instantly receive a compact, shareable link. Copy it with one click and share anywhere.',
  },
  {
    id: 'track-analytics',
    number: 3,
    icon: (
      <svg
        className="w-10 h-10 text-primary"
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
    title: 'Track Performance',
    description:
      'Monitor clicks, analyze traffic sources, and track geographic data with detailed analytics.',
  },
]

export function HowItWorks() {
  return (
    <section
      data-testid="how-it-works-section"
      className="py-16 px-4 bg-base-200/50"
      aria-labelledby="how-it-works-heading"
    >
      <div className="max-w-6xl mx-auto">
        <motion.h2
          id="how-it-works-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          How It Works
        </motion.h2>

        <ol
          className="grid grid-cols-1 md:grid-cols-3 gap-8 md:gap-12"
          role="list"
          aria-label="Steps to use the URL shortening service"
        >
          {steps.map((step, index) => (
            <motion.li
              key={step.id}
              data-testid={`step-${step.number}`}
              className="relative flex flex-col items-center text-center"
              role="listitem"
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ duration: 0.5, delay: index * 0.15 }}
            >
              <div
                className="flex items-center justify-center w-16 h-16 rounded-full bg-primary/10 mb-4"
                data-testid={`step-icon-${step.number}`}
              >
                <span
                  className="absolute -top-2 -right-2 w-8 h-8 flex items-center justify-center rounded-full bg-primary text-primary-content font-bold text-lg"
                  data-testid={`step-number-${step.number}`}
                  aria-hidden="true"
                >
                  {step.number}
                </span>
                {step.icon}
              </div>
              <h3 className="text-xl font-semibold mb-2">{step.title}</h3>
              <p className="text-base-content/70">{step.description}</p>
            </motion.li>
          ))}
        </ol>
      </div>
    </section>
  )
}
