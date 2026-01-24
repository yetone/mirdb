/**
 * How It Works Section Component
 * Owner: Scenario 4 - How It Works Section
 *
 * Displays a 3-step visual workflow:
 * 1. Paste your long URL
 * 2. Get a short, trackable link
 * 3. Monitor performance with analytics
 *
 * Requirements: REQ-4
 */

import type { HowItWorksSectionProps, WorkflowStep } from '../../types/landing'

const DEFAULT_STEPS: WorkflowStep[] = [
  {
    number: 1,
    title: 'Paste URL',
    description: 'Paste your long URL into our simple form',
  },
  {
    number: 2,
    title: 'Get Link',
    description: 'Get a short, trackable link instantly',
  },
  {
    number: 3,
    title: 'Track Clicks',
    description: 'Monitor performance with analytics',
  },
]

function StepIcon({ number }: { number: number }) {
  return (
    <div
      className="w-16 h-16 rounded-full bg-primary text-primary-content flex items-center justify-center text-2xl font-bold mb-4"
      aria-hidden="true"
    >
      {number}
    </div>
  )
}

function StepConnector() {
  return (
    <div
      className="hidden md:flex items-center justify-center flex-1 mx-4"
      aria-hidden="true"
    >
      <div className="w-full h-0.5 bg-primary/30 relative">
        <svg
          className="absolute right-0 top-1/2 -translate-y-1/2 translate-x-1/2 w-4 h-4 text-primary/50"
          fill="currentColor"
          viewBox="0 0 24 24"
        >
          <path d="M8.59 16.59L13.17 12 8.59 7.41 10 6l6 6-6 6-1.41-1.41z" />
        </svg>
      </div>
    </div>
  )
}

export function HowItWorksSection({ steps = DEFAULT_STEPS }: HowItWorksSectionProps) {
  return (
    <section
      id="how-it-works"
      className="py-16 px-4 md:px-8 lg:px-16 bg-base-200/50"
      aria-labelledby="how-it-works-heading"
      data-testid="how-it-works-section"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="how-it-works-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12 text-base-content"
          data-testid="how-it-works-heading"
        >
          How It Works
        </h2>

        <div
          className="flex flex-col md:flex-row items-center justify-center"
          role="list"
          aria-label="Workflow steps"
        >
          {steps.map((step, index) => (
            <div key={step.number} className="contents">
              <article
                className="flex flex-col items-center text-center max-w-xs"
                role="listitem"
                data-testid={`workflow-step-${step.number}`}
              >
                <StepIcon number={step.number} />
                <h3
                  className="text-xl font-semibold mb-2 text-base-content"
                  data-testid={`workflow-step-${step.number}-title`}
                >
                  {step.title}
                </h3>
                <p
                  className="text-base-content/70"
                  data-testid={`workflow-step-${step.number}-description`}
                >
                  {step.description}
                </p>
              </article>
              {index < steps.length - 1 && <StepConnector />}
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

export default HowItWorksSection
