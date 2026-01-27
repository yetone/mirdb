/**
 * How It Works Section Component
 * Owner: Scenario 4 - How It Works Section
 *
 * Displays the 3-step process:
 * - Step 1: Paste your long URL
 * - Step 2: Get your short link instantly
 * - Step 3: Track clicks and analyze performance
 *
 * Visual requirements:
 * - Numbered steps with visual flow
 * - Each step has title and description
 * - Responsive layout (horizontal on desktop, vertical on mobile)
 */

import { GlassMorphismCard } from '../GlassMorphismCard'
import type { Step } from '../../types/homepage'

const defaultSteps: Step[] = [
  {
    number: 1,
    title: 'Paste your long URL',
    description:
      'Copy and paste any long URL into our shortener. We support all types of links from any website.',
  },
  {
    number: 2,
    title: 'Get your short link instantly',
    description:
      'Instantly receive a short, memorable link that\'s easy to share across all platforms.',
  },
  {
    number: 3,
    title: 'Track clicks and analyze performance',
    description:
      'Monitor your link performance with detailed analytics. See clicks, locations, and referrers in real-time.',
  },
]

interface StepCardProps {
  step: Step
}

function StepCard({ step }: StepCardProps) {
  return (
    <GlassMorphismCard className="flex-1 min-w-0">
      <div className="flex flex-col items-center text-center">
        <span
          className="w-12 h-12 rounded-full bg-primary text-primary-content flex items-center justify-center text-xl font-bold mb-4"
          aria-label={`Step ${step.number}`}
        >
          {step.number}
        </span>
        <h3 className="text-xl font-semibold mb-3">{step.title}</h3>
        <p className="text-base-content">{step.description}</p>
      </div>
    </GlassMorphismCard>
  )
}

function StepConnector() {
  return (
    <div
      className="hidden lg:flex items-center justify-center px-4"
      data-testid="step-connector"
    >
      <div className="w-12 h-0.5 bg-primary/50" />
      <div className="w-0 h-0 border-t-4 border-b-4 border-l-8 border-transparent border-l-primary/50" />
    </div>
  )
}

export function HowItWorksSection() {
  return (
    <section
      id="how-it-works"
      aria-label="How It Works"
      className="py-16 sm:py-24 px-4 sm:px-6 lg:px-8"
    >
      <div className="max-w-6xl mx-auto">
        <h2 className="text-3xl sm:text-4xl font-bold text-center mb-12">
          How It Works
        </h2>

        <div
          className="flex flex-col lg:flex-row items-stretch gap-6 lg:gap-0"
          data-testid="how-it-works-steps"
        >
          {defaultSteps.map((step, index) => (
            <div key={step.number} className="contents">
              <StepCard step={step} />
              {index < defaultSteps.length - 1 && <StepConnector />}
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

export default HowItWorksSection
