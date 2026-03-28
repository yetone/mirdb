import React from 'react'
import StepIndicator from '../shared/StepIndicator'
import { Step } from '../../types/homepage'

const steps: Step[] = [
  {
    number: 1,
    title: 'Paste Long URL',
    description: 'Enter or paste your long URL into our shortener. We accept any valid web address.',
  },
  {
    number: 2,
    title: 'Get Short Link',
    description: 'Instantly receive a compact, shareable short link that redirects to your original URL.',
  },
  {
    number: 3,
    title: 'Track Performance',
    description: 'Monitor clicks, analyze traffic sources, and track your link performance with detailed analytics.',
  },
]

const HowItWorksSection: React.FC = () => {
  return (
    <section
      className="py-16 px-4 bg-base-200"
      data-testid="how-it-works-section"
      aria-labelledby="how-it-works-heading"
    >
      <div className="container mx-auto">
        <h2
          id="how-it-works-heading"
          className="text-3xl font-bold text-center mb-12"
        >
          How It Works
        </h2>
        <div
          className="flex flex-col md:flex-row items-center justify-center gap-4 md:gap-0"
          data-testid="steps-container"
        >
          {steps.map((step, index) => (
            <StepIndicator
              key={step.number}
              stepNumber={step.number}
              title={step.title}
              description={step.description}
              isLast={index === steps.length - 1}
            />
          ))}
        </div>
      </div>
    </section>
  )
}

export default HowItWorksSection
