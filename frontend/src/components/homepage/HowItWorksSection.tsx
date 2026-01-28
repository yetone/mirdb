/**
 * How It Works Section Component.
 * Owner: Scenario 8 - How It Works Section
 *
 * Three-step visual guide:
 * 1. Paste your long URL
 * 2. Get your short link
 * 3. Track performance
 *
 * Requirements:
 * - REQ-7: Include "How it works" section explaining the process
 */

import React from 'react'
import type { HowItWorksStep, HowItWorksSectionProps } from '@/types/homepage'

const defaultSteps: HowItWorksStep[] = [
  {
    step: 1,
    icon: '📋',
    title: 'Paste your long URL',
    description: 'Enter any long URL you want to shorten.',
  },
  {
    step: 2,
    icon: '✨',
    title: 'Get your short link',
    description: 'Instantly receive a short, memorable link.',
  },
  {
    step: 3,
    icon: '📈',
    title: 'Track performance',
    description: 'Monitor clicks, locations, and referrers.',
  },
]

export function HowItWorksSection({ steps = defaultSteps }: HowItWorksSectionProps) {
  return (
    <section className="py-16 px-4 bg-base-200" aria-labelledby="how-it-works-title">
      <div className="max-w-4xl mx-auto">
        <h2 id="how-it-works-title" className="text-3xl font-bold text-center mb-12">
          How It Works
        </h2>
        <div className="flex flex-col md:flex-row gap-8 justify-center">
          {steps.map((step, index) => (
            <div key={index} className="flex-1 text-center">
              <div className="text-5xl mb-4">{step.icon}</div>
              <div className="badge badge-primary mb-2">Step {step.step}</div>
              <h3 className="text-xl font-semibold mb-2">{step.title}</h3>
              <p className="text-base-content/70">{step.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
