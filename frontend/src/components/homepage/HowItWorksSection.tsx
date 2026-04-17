/**
 * How It Works Section Component
 * Owner: Scenario 4 - How It Works Section
 *
 * 3-step visual process explanation:
 * 1. Paste your long URL
 * 2. Get your short link
 * 3. Share and track
 *
 * Uses StepCard component for each step.
 * Left-to-right layout on desktop, stacked on mobile.
 *
 * Related requirements: REQ-5, REQ-10, NFR-3
 */

import { motion } from 'framer-motion'
import { Link2, Zap, Share2 } from 'lucide-react'
import { StepCard } from './StepCard'
import type { HowItWorksSectionProps, Step } from '@/types/homepage'

const steps: Step[] = [
  {
    stepNumber: 1,
    icon: <Link2 className="w-8 h-8" />,
    title: 'Paste Your URL',
    description: 'Enter any long URL you want to shorten into our simple input field.'
  },
  {
    stepNumber: 2,
    icon: <Zap className="w-8 h-8" />,
    title: 'Get Your Short Link',
    description: 'Instantly receive a compact, shareable link that redirects to your original URL.'
  },
  {
    stepNumber: 3,
    icon: <Share2 className="w-8 h-8" />,
    title: 'Share & Track',
    description: 'Share your link anywhere and monitor clicks, geography, and engagement in real-time.'
  }
]

export function HowItWorksSection({ className = '' }: HowItWorksSectionProps) {
  return (
    <section
      className={`py-16 px-4 bg-base-200/50 ${className}`}
      data-testid="how-it-works-section"
      aria-labelledby="how-it-works-heading"
    >
      <div className="max-w-6xl mx-auto">
        <motion.div
          className="text-center mb-12"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2
            id="how-it-works-heading"
            className="text-3xl md:text-4xl font-bold mb-4"
            data-testid="how-it-works-heading"
          >
            How It Works
          </h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Shorten your links in three simple steps
          </p>
        </motion.div>

        <div
          className="grid grid-cols-1 md:grid-cols-3 gap-8"
          data-testid="steps-container"
        >
          {steps.map((step) => (
            <StepCard
              key={step.stepNumber}
              stepNumber={step.stepNumber}
              icon={step.icon}
              title={step.title}
              description={step.description}
            />
          ))}
        </div>
      </div>
    </section>
  )
}
