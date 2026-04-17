/**
 * Step Card Component
 * Owner: Scenario 4 - How It Works Section
 *
 * Individual step display with:
 * - Step number indicator
 * - Icon representing the action
 * - Brief description text
 *
 * Related requirements: REQ-5, REQ-10, NFR-3
 */

import { motion } from 'framer-motion'
import type { StepCardProps } from '@/types/homepage'

export function StepCard({ stepNumber, icon, title, description }: StepCardProps) {
  return (
    <motion.div
      className="flex flex-col items-center text-center p-6"
      data-testid="step-card"
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.5, delay: stepNumber * 0.1 }}
    >
      <div
        className="w-12 h-12 rounded-full bg-primary text-primary-content flex items-center justify-center text-xl font-bold mb-4"
        data-testid="step-number"
        aria-label={`Step ${stepNumber}`}
      >
        {stepNumber}
      </div>
      <div
        className="w-16 h-16 flex items-center justify-center rounded-full bg-base-200 text-primary mb-4"
        data-testid="step-icon"
        aria-hidden="true"
      >
        {icon}
      </div>
      <h3
        className="text-lg font-semibold mb-2"
        data-testid="step-title"
      >
        {title}
      </h3>
      <p
        className="text-base-content/70 max-w-xs"
        data-testid="step-description"
      >
        {description}
      </p>
    </motion.div>
  )
}
