/**
 * How It Works Section Component
 * Owner: Scenario 3 - How It Works Section
 *
 * Purpose: Visual 3-step guide explaining the service process.
 *
 * Expected steps:
 * 1. Create: Paste your long URL and get a short link
 * 2. Share: Share your short link anywhere
 * 3. Track: Monitor performance with real-time analytics
 *
 * Expected features:
 * - Step numbers with visual indicators
 * - Icons/illustrations for each step
 * - Clear titles and descriptions
 * - Responsive layout (horizontal on desktop, vertical on mobile)
 */

import { motion } from 'framer-motion'
import { Link2, Share2, BarChart3 } from 'lucide-react'
import type { Step } from '../../types/home'
import GlassMorphismCard from '../GlassMorphismCard'

const steps: Step[] = [
  {
    number: 1,
    title: 'Create',
    description: 'Paste your long URL and get a short link instantly',
    icon: <Link2 className="w-8 h-8" aria-hidden="true" />,
  },
  {
    number: 2,
    title: 'Share',
    description: 'Share your short link anywhere - social media, emails, or messages',
    icon: <Share2 className="w-8 h-8" aria-hidden="true" />,
  },
  {
    number: 3,
    title: 'Track',
    description: 'Monitor clicks and performance with real-time analytics',
    icon: <BarChart3 className="w-8 h-8" aria-hidden="true" />,
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
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
    },
  },
}

interface StepCardProps {
  step: Step
  index: number
}

const StepCard = ({ step, index }: StepCardProps) => {
  return (
    <motion.div variants={itemVariants} className="flex-1">
      <GlassMorphismCard
        className="h-full p-6"
        data-testid={`step-card-${step.number}`}
        whileHover={{ scale: 1.02 }}
        transition={{ type: 'spring', stiffness: 300 }}
      >
        <div className="flex flex-col items-center text-center space-y-4">
          {/* Step Number Indicator */}
          <div
            className="flex items-center justify-center w-12 h-12 rounded-full bg-primary text-primary-content font-bold text-xl"
            data-testid={`step-number-${step.number}`}
            aria-label={`Step ${step.number}`}
          >
            {step.number}
          </div>

          {/* Step Icon */}
          <div
            className="text-primary"
            data-testid={`step-icon-${step.number}`}
            role="img"
            aria-label={`${step.title} icon`}
          >
            {step.icon}
          </div>

          {/* Step Title */}
          <h3
            className="text-xl font-semibold text-base-content"
            data-testid={`step-title-${step.number}`}
          >
            {step.title}
          </h3>

          {/* Step Description */}
          <p
            className="text-base-content/70"
            data-testid={`step-description-${step.number}`}
          >
            {step.description}
          </p>
        </div>
      </GlassMorphismCard>

      {/* Connector Line (hidden on mobile and for the last step) */}
      {index < steps.length - 1 && (
        <div
          className="hidden md:block absolute top-1/2 right-0 w-8 h-0.5 bg-primary/30 transform translate-x-full"
          aria-hidden="true"
        />
      )}
    </motion.div>
  )
}

const HowItWorksSection = () => {
  return (
    <section
      id="how-it-works"
      className="py-16 px-4 sm:px-6 lg:px-8"
      aria-labelledby="how-it-works-title"
      data-testid="how-it-works-section"
    >
      <div className="max-w-6xl mx-auto">
        {/* Section Header */}
        <div className="text-center mb-12">
          <motion.h2
            id="how-it-works-title"
            className="text-3xl sm:text-4xl font-bold text-base-content mb-4"
            initial={{ opacity: 0, y: -20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5 }}
          >
            How It Works
          </motion.h2>
          <motion.p
            className="text-lg text-base-content/70 max-w-2xl mx-auto"
            initial={{ opacity: 0 }}
            whileInView={{ opacity: 1 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.2 }}
          >
            Get started in three simple steps
          </motion.p>
        </div>

        {/* Steps Container - Horizontal on desktop, vertical on mobile */}
        <motion.div
          className="flex flex-col md:flex-row gap-6 md:gap-8 relative"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          data-testid="steps-container"
        >
          {steps.map((step, index) => (
            <StepCard key={step.number} step={step} index={index} />
          ))}
        </motion.div>
      </div>
    </section>
  )
}

export default HowItWorksSection
