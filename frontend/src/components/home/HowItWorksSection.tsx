/**
 * HowItWorksSection - Workflow explanation
 * Owner: Scenario 3 - How It Works Section
 *
 * Displays a 3-step visual workflow explanation.
 *
 * Expected exports:
 * - HowItWorksSection: React.FC
 *
 * Steps:
 * 1. Paste your long URL
 * 2. Get a short, memorable link
 * 3. Track clicks and analytics
 *
 * Layout:
 * - Vertical stack on mobile
 * - Horizontal row on desktop
 * - Number indicators for each step
 * - Icons or illustrations for each step
 */
import { motion } from 'framer-motion'
import { LinkIcon, DocumentDuplicateIcon, ChartBarIcon } from '@heroicons/react/24/outline'
import GlassMorphismCard from '../GlassMorphismCard'

interface WorkflowStep {
  number: number
  title: string
  description: string
  icon: React.ReactNode
}

const workflowSteps: WorkflowStep[] = [
  {
    number: 1,
    title: 'Paste your long URL',
    description: 'Enter any long URL you want to shorten. We accept links from any website.',
    icon: <LinkIcon className="w-8 h-8" />,
  },
  {
    number: 2,
    title: 'Get a short, memorable link',
    description: 'Instantly receive a compact, easy-to-share link that redirects to your original URL.',
    icon: <DocumentDuplicateIcon className="w-8 h-8" />,
  },
  {
    number: 3,
    title: 'Track clicks and analytics',
    description: 'Monitor your link performance with detailed analytics including clicks, locations, and devices.',
    icon: <ChartBarIcon className="w-8 h-8" />,
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

export function HowItWorksSection() {
  return (
    <section
      className="py-16 px-4 lg:px-8 bg-base-200"
      aria-labelledby="how-it-works-heading"
    >
      <div className="max-w-6xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-center mb-12"
        >
          <h2 id="how-it-works-heading" className="text-3xl lg:text-4xl font-bold mb-4">
            How It Works
          </h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Shortening your URLs is quick and easy. Follow these simple steps to get started.
          </p>
        </motion.div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
        >
          {workflowSteps.map((step) => (
            <motion.div key={step.number} variants={itemVariants}>
              <GlassMorphismCard className="h-full text-center">
                <div className="flex flex-col items-center gap-4">
                  {/* Step Number Indicator */}
                  <div
                    className="w-12 h-12 rounded-full bg-primary text-primary-content flex items-center justify-center text-xl font-bold"
                    aria-label={`Step ${step.number}`}
                    data-testid={`step-number-${step.number}`}
                  >
                    {step.number}
                  </div>

                  {/* Icon */}
                  <div className="text-primary">{step.icon}</div>

                  {/* Title */}
                  <h3 className="text-xl font-semibold">{step.title}</h3>

                  {/* Description */}
                  <p className="text-base-content/70">{step.description}</p>
                </div>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}

export default HowItWorksSection
