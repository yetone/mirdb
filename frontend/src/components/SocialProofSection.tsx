import React from 'react'
import { motion } from 'framer-motion'
import GlassMorphismCard from './GlassMorphismCard'

interface Statistic {
  id: number
  value: string
  label: string
  testId: string
}

interface TrustIndicator {
  id: number
  icon: React.ReactNode
  title: string
  description: string
  testId: string
}

const statistics: Statistic[] = [
  {
    id: 1,
    value: '10M+',
    label: 'URLs Shortened',
    testId: 'stat-urls-shortened',
  },
  {
    id: 2,
    value: '50M+',
    label: 'Clicks Tracked',
    testId: 'stat-clicks-tracked',
  },
]

const trustIndicators: TrustIndicator[] = [
  {
    id: 1,
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-10 w-10"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"
        />
      </svg>
    ),
    title: 'Secure & Encrypted',
    description: 'Enterprise-grade security with SSL encryption',
    testId: 'trust-indicator-security',
  },
  {
    id: 2,
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-10 w-10"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"
        />
      </svg>
    ),
    title: '99.9% Uptime',
    description: 'Reliable infrastructure you can count on',
    testId: 'trust-indicator-uptime',
  },
]

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.1,
    },
  },
}

interface SocialProofSectionProps {
  'data-testid'?: string
}

const SocialProofSection: React.FC<SocialProofSectionProps> = ({ 'data-testid': testId }) => {
  return (
    <section
      id="social-proof"
      className="py-20 px-4 sm:px-6 lg:px-8 bg-base-100"
      aria-labelledby="social-proof-heading"
      data-testid={testId || 'social-proof-section'}
    >
      <div className="max-w-7xl mx-auto">
        <motion.div
          className="text-center mb-16"
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          <h2
            id="social-proof-heading"
            className="text-3xl sm:text-4xl font-bold text-base-content mb-4"
          >
            Trusted by Thousands
          </h2>
          <p className="text-lg text-base-content/70 max-w-2xl mx-auto">
            Join our growing community of users who rely on our platform every day.
          </p>
        </motion.div>

        <motion.div
          className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6"
          variants={containerVariants}
          initial="hidden"
          animate="visible"
          data-testid="social-proof-grid"
        >
          {statistics.map((stat) => (
            <GlassMorphismCard key={stat.id} className="text-center">
              <div
                className="text-4xl font-bold text-primary mb-2"
                data-testid={`stat-value-${stat.id}`}
              >
                {stat.value}
              </div>
              <div
                className="text-base-content/70 text-sm"
                data-testid={`stat-label-${stat.id}`}
              >
                {stat.label}
              </div>
              <div data-testid={stat.testId} className="sr-only">
                {stat.value} {stat.label}
              </div>
            </GlassMorphismCard>
          ))}

          {trustIndicators.map((indicator) => (
            <GlassMorphismCard key={`trust-${indicator.id}`} className="text-center">
              <div
                className="text-primary mb-4 flex justify-center"
                aria-hidden="true"
                data-testid={`trust-icon-${indicator.id}`}
              >
                {indicator.icon}
              </div>
              <h3 className="text-xl font-semibold text-base-content mb-2">
                {indicator.title}
              </h3>
              <p className="text-base-content/70 text-sm">
                {indicator.description}
              </p>
              <div data-testid={indicator.testId} className="sr-only">
                {indicator.title}: {indicator.description}
              </div>
            </GlassMorphismCard>
          ))}
        </motion.div>
      </div>
    </section>
  )
}

export default SocialProofSection
