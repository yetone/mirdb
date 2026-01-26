/**
 * Features Section Component
 * Owner: Scenario 4 - Features Section Display
 *
 * This component renders the three key features in a grid layout.
 * Each feature card uses the GlassMorphismCard component.
 */

import { motion } from 'framer-motion'
import { GlassMorphismCard } from '../GlassMorphismCard'
import { Feature } from '@/types/homepage'

const features: Feature[] = [
  {
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-8 h-8"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M13.19 8.688a4.5 4.5 0 0 1 1.242 7.244l-4.5 4.5a4.5 4.5 0 0 1-6.364-6.364l1.757-1.757m13.35-.622 1.757-1.757a4.5 4.5 0 0 0-6.364-6.364l-4.5 4.5a4.5 4.5 0 0 0 1.242 7.244"
        />
      </svg>
    ),
    title: 'Fast URL Shortening',
    description:
      'Create memorable short links instantly. Transform long, unwieldy URLs into clean, shareable links with just one click.',
  },
  {
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-8 h-8"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 0 1 3 19.875v-6.75ZM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V8.625ZM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 0 1-1.125-1.125V4.125Z"
        />
      </svg>
    ),
    title: 'Detailed Analytics',
    description:
      'Track every click with comprehensive analytics. Monitor visits, geographic locations, devices, and referral sources in real-time.',
  },
  {
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-8 h-8"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M10.5 6h9.75M10.5 6a1.5 1.5 0 1 1-3 0m3 0a1.5 1.5 0 1 0-3 0M3.75 6H7.5m3 12h9.75m-9.75 0a1.5 1.5 0 0 1-3 0m3 0a1.5 1.5 0 0 0-3 0m-3.75 0H7.5m9-6h3.75m-3.75 0a1.5 1.5 0 0 1-3 0m3 0a1.5 1.5 0 0 0-3 0m-9.75 0h9.75"
        />
      </svg>
    ),
    title: 'Dashboard Management',
    description:
      'Manage all your links in one powerful dashboard. Organize, edit, and monitor your entire link portfolio with ease.',
  },
]

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.15,
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

export function FeaturesSection() {
  return (
    <section
      id="features"
      data-testid="features-section"
      className="py-16 md:py-24 px-4"
      aria-labelledby="features-heading"
    >
      <div className="container mx-auto max-w-6xl">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-center mb-12"
        >
          <h2
            id="features-heading"
            data-testid="features-heading"
            className="text-3xl md:text-4xl font-bold mb-4"
          >
            Everything You Need
          </h2>
          <p className="text-base-content/70 text-lg max-w-2xl mx-auto">
            Powerful features to help you shorten, share, and track your links effectively.
          </p>
        </motion.div>

        <motion.div
          data-testid="features-grid"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
        >
          {features.map((feature, index) => (
            <motion.div key={index} variants={itemVariants}>
              <GlassMorphismCard className="h-full">
                <div
                  data-testid={`feature-card-${index}`}
                  className="card-body"
                >
                  <div
                    data-testid={`feature-icon-${index}`}
                    className="w-14 h-14 rounded-lg bg-primary/10 flex items-center justify-center text-primary mb-4"
                  >
                    {feature.icon}
                  </div>
                  <h3
                    data-testid={`feature-title-${index}`}
                    className="card-title text-xl"
                  >
                    {feature.title}
                  </h3>
                  <p
                    data-testid={`feature-description-${index}`}
                    className="text-base-content/70"
                  >
                    {feature.description}
                  </p>
                </div>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}
