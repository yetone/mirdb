/**
 * Features Section Component
 * Owner: Scenario 3 - Features Section Display
 *
 * Displays feature cards in a grid layout:
 * - URL Shortening feature
 * - Click Analytics feature
 * - GeoIP Tracking feature
 * - Share Tokens feature
 *
 * Each card includes: icon, title, description
 *
 * Requirements: REQ-3, US-4
 */

import { motion } from 'framer-motion'

export interface Feature {
  id: string
  icon: React.ReactNode
  title: string
  description: string
}

export interface FeaturesSectionProps {
  features?: Feature[]
}

const defaultFeatures: Feature[] = [
  {
    id: 'url-shortening',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-12 w-12"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        strokeWidth={2}
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
        />
      </svg>
    ),
    title: 'URL Shortening',
    description:
      'Create short, memorable links in seconds. Transform long URLs into clean, shareable links that are easy to remember and track.',
  },
  {
    id: 'click-analytics',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-12 w-12"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        strokeWidth={2}
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
        />
      </svg>
    ),
    title: 'Click Analytics',
    description:
      'Track every click with detailed analytics. Monitor referrers, browsers, operating systems, and engagement patterns in real-time.',
  },
  {
    id: 'geoip-tracking',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-12 w-12"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        strokeWidth={2}
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
        />
      </svg>
    ),
    title: 'GeoIP Location Tracking',
    description:
      'Know exactly where your clicks come from. Geographic insights help you understand your global audience and optimize your reach.',
  },
  {
    id: 'share-tokens',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-12 w-12"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        strokeWidth={2}
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.368 2.684 3 3 0 00-5.368-2.684z"
        />
      </svg>
    ),
    title: 'Share Tokens',
    description:
      'Share your link statistics publicly without exposing your account. Generate secure tokens that let others view performance data.',
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

const cardVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
    },
  },
}

export function FeaturesSection({ features = defaultFeatures }: FeaturesSectionProps) {
  return (
    <section
      id="features"
      className="py-16 px-4"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="max-w-6xl mx-auto">
        <motion.div
          className="text-center mb-12"
          initial={{ opacity: 0, y: -20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2
            id="features-heading"
            className="text-3xl md:text-4xl font-bold mb-4 text-base-content"
            data-testid="features-heading"
          >
            Powerful Features
          </h2>
          <p className="text-lg text-base-content/70 max-w-2xl mx-auto">
            Everything you need to shorten, track, and analyze your links in one place.
          </p>
        </motion.div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <motion.div
              key={feature.id}
              className="card bg-base-200/50 backdrop-blur-sm border border-base-300 hover:border-primary/50 transition-colors"
              variants={cardVariants}
              whileHover={{ scale: 1.02 }}
              data-testid="feature-card"
            >
              <div className="card-body items-center text-center">
                <div
                  className="text-primary mb-4"
                  data-testid="feature-icon"
                  aria-hidden="true"
                >
                  {feature.icon}
                </div>
                <h3
                  className="card-title text-lg font-semibold text-base-content"
                  data-testid="feature-title"
                >
                  {feature.title}
                </h3>
                <p
                  className="text-base-content/70 text-sm"
                  data-testid="feature-description"
                >
                  {feature.description}
                </p>
              </div>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}

export default FeaturesSection
