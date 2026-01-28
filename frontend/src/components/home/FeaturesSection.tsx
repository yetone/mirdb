/**
 * Features Section Component
 * Owner: Scenario 4 - Features Section Display
 *
 * Showcases three key product features:
 * 1. URL Shortening - Create short, memorable links
 * 2. Analytics Dashboard - Track clicks, locations, referrers
 * 3. Link Management - Organize and manage URLs
 *
 * Must use:
 * - GlassMorphismCard for each feature card
 * - Icons for each feature
 * - Responsive grid layout (1 col mobile, 3 col desktop)
 */
import { motion } from 'framer-motion'
import { GlassMorphismCard } from '../GlassMorphismCard'

interface Feature {
  id: string
  icon: React.ReactNode
  title: string
  description: string
}

const features: Feature[] = [
  {
    id: 'url-shortening',
    icon: (
      <svg
        className="w-12 h-12 text-primary"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
        />
      </svg>
    ),
    title: 'URL Shortening',
    description:
      'Transform long, unwieldy URLs into short, memorable links instantly. Share easily across any platform.',
  },
  {
    id: 'analytics',
    icon: (
      <svg
        className="w-12 h-12 text-primary"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
        />
      </svg>
    ),
    title: 'Analytics Dashboard',
    description:
      'Track every click with detailed analytics. Monitor locations, referrers, devices, and more in real-time.',
  },
  {
    id: 'link-management',
    icon: (
      <svg
        className="w-12 h-12 text-primary"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"
        />
      </svg>
    ),
    title: 'Link Management',
    description:
      'Organize and manage all your shortened URLs in one place. Edit, delete, or update links anytime.',
  },
]

export function FeaturesSection() {
  return (
    <section
      data-testid="features-section"
      className="py-16 px-4"
      aria-labelledby="features-heading"
    >
      <div className="max-w-6xl mx-auto">
        <motion.h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          Powerful Features
        </motion.h2>

        <div
          className="grid grid-cols-1 md:grid-cols-3 gap-6 md:gap-8"
          role="list"
          aria-label="Product features"
        >
          {features.map((feature, index) => (
            <GlassMorphismCard
              key={feature.id}
              className="feature-card"
            >
              <motion.article
                className="card-body items-center text-center"
                data-testid={`feature-card-${feature.id}`}
                role="listitem"
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: index * 0.1 }}
              >
                <div className="mb-4" data-testid={`feature-icon-${feature.id}`}>
                  {feature.icon}
                </div>
                <h3 className="card-title text-xl font-semibold mb-2">
                  {feature.title}
                </h3>
                <p className="text-base-content/70">{feature.description}</p>
              </motion.article>
            </GlassMorphismCard>
          ))}
        </div>
      </div>
    </section>
  )
}
