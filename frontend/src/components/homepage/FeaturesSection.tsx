/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Displays 3-4 feature cards highlighting:
 * - URL Shortening: "Create short, memorable links in seconds"
 * - Click Analytics: "Track performance with detailed analytics"
 * - Dashboard: "Manage all your URLs in one place"
 * - Share Stats: "Share public analytics with stakeholders"
 */
import { motion } from 'framer-motion'
import GlassMorphismCard from '../GlassMorphismCard'

interface Feature {
  id: string
  title: string
  description: string
  icon: React.ReactNode
}

const LinkIcon = () => (
  <svg
    className="w-8 h-8"
    fill="none"
    stroke="currentColor"
    viewBox="0 0 24 24"
    xmlns="http://www.w3.org/2000/svg"
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth={2}
      d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
    />
  </svg>
)

const ChartIcon = () => (
  <svg
    className="w-8 h-8"
    fill="none"
    stroke="currentColor"
    viewBox="0 0 24 24"
    xmlns="http://www.w3.org/2000/svg"
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth={2}
      d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
    />
  </svg>
)

const DashboardIcon = () => (
  <svg
    className="w-8 h-8"
    fill="none"
    stroke="currentColor"
    viewBox="0 0 24 24"
    xmlns="http://www.w3.org/2000/svg"
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth={2}
      d="M4 5a1 1 0 011-1h14a1 1 0 011 1v2a1 1 0 01-1 1H5a1 1 0 01-1-1V5zM4 13a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H5a1 1 0 01-1-1v-6zM16 13a1 1 0 011-1h2a1 1 0 011 1v6a1 1 0 01-1 1h-2a1 1 0 01-1-1v-6z"
    />
  </svg>
)

const ShareIcon = () => (
  <svg
    className="w-8 h-8"
    fill="none"
    stroke="currentColor"
    viewBox="0 0 24 24"
    xmlns="http://www.w3.org/2000/svg"
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth={2}
      d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.368 2.684 3 3 0 00-5.368-2.684z"
    />
  </svg>
)

const features: Feature[] = [
  {
    id: 'url-shortening',
    title: 'URL Shortening',
    description: 'Create short, memorable links in seconds',
    icon: <LinkIcon />,
  },
  {
    id: 'click-analytics',
    title: 'Click Analytics',
    description: 'Track performance with detailed analytics',
    icon: <ChartIcon />,
  },
  {
    id: 'dashboard',
    title: 'Dashboard',
    description: 'Manage all your URLs in one place',
    icon: <DashboardIcon />,
  },
  {
    id: 'share-stats',
    title: 'Share Stats',
    description: 'Share public analytics with stakeholders',
    icon: <ShareIcon />,
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

export default function FeaturesSection() {
  return (
    <section
      className="py-20 px-4"
      data-testid="features-section"
      aria-labelledby="features-heading"
    >
      <div className="container mx-auto max-w-6xl">
        <motion.div
          className="text-center mb-12"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2
            id="features-heading"
            className="text-3xl md:text-4xl font-bold mb-4"
          >
            Powerful Features
          </h2>
          <p className="text-base-content/70 text-lg max-w-2xl mx-auto">
            Everything you need to shorten, track, and manage your URLs effectively
          </p>
        </motion.div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
        >
          {features.map((feature) => (
            <motion.div key={feature.id} variants={itemVariants}>
              <GlassMorphismCard className="p-6 h-full">
                <div
                  className="flex flex-col items-center text-center"
                  data-testid={`feature-card-${feature.id}`}
                >
                  <div
                    className="text-primary mb-4"
                    data-testid={`feature-icon-${feature.id}`}
                    aria-hidden="true"
                  >
                    {feature.icon}
                  </div>
                  <h3 className="text-xl font-semibold mb-2">{feature.title}</h3>
                  <p className="text-base-content/70">{feature.description}</p>
                </div>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}
