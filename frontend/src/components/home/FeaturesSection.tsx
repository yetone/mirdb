/**
 * FeaturesSection - Feature highlights grid
 * Owner: Scenario 2 - Features Section Display
 *
 * Displays key product features with icons and descriptions.
 *
 * Expected exports:
 * - FeaturesSection: React.FC
 *
 * Features to highlight (4-6 cards):
 * - URL Shortening with unique short codes
 * - Click Analytics (referrers, browsers, OS)
 * - GeoIP Location Tracking
 * - Share Tokens for public stats
 * - User Dashboard
 * - Dark Mode Support
 *
 * Components used:
 * - GlassMorphismCard
 * - Responsive grid layout (1 col mobile, 2 col tablet, 3-4 col desktop)
 */
import { motion } from 'framer-motion'
import {
  LinkIcon,
  ChartBarIcon,
  GlobeAltIcon,
  ShareIcon,
  Squares2X2Icon,
  MoonIcon,
} from '@heroicons/react/24/outline'
import GlassMorphismCard from '../GlassMorphismCard'

interface Feature {
  title: string
  description: string
  icon: React.ReactNode
}

const features: Feature[] = [
  {
    title: 'URL Shortening',
    description: 'Create unique, short codes for any URL. Easy to share and remember.',
    icon: <LinkIcon className="w-8 h-8" />,
  },
  {
    title: 'Click Analytics',
    description: 'Track referrers, browsers, and operating systems for every click.',
    icon: <ChartBarIcon className="w-8 h-8" />,
  },
  {
    title: 'GeoIP Tracking',
    description: 'See where your visitors are coming from with location analytics.',
    icon: <GlobeAltIcon className="w-8 h-8" />,
  },
  {
    title: 'Share Tokens',
    description: 'Generate public share tokens to let others view your link stats.',
    icon: <ShareIcon className="w-8 h-8" />,
  },
  {
    title: 'User Dashboard',
    description: 'Manage all your shortened URLs from a centralized dashboard.',
    icon: <Squares2X2Icon className="w-8 h-8" />,
  },
  {
    title: 'Dark Mode',
    description: 'Multiple theme options including dark mode for comfortable viewing.',
    icon: <MoonIcon className="w-8 h-8" />,
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

export function FeaturesSection() {
  return (
    <section
      className="py-16 px-4 lg:px-8"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="max-w-6xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-center mb-12"
        >
          <h2 id="features-heading" className="text-3xl lg:text-4xl font-bold mb-4">
            Powerful Features
          </h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Everything you need to shorten, share, and track your links effectively.
          </p>
        </motion.div>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          data-testid="features-grid"
        >
          {features.map((feature, index) => (
            <motion.div key={index} variants={itemVariants}>
              <GlassMorphismCard className="h-full" data-testid={`feature-card-${index}`}>
                <div className="flex flex-col items-center text-center gap-4">
                  <div className="text-primary">{feature.icon}</div>
                  <h3 className="text-xl font-semibold">{feature.title}</h3>
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

export default FeaturesSection
