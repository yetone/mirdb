/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section Display
 *
 * Displays product features in a responsive grid:
 * - URL Shortening: Create short, memorable links
 * - Click Analytics: Track performance with detailed insights
 * - Dashboard Management: Organize and manage all links
 * - Share Stats: Public share tokens for transparency
 *
 * Each feature card includes:
 * - Icon or illustration (from Heroicons or similar)
 * - Title
 * - Description (2-3 sentences)
 *
 * Uses:
 * - GlassMorphismCard for feature cards
 * - Framer Motion for scroll-triggered animations
 * - Tailwind CSS grid for responsive layout
 *
 * Props:
 * - features?: FeatureItem[] - Optional custom features array
 *
 * Types:
 * - FeatureItem: { icon: ReactNode, title: string, description: string }
 */

import { ReactNode } from 'react'
import { motion } from 'framer-motion'
import {
  LinkIcon,
  ChartBarIcon,
  Squares2X2Icon,
  ShareIcon,
} from '@heroicons/react/24/outline'
import GlassMorphismCard from '../GlassMorphismCard'

export interface FeatureItem {
  icon: ReactNode
  title: string
  description: string
}

interface FeaturesSectionProps {
  features?: FeatureItem[]
}

const defaultFeatures: FeatureItem[] = [
  {
    icon: <LinkIcon className="h-10 w-10 text-primary" aria-hidden="true" />,
    title: 'URL Shortening',
    description:
      'Create short, memorable links from long URLs. Transform lengthy web addresses into clean, shareable links that are easy to remember and type.',
  },
  {
    icon: <ChartBarIcon className="h-10 w-10 text-primary" aria-hidden="true" />,
    title: 'Click Analytics',
    description:
      'Track performance with detailed insights. Monitor click counts, geographic data, referral sources, and browser statistics to understand your audience.',
  },
  {
    icon: <Squares2X2Icon className="h-10 w-10 text-primary" aria-hidden="true" />,
    title: 'Dashboard Management',
    description:
      'Organize and manage all your links in one place. View, edit, and delete your shortened URLs through an intuitive dashboard interface.',
  },
  {
    icon: <ShareIcon className="h-10 w-10 text-primary" aria-hidden="true" />,
    title: 'Share Stats',
    description:
      'Generate public share tokens for transparency. Allow others to view your link analytics without giving them access to your account.',
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

export default function FeaturesSection({ features = defaultFeatures }: FeaturesSectionProps) {
  return (
    <section className="py-16 px-4 sm:px-6 lg:px-8" data-testid="features-section">
      <div className="max-w-7xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-center mb-12"
        >
          <h2 className="text-3xl font-bold mb-4">Features</h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Everything you need to shorten, track, and manage your links effectively.
          </p>
        </motion.div>

        <motion.div
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
          data-testid="features-grid"
        >
          {features.map((feature, index) => (
            <GlassMorphismCard key={index} className="h-full">
              <div className="card-body" data-testid="feature-card">
                <div className="mb-4" data-testid="feature-icon">
                  {feature.icon}
                </div>
                <motion.h3
                  variants={itemVariants}
                  className="card-title text-lg"
                  data-testid="feature-title"
                >
                  {feature.title}
                </motion.h3>
                <motion.p
                  variants={itemVariants}
                  className="text-base-content/70 text-sm"
                  data-testid="feature-description"
                >
                  {feature.description}
                </motion.p>
              </div>
            </GlassMorphismCard>
          ))}
        </motion.div>
      </div>
    </section>
  )
}
