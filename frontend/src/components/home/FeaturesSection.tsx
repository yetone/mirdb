/**
 * Features Section Component
 * Owner: Scenario 3 - Features Section Display
 *
 * Displays 3-5 key features of the URL shortening service
 * using GlassMorphismCard components with staggered entrance animations.
 */

import { motion } from 'framer-motion'
import { Link, BarChart3, Shield, LayoutDashboard } from 'lucide-react'
import { GlassMorphismCard } from '../GlassMorphismCard'
import { Feature } from '../../types/home'
import { featureCardVariants } from '../../utils/animations'

const features: Feature[] = [
  {
    id: 1,
    title: 'Instant URL Shortening',
    description: 'Generate unique, memorable short codes in seconds with our lightning-fast shortening engine.',
    icon: Link,
  },
  {
    id: 2,
    title: 'Detailed Analytics',
    description: 'Track clicks by location, device, browser, and referrer. Know your audience better.',
    icon: BarChart3,
  },
  {
    id: 3,
    title: 'Secure & Private',
    description: 'Your data is protected with enterprise-grade authentication. Share stats via secure tokens.',
    icon: Shield,
  },
  {
    id: 4,
    title: 'Modern Dashboard',
    description: 'Manage all your URLs from a beautiful, responsive dashboard with dark mode themes.',
    icon: LayoutDashboard,
  },
]

export interface FeaturesSectionProps {
  className?: string
}

export function FeaturesSection({ className = '' }: FeaturesSectionProps) {
  return (
    <section
      id="features"
      data-testid="features-section"
      className={`py-16 px-4 md:px-8 ${className}`}
      aria-labelledby="features-heading"
    >
      <div className="max-w-6xl mx-auto">
        <motion.h2
          id="features-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
          initial={{ opacity: 0, y: -20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          Why Choose Us
        </motion.h2>

        <motion.div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-100px' }}
        >
          {features.map((feature, index) => (
            <GlassMorphismCard
              key={feature.id}
              data-testid={`feature-card-${feature.id}`}
              custom={index}
              variants={featureCardVariants}
              whileHover={{ scale: 1.03, transition: { duration: 0.2 } }}
              className="flex flex-col items-center text-center"
            >
              <div className="mb-4 p-3 rounded-full bg-primary/10">
                <feature.icon
                  className="w-8 h-8 text-primary"
                  data-testid={`feature-icon-${feature.id}`}
                  aria-hidden="true"
                />
              </div>
              <h3 className="text-lg font-semibold mb-2" data-testid={`feature-title-${feature.id}`}>
                {feature.title}
              </h3>
              <p className="text-base-content/70" data-testid={`feature-description-${feature.id}`}>
                {feature.description}
              </p>
            </GlassMorphismCard>
          ))}
        </motion.div>
      </div>
    </section>
  )
}

export default FeaturesSection
