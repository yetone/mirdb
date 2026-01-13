import { Link, BarChart3, Settings, Palette } from 'lucide-react'
import { GlassMorphismCard } from './GlassMorphismCard'
import { motion } from 'framer-motion'

interface Feature {
  id: string
  title: string
  description: string
  icon: React.ReactNode
}

const features: Feature[] = [
  {
    id: 'url-shortening',
    title: 'URL Shortening',
    description: 'Create short, memorable links in seconds. Generate unique, shareable codes for any URL.',
    icon: <Link className="w-8 h-8" aria-hidden="true" />,
  },
  {
    id: 'analytics',
    title: 'Analytics Dashboard',
    description: 'Track clicks, locations, referrers, and more. Get detailed insights into your link performance.',
    icon: <BarChart3 className="w-8 h-8" aria-hidden="true" />,
  },
  {
    id: 'link-management',
    title: 'Link Management',
    description: 'Organize, edit, and delete your links. Share them easily with built-in sharing features.',
    icon: <Settings className="w-8 h-8" aria-hidden="true" />,
  },
  {
    id: 'themes',
    title: 'Multiple Themes',
    description: 'Customize your experience with dark mode and various theme options to suit your style.',
    icon: <Palette className="w-8 h-8" aria-hidden="true" />,
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

export function FeaturesSection() {
  return (
    <section
      id="features"
      data-testid="features-section"
      className="py-20 px-4 sm:px-6 lg:px-8"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-16">
          <h2 className="text-3xl sm:text-4xl font-bold text-base-content mb-4">
            Powerful Features
          </h2>
          <p className="text-lg text-base-content max-w-2xl mx-auto">
            Everything you need to manage and track your links effectively
          </p>
        </div>

        <motion.div
          data-testid="feature-cards-container"
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-100px' }}
        >
          {features.map((feature) => (
            <motion.div key={feature.id} variants={cardVariants}>
              <GlassMorphismCard
                data-testid={`feature-card-${feature.id}`}
                className="h-full flex flex-col"
              >
                <div
                  data-testid={`feature-icon-${feature.id}`}
                  className="w-14 h-14 rounded-xl bg-primary/20 flex items-center justify-center text-primary mb-4"
                >
                  {feature.icon}
                </div>
                <h3
                  data-testid={`feature-title-${feature.id}`}
                  className="text-xl font-semibold text-base-content mb-2"
                >
                  {feature.title}
                </h3>
                <p
                  data-testid={`feature-description-${feature.id}`}
                  className="text-base-content flex-grow"
                >
                  {feature.description}
                </p>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </motion.div>
      </div>
    </section>
  )
}

export default FeaturesSection
