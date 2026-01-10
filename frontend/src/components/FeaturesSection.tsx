import { motion } from 'framer-motion'
import { useContext } from 'react'
import GlassMorphismCard from './GlassMorphismCard'
import { ReducedMotionContext } from './HeroSection'

interface Feature {
  icon: React.ReactNode
  title: string
  description: string
}

// Hook to check reduced motion preference
function useIsReducedMotion(): boolean {
  const contextValue = useContext(ReducedMotionContext)
  if (contextValue !== null) {
    return contextValue
  }
  if (typeof window === 'undefined') return false
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches
}

const features: Feature[] = [
  {
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-12 h-12"
        data-testid="url-shortening-icon"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244"
        />
      </svg>
    ),
    title: 'URL Shortening',
    description: 'Create memorable, short links instantly. Transform long URLs into clean, shareable links in seconds.',
  },
  {
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-12 h-12"
        data-testid="analytics-icon"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z"
        />
      </svg>
    ),
    title: 'Analytics Dashboard',
    description: 'Track clicks, locations, and referrers. Get detailed insights into your link performance.',
  },
  {
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
        strokeWidth={1.5}
        stroke="currentColor"
        className="w-12 h-12"
        data-testid="link-management-icon"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M3.75 12h16.5m-16.5 3.75h16.5M3.75 19.5h16.5M5.625 4.5h12.75a1.875 1.875 0 010 3.75H5.625a1.875 1.875 0 010-3.75z"
        />
      </svg>
    ),
    title: 'Link Management',
    description: 'Organize and manage all your links in one place. Edit, delete, and categorize your URLs effortlessly.',
  },
]

function FeaturesSection() {
  const prefersReducedMotion = useIsReducedMotion()

  // Animation variants for section header
  const headerVariants = prefersReducedMotion
    ? {}
    : {
        initial: { opacity: 0, y: 30 },
        whileInView: { opacity: 1, y: 0 },
        transition: { duration: 0.6, ease: 'easeOut' },
        viewport: { once: true, amount: 0.3 },
      }

  // Animation variants for feature cards with stagger effect
  const cardVariants = (index: number) =>
    prefersReducedMotion
      ? {}
      : {
          initial: { opacity: 0, y: 40 },
          whileInView: { opacity: 1, y: 0 },
          transition: { duration: 0.5, delay: index * 0.15, ease: 'easeOut' },
          viewport: { once: true, amount: 0.2 },
        }

  return (
    <section className="py-16 px-4 bg-base-200" data-testid="features-section">
      <div className="container mx-auto max-w-6xl">
        <motion.h2
          className="text-3xl font-bold text-center mb-12"
          data-testid="features-heading"
          {...headerVariants}
        >
          Powerful Features
        </motion.h2>
        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8"
          data-testid="features-grid"
        >
          {features.map((feature, index) => (
            <motion.div key={index} {...cardVariants(index)}>
              <GlassMorphismCard className="p-6">
                <div
                  className="flex flex-col items-center text-center"
                  data-testid={`feature-card-${index}`}
                >
                  <div className="text-primary mb-4">{feature.icon}</div>
                  <h3 className="text-xl font-semibold mb-3">{feature.title}</h3>
                  <p className="text-base-content/70">{feature.description}</p>
                </div>
              </GlassMorphismCard>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  )
}

export default FeaturesSection
