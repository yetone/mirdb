import { HiLink, HiChartBar, HiCollection, HiShieldCheck } from 'react-icons/hi'
import GlassMorphismCard from './GlassMorphismCard'

interface Feature {
  id: string
  icon: React.ReactNode
  title: string
  description: string
}

const features: Feature[] = [
  {
    id: 'url-shortening',
    icon: <HiLink className="w-10 h-10 text-primary" aria-hidden="true" />,
    title: 'Quick URL Shortening',
    description: 'Create short, memorable links in seconds. Just paste your long URL and get a clean, shareable link instantly.',
  },
  {
    id: 'analytics',
    icon: <HiChartBar className="w-10 h-10 text-secondary" aria-hidden="true" />,
    title: 'Detailed Analytics',
    description: 'Track every click with comprehensive analytics. Monitor referrers, browsers, locations, and more in real-time.',
  },
  {
    id: 'link-management',
    icon: <HiCollection className="w-10 h-10 text-accent" aria-hidden="true" />,
    title: 'Easy Link Management',
    description: 'Organize and manage all your shortened URLs from a single dashboard. Edit, delete, or share with ease.',
  },
  {
    id: 'security',
    icon: <HiShieldCheck className="w-10 h-10 text-success" aria-hidden="true" />,
    title: 'Secure & Reliable',
    description: 'Your links are safe with enterprise-grade security. Enjoy 99.9% uptime and encrypted data protection.',
  },
]

export default function FeaturesSection() {
  return (
    <section
      id="features"
      className="py-20 px-4 sm:px-6 lg:px-8"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-16">
          <h2 id="features-heading" className="text-3xl sm:text-4xl font-bold mb-4">
            Powerful Features
          </h2>
          <p className="text-base-content/70 text-lg max-w-2xl mx-auto">
            Everything you need to shorten, share, and track your links effectively.
          </p>
        </div>

        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <GlassMorphismCard key={feature.id}>
              <div className="flex flex-col items-center text-center" data-testid={`feature-card-${feature.id}`}>
                <div className="mb-4" data-testid={`feature-icon-${feature.id}`}>
                  {feature.icon}
                </div>
                <h3 className="text-xl font-semibold mb-2" data-testid={`feature-title-${feature.id}`}>
                  {feature.title}
                </h3>
                <p className="text-base-content/70" data-testid={`feature-description-${feature.id}`}>
                  {feature.description}
                </p>
              </div>
            </GlassMorphismCard>
          ))}
        </div>
      </div>
    </section>
  )
}

export { features }
