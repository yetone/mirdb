/**
 * Feature Card Component
 * Owner: Scenario 4 - Feature Highlights Display
 *
 * Displays a single feature highlight using GlassMorphismCard.
 *
 * Requirements:
 * - Uses GlassMorphismCard component
 * - Hover effects on desktop
 * - Responsive layout (single column mobile, grid desktop)
 */
import GlassMorphismCard from './GlassMorphismCard'
import type { Feature, FeatureCardProps } from '../types/home.types'

/**
 * Array of features to display on the homepage
 * Contains 5 key features of the URL Shortening Service
 */
export const FEATURES: Feature[] = [
  {
    icon: '⚡',
    title: 'Instant Shortening',
    description: 'Create short URLs in seconds with our lightning-fast service.',
  },
  {
    icon: '📊',
    title: 'Detailed Analytics',
    description: 'Track clicks, referrers, browsers, and geographic location of visitors.',
  },
  {
    icon: '📈',
    title: 'Track Over Time',
    description: 'Monitor URL performance with interactive charts and historical data.',
  },
  {
    icon: '🔗',
    title: 'Public Sharing',
    description: 'Share analytics with others using secure share tokens.',
  },
  {
    icon: '🎨',
    title: 'Multiple Themes',
    description: 'Personalize your dashboard experience with various theme options.',
  },
]

/**
 * FeatureCard component displays a single feature with icon, title, and description
 */
export function FeatureCard({ icon, title, description }: FeatureCardProps) {
  return (
    <div data-testid="feature-card">
      <GlassMorphismCard className="transition-all duration-300 hover:scale-105 hover:shadow-2xl hover:border-primary">
        <div className="flex flex-col items-center text-center">
          <span className="text-4xl mb-4" role="img" aria-label={title} data-testid="feature-icon">
            {icon}
          </span>
          <h3 className="text-xl font-bold mb-2" data-testid="feature-title">
            {title}
          </h3>
          <p className="text-base-content/70" data-testid="feature-description">
            {description}
          </p>
        </div>
      </GlassMorphismCard>
    </div>
  )
}

/**
 * FeaturesSection component displays a grid of feature cards
 */
export function FeaturesSection() {
  return (
    <section
      className="py-16 px-4"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="max-w-6xl mx-auto">
        <h2 id="features-heading" className="text-3xl font-bold text-center mb-12">
          Why Choose Us?
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {FEATURES.map((feature, index) => (
            <FeatureCard
              key={index}
              icon={feature.icon}
              title={feature.title}
              description={feature.description}
            />
          ))}
        </div>
      </div>
    </section>
  )
}

export default FeatureCard
