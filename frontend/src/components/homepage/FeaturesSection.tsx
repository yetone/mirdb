/**
 * Features Section Component
 * Owner: Scenario 2 - Features Section with Three Core Features
 *
 * Displays three feature cards in a horizontal layout (stacks on mobile):
 * - Smart Shortening / URL shortening
 * - Real-time Analytics
 * - Link Dashboard / Link management
 *
 * Requirements: REQ-2
 */
import { LinkIcon, ChartBarIcon, TableCellsIcon } from '@heroicons/react/24/outline'
import { FeatureCard } from './FeatureCard'

const features = [
  {
    id: 'smart-shortening',
    icon: <LinkIcon className="w-12 h-12" />,
    title: 'Smart Shortening',
    description:
      'Create short, memorable links instantly. Our smart algorithm generates clean URLs that are easy to share and remember.',
  },
  {
    id: 'real-time-analytics',
    icon: <ChartBarIcon className="w-12 h-12" />,
    title: 'Real-time Analytics',
    description:
      'Track every click with detailed analytics. See who clicks your links, when, and from where in real-time.',
  },
  {
    id: 'link-dashboard',
    icon: <TableCellsIcon className="w-12 h-12" />,
    title: 'Link Dashboard',
    description:
      'Manage all your links in one place. Organize, edit, and monitor your entire link portfolio with ease.',
  },
]

export function FeaturesSection() {
  return (
    <section
      className="py-16 px-4 md:px-8 lg:px-16 bg-base-100"
      data-testid="features-section"
      id="features"
    >
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-12">
          <h2 className="text-3xl font-bold mb-4">Powerful Features</h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Everything you need to shorten, track, and manage your links effectively.
          </p>
        </div>

        <div
          className="grid grid-cols-1 md:grid-cols-3 gap-6 lg:gap-8"
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <FeatureCard
              key={feature.id}
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
