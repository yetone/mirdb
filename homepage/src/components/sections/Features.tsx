/**
 * Features section with product capability cards.
 * Owner: Scenario 3 - Features Section Implementation
 *
 * Requirements:
 * - Grid layout with 3-5 feature cards (REQ-3)
 * - Each card: icon, bold title (5-10 words), description (15-30 words)
 * - Hover effects for interactivity
 * - Responsive grid (3 col desktop, 2 col tablet, 1 col mobile)
 */

import type { Feature } from '../../types'
import {
  BoltIcon,
  ChartBarIcon,
  CogIcon,
  ShieldCheckIcon,
  UserGroupIcon,
} from '@heroicons/react/24/outline'

export interface FeaturesProps {
  features?: Feature[]
  sectionTitle?: string
  sectionDescription?: string
}

export interface FeatureCardProps {
  feature: Feature
}

const iconMap: Record<string, React.ComponentType<React.SVGProps<SVGSVGElement>>> = {
  bolt: BoltIcon,
  chart: ChartBarIcon,
  cog: CogIcon,
  shield: ShieldCheckIcon,
  users: UserGroupIcon,
}

const defaultFeatures: Feature[] = [
  {
    id: 'automation',
    icon: 'bolt',
    title: 'Intelligent Workflow Automation',
    description:
      'Automate repetitive tasks with AI-powered workflows. Save hours every week and focus on what matters most to your business.',
  },
  {
    id: 'analytics',
    icon: 'chart',
    title: 'Real-Time Analytics Dashboard',
    description:
      'Get instant insights with customizable dashboards. Track performance metrics and make data-driven decisions with confidence.',
  },
  {
    id: 'integrations',
    icon: 'cog',
    title: 'Seamless Third-Party Integrations',
    description:
      'Connect with your favorite tools effortlessly. Over 100+ integrations available to streamline your entire tech stack.',
  },
  {
    id: 'security',
    icon: 'shield',
    title: 'Enterprise-Grade Security',
    description:
      'Your data is protected with bank-level encryption. SOC 2 compliant with regular security audits and monitoring.',
  },
  {
    id: 'collaboration',
    icon: 'users',
    title: 'Team Collaboration Tools',
    description:
      'Work together seamlessly with real-time collaboration features. Share, comment, and iterate faster than ever before.',
  },
]

export function FeatureCard({ feature }: FeatureCardProps) {
  const IconComponent = iconMap[feature.icon] || BoltIcon

  return (
    <article
      className="group relative bg-white rounded-xl p-6 shadow-sm border border-secondary-100
                 transition-all duration-300 ease-in-out
                 hover:shadow-lg hover:scale-[1.02] hover:border-primary-200
                 focus-within:ring-2 focus-within:ring-primary-500 focus-within:ring-offset-2"
      data-testid="feature-card"
      tabIndex={0}
      aria-labelledby={`feature-title-${feature.id}`}
    >
      <div
        className="flex items-center justify-center w-12 h-12 mb-4 rounded-lg
                    bg-primary-50 text-primary-600
                    group-hover:bg-primary-100 transition-colors duration-300"
        data-testid="feature-icon"
        aria-hidden="true"
      >
        <IconComponent className="w-6 h-6" />
      </div>

      <h3
        id={`feature-title-${feature.id}`}
        className="text-lg font-semibold text-secondary-900 mb-2
                   group-hover:text-primary-700 transition-colors duration-300"
        data-testid="feature-title"
      >
        {feature.title}
      </h3>

      <p
        className="text-secondary-600 text-sm leading-relaxed"
        data-testid="feature-description"
      >
        {feature.description}
      </p>
    </article>
  )
}

export function Features({
  features = defaultFeatures,
  sectionTitle = 'Powerful Features for Modern Teams',
  sectionDescription = 'Everything you need to streamline your workflow and boost productivity.',
}: FeaturesProps) {
  return (
    <section
      id="features"
      className="py-16 md:py-24 bg-secondary-50"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="container-main">
        <div className="text-center mb-12 md:mb-16">
          <h2
            id="features-heading"
            className="text-3xl md:text-4xl font-bold text-secondary-900 mb-4"
            data-testid="features-heading"
          >
            {sectionTitle}
          </h2>
          <p
            className="text-lg text-secondary-600 max-w-2xl mx-auto"
            data-testid="features-description"
          >
            {sectionDescription}
          </p>
        </div>

        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 md:gap-8"
          data-testid="features-grid"
          role="list"
        >
          {features.map((feature) => (
            <div key={feature.id} role="listitem">
              <FeatureCard feature={feature} />
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
