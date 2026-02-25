/**
 * Features Section Component
 * Owner: Scenario 3 - Features Section Display
 *
 * Displays three key product features in a grid layout.
 *
 * Features to display:
 * 1. Lightning URL Shortening - Fast URL shortening icon and description
 * 2. Powerful Analytics - Dashboard visualization icon and description
 * 3. Secure & Reliable - Authentication/security icon and description
 *
 * Layout:
 * - Desktop: 3-column grid
 * - Tablet: 2-column or 3-column grid
 * - Mobile: Single column stack
 */

interface FeatureCardProps {
  icon: React.ReactNode
  title: string
  description: string
}

function FeatureCard({ icon, title, description }: FeatureCardProps) {
  return (
    <div
      className="card bg-base-100 shadow-xl hover:shadow-2xl transition-shadow"
      data-testid="feature-card"
    >
      <div className="card-body items-center text-center">
        <div className="text-primary mb-4" data-testid="feature-icon">
          {icon}
        </div>
        <h3 className="card-title" data-testid="feature-title">{title}</h3>
        <p className="text-base-content/80" data-testid="feature-description">{description}</p>
      </div>
    </div>
  )
}

// Lightning bolt icon for URL Shortening
function LightningIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      className="h-12 w-12"
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={2}
        d="M13 10V3L4 14h7v7l9-11h-7z"
      />
    </svg>
  )
}

// Chart/Analytics icon for Powerful Analytics
function ChartIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      className="h-12 w-12"
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
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
}

// Shield/Security icon for Secure & Reliable
function ShieldIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      className="h-12 w-12"
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={2}
        d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"
      />
    </svg>
  )
}

const features = [
  {
    id: 'lightning-shortening',
    icon: <LightningIcon />,
    title: 'Lightning URL Shortening',
    description: 'Transform long URLs into short, shareable links in milliseconds. Our high-performance engine ensures instant link generation every time.',
  },
  {
    id: 'powerful-analytics',
    icon: <ChartIcon />,
    title: 'Powerful Analytics',
    description: 'Track clicks, geographic data, and referral sources with our comprehensive analytics dashboard. Make data-driven decisions with real-time insights.',
  },
  {
    id: 'secure-reliable',
    icon: <ShieldIcon />,
    title: 'Secure & Reliable',
    description: 'Enterprise-grade security with SSL encryption and 99.9% uptime guarantee. Your links are protected and always available when you need them.',
  },
]

export default function FeaturesSection() {
  return (
    <section
      className="py-16 px-4 bg-base-200"
      aria-labelledby="features-heading"
      data-testid="features-section"
    >
      <div className="container mx-auto max-w-6xl">
        <h2
          id="features-heading"
          className="text-3xl font-bold text-center mb-12"
        >
          Why Choose Our Service?
        </h2>
        <div
          className="grid grid-cols-1 md:grid-cols-3 gap-8"
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
