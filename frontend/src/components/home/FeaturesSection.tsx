/**
 * Features display section for homepage.
 * Owner: Scenario 2 - Features Section Display
 *
 * Displays 3-5 key service features in card format:
 * - URL Shortening capability
 * - Click analytics
 * - Secure URL management
 * - (optional) Custom short codes
 * - (optional) Share statistics
 *
 * Uses GlassMorphismCard for visual consistency.
 */

import React from 'react'
import { GlassMorphismCard } from '../common'

/** Feature data interface */
export interface Feature {
  id: string
  icon: React.ReactNode
  title: string
  description: string
}

/** Icon component for Link/URL shortening */
function LinkIcon() {
  return (
    <svg
      data-testid="feature-icon"
      className="w-12 h-12 text-primary"
      fill="none"
      stroke="currentColor"
      viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={2}
        d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
      />
    </svg>
  )
}

/** Icon component for Analytics */
function ChartIcon() {
  return (
    <svg
      data-testid="feature-icon"
      className="w-12 h-12 text-primary"
      fill="none"
      stroke="currentColor"
      viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg"
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

/** Icon component for Security/Shield */
function ShieldIcon() {
  return (
    <svg
      data-testid="feature-icon"
      className="w-12 h-12 text-primary"
      fill="none"
      stroke="currentColor"
      viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg"
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

/** Icon component for Custom codes */
function CodeIcon() {
  return (
    <svg
      data-testid="feature-icon"
      className="w-12 h-12 text-primary"
      fill="none"
      stroke="currentColor"
      viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={2}
        d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4"
      />
    </svg>
  )
}

/** The features data */
const features: Feature[] = [
  {
    id: 'url-shortening',
    icon: <LinkIcon />,
    title: 'URL Shortening',
    description:
      'Transform long, unwieldy URLs into short, memorable links instantly. Share cleaner links that are easier to remember and type.',
  },
  {
    id: 'analytics',
    icon: <ChartIcon />,
    title: 'Click Analytics',
    description:
      'Track every click with detailed analytics. Get insights on referrers, browsers, devices, and geographic location of your visitors.',
  },
  {
    id: 'secure-management',
    icon: <ShieldIcon />,
    title: 'Secure Management',
    description:
      'Keep your URLs safe with secure authentication. Manage all your shortened links from a protected dashboard with full control.',
  },
  {
    id: 'custom-codes',
    icon: <CodeIcon />,
    title: 'Custom Short Codes',
    description:
      'Create branded, memorable short codes for your links. Choose your own custom aliases to make your URLs stand out.',
  },
]

export function FeaturesSection() {
  return (
    <section className="py-20 bg-base-200/50" aria-labelledby="features-heading">
      <div className="container mx-auto px-4">
        <h2 id="features-heading" className="text-3xl font-bold text-center mb-12">
          Why Choose URLShort?
        </h2>
        <div
          data-testid="features-grid"
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
        >
          {features.map((feature) => (
            <GlassMorphismCard
              key={feature.id}
              testId={`feature-card-${feature.id}`}
              className="flex flex-col items-center text-center"
            >
              <div className="mb-4">{feature.icon}</div>
              <h3 data-testid="feature-title" className="text-xl font-semibold mb-3">
                {feature.title}
              </h3>
              <p data-testid="feature-description" className="text-base-content">
                {feature.description}
              </p>
            </GlassMorphismCard>
          ))}
        </div>
      </div>
    </section>
  )
}
