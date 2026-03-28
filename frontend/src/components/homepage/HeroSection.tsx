/**
 * Hero Section component.
 * Owner: Scenario 1 - Hero Section Rendering and CTA
 *
 * Requirements:
 * - H1 headline with value proposition
 * - H2 subheading with benefits
 * - Primary CTA button (Get Started / Start Shortening)
 * - Secondary CTA link
 * - Responsive layout (stack on mobile, side-by-side on desktop)
 */

import { Link } from 'react-router-dom'
import type { HeroProps } from '../../types/homepage'

const defaultProps: Required<HeroProps> = {
  headline: 'Shorten URLs, Amplify Your Reach',
  subheading: 'Create branded short links, track engagement, and drive more clicks with our powerful URL shortening platform.',
  primaryCtaText: 'Get Started',
  primaryCtaLink: '/register',
  secondaryCtaText: 'Login',
  secondaryCtaLink: '/login',
}

export function HeroSection(props: HeroProps = {}) {
  const {
    headline,
    subheading,
    primaryCtaText,
    primaryCtaLink,
    secondaryCtaText,
    secondaryCtaLink,
  } = { ...defaultProps, ...props }

  return (
    <section
      className="hero min-h-[70vh] bg-base-200"
      data-testid="hero-section"
    >
      <div className="hero-content text-center py-16 px-4">
        <div className="max-w-2xl">
          <h1 className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6">
            {headline}
          </h1>
          <h2 className="text-lg md:text-xl text-base-content/80 mb-8">
            {subheading}
          </h2>
          <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
            <Link
              to={primaryCtaLink}
              className="btn btn-primary btn-lg"
              data-testid="primary-cta"
            >
              {primaryCtaText}
            </Link>
            <Link
              to={secondaryCtaLink}
              className="btn btn-ghost btn-lg"
              data-testid="secondary-cta"
            >
              {secondaryCtaText}
            </Link>
          </div>
        </div>
      </div>
    </section>
  )
}

export default HeroSection
