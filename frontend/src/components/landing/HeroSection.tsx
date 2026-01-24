/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays the main hero section of the landing page with:
 * - Compelling headline communicating core value
 * - Supporting subheadline with feature summary
 * - Primary CTA button (Get Started Free)
 * - Secondary CTA button (Sign In)
 *
 * Requirements: REQ-1
 */

import React from 'react'
import { HeroSectionProps } from '../../types/landing'
import FuturisticButton from '../FuturisticButton'

const DEFAULT_HEADLINE = 'Shorten URLs. Track Everything.'
const DEFAULT_SUBHEADLINE = 'Create short, memorable links with powerful analytics in seconds. Track clicks, monitor performance, and share your success.'

const DEFAULT_PRIMARY_CTA = {
  text: 'Get Started Free',
  href: '/register',
}

const DEFAULT_SECONDARY_CTA = {
  text: 'Sign In',
  href: '/login',
}

export function HeroSection({
  headline = DEFAULT_HEADLINE,
  subheadline = DEFAULT_SUBHEADLINE,
  primaryCTA = DEFAULT_PRIMARY_CTA,
  secondaryCTA = DEFAULT_SECONDARY_CTA,
}: HeroSectionProps) {
  return (
    <section
      data-testid="hero-section"
      className="min-h-[80vh] flex items-center justify-center px-4 py-16"
      aria-labelledby="hero-headline"
    >
      <div className="max-w-4xl mx-auto text-center">
        <h1
          id="hero-headline"
          data-testid="hero-headline"
          className="text-4xl md:text-5xl lg:text-6xl font-bold text-base-content mb-6 leading-tight"
        >
          {headline}
        </h1>

        <p
          data-testid="hero-subheadline"
          className="text-lg md:text-xl text-base-content/80 mb-10 max-w-2xl mx-auto leading-relaxed"
        >
          {subheadline}
        </p>

        <div
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
          role="group"
          aria-label="Call to action buttons"
        >
          <FuturisticButton
            href={primaryCTA.href}
            variant="primary"
            size="lg"
            data-testid="hero-primary-cta"
          >
            {primaryCTA.text}
          </FuturisticButton>

          <FuturisticButton
            href={secondaryCTA.href}
            variant="secondary"
            size="lg"
            data-testid="hero-secondary-cta"
          >
            {secondaryCTA.text}
          </FuturisticButton>
        </div>
      </div>
    </section>
  )
}

export default HeroSection
