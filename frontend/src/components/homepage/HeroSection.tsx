/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays the main hero area with:
 * - Headline tagline about URL shortening
 * - Brief 2-3 sentence description
 * - Primary CTA button (Shorten URL)
 * - Secondary CTA button (Sign Up / Learn More)
 * - Gradient/pattern background
 *
 * Requirements: REQ-1, REQ-3
 */

import type { HeroProps } from '../../types/homepage'

export function HeroSection({ onPrimaryClick, onSecondaryClick }: HeroProps) {
  return (
    <section
      data-testid="hero-section"
      className="min-h-[80vh] flex items-center justify-center bg-gradient-to-br from-primary/10 via-base-100 to-secondary/10"
    >
      <div className="container mx-auto px-4 text-center">
        <h1 className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6">
          Shorten Your URLs,{' '}
          <span className="text-primary">Amplify Your Reach</span>
        </h1>

        <p
          data-testid="hero-description"
          className="text-lg md:text-xl text-base-content/80 max-w-2xl mx-auto mb-8"
        >
          Transform long, unwieldy links into clean, shareable URLs in seconds.
          Track clicks, analyze performance, and gain insights into your audience.
          Start shortening for free, no account required.
        </p>

        <div className="flex flex-col sm:flex-row gap-4 justify-center">
          <button
            data-testid="primary-cta"
            onClick={onPrimaryClick}
            className="btn btn-primary btn-lg"
          >
            Shorten URL
          </button>

          <button
            data-testid="secondary-cta"
            onClick={onSecondaryClick}
            className="btn btn-outline btn-lg"
          >
            Sign Up Free
          </button>
        </div>
      </div>
    </section>
  )
}

export default HeroSection
