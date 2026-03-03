/**
 * Hero Section Component
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 *
 * Displays the main hero content at the top of the homepage.
 *
 * Requirements:
 * - Large centered headline with service name
 * - Subheadline with value proposition
 * - "Get Started" primary button -> /register
 * - "Sign In" secondary button -> /login
 * - Theme-aware styling
 */

import { Link } from 'react-router-dom';
import { FuturisticButton } from './FuturisticButton';

interface HeroSectionProps {
  serviceName?: string;
  tagline?: string;
}

export function HeroSection({
  serviceName = 'URL Shortener',
  tagline = 'Shorten URLs, track clicks, analyze your audience',
}: HeroSectionProps) {
  return (
    <section
      className="min-h-[80vh] flex flex-col items-center justify-center text-center px-4 py-20"
      data-testid="hero-section"
      role="banner"
      aria-label="Hero section"
    >
      <div className="max-w-4xl mx-auto">
        {/* Service Name / Main Headline */}
        <h1
          className="text-4xl sm:text-5xl md:text-6xl lg:text-7xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
          data-testid="hero-headline"
        >
          {serviceName}
        </h1>

        {/* Tagline / Subheadline */}
        <p
          className="text-lg sm:text-xl md:text-2xl text-base-content/80 mb-10 max-w-2xl mx-auto"
          data-testid="hero-tagline"
        >
          {tagline}
        </p>

        {/* CTA Buttons */}
        <div
          className="flex flex-col sm:flex-row gap-4 justify-center items-center"
          data-testid="hero-cta-buttons"
        >
          {/* Get Started - Primary CTA */}
          <Link to="/register" data-testid="get-started-link">
            <FuturisticButton
              variant="primary"
              size="lg"
              className="min-w-[180px]"
              data-testid="get-started-button"
              aria-label="Get started with URL Shortener"
            >
              Get Started
            </FuturisticButton>
          </Link>

          {/* Sign In - Secondary CTA */}
          <Link to="/login" data-testid="sign-in-link">
            <FuturisticButton
              variant="outline"
              size="lg"
              className="min-w-[180px]"
              data-testid="sign-in-button"
              aria-label="Sign in to your account"
            >
              Sign In
            </FuturisticButton>
          </Link>
        </div>

        {/* Additional Value Proposition */}
        <p className="mt-8 text-sm text-base-content">
          No credit card required. Start shortening URLs in seconds.
        </p>
      </div>
    </section>
  );
}
