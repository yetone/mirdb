/**
 * HeroSection Component
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 *
 * Renders the hero section of the homepage with headline,
 * subheading, URL shortening form, and CTA button.
 *
 * Expected props:
 * - none (self-contained, uses internal state)
 *
 * Expected exports:
 * - HeroSection: React.FC component
 */
import { Link } from 'react-router-dom';
import type { ReactNode } from 'react';

export interface HeroSectionProps {
  urlShortenerForm?: ReactNode;
}

export default function HeroSection({ urlShortenerForm }: HeroSectionProps) {
  return (
    <section
      data-testid="hero-section"
      aria-label="Hero section"
      className="hero-section px-4 sm:px-6 lg:px-8 py-12 md:py-20 lg:py-24"
    >
      <div className="hero-content max-w-7xl mx-auto">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 lg:gap-12 items-center">
          <div className="hero-text">
            <h1
              data-testid="hero-headline"
              className="hero-headline text-3xl sm:text-4xl md:text-5xl font-bold leading-tight mb-4 md:mb-6"
            >
              Shorten Your URLs in Seconds
            </h1>
            <p
              data-testid="hero-subheading"
              className="hero-subheading text-base sm:text-lg md:text-xl opacity-80 mb-6 md:mb-8 max-w-xl"
            >
              Transform long, unwieldy links into clean, shareable URLs.
              Track clicks, manage your links, and boost your online presence
              with our powerful, easy-to-use URL shortening service.
            </p>

            <div className="hero-cta mb-8 md:mb-0">
              <Link
                to="/register"
                data-testid="hero-cta-button"
                className="cta-button inline-flex items-center justify-center px-6 py-3 min-h-[44px] text-base font-medium rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition-colors"
              >
                Get Started Free
              </Link>
            </div>
          </div>

          {urlShortenerForm && (
            <div data-testid="hero-form-container" className="hero-form">
              {urlShortenerForm}
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
