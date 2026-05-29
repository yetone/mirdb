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
      className="hero-section"
    >
      <div className="hero-content">
        <h1 data-testid="hero-headline">
          Shorten Your URLs in Seconds
        </h1>
        <p data-testid="hero-subheading">
          Transform long, unwieldy links into clean, shareable URLs.
          Track clicks, manage your links, and boost your online presence
          with our powerful, easy-to-use URL shortening service.
        </p>

        {urlShortenerForm && (
          <div data-testid="hero-form-container" className="hero-form">
            {urlShortenerForm}
          </div>
        )}

        <div className="hero-cta">
          <Link
            to="/register"
            data-testid="hero-cta-button"
            className="cta-button"
            role="button"
          >
            Get Started Free
          </Link>
        </div>
      </div>
    </section>
  );
}
