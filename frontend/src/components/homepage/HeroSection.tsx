/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Content and Value Proposition
 *
 * Displays the main value proposition with:
 * - Product name/logo
 * - Headline with clear value proposition
 * - Subheadline explaining key benefit
 * - Primary CTA "Get Started Free" button
 * - Secondary CTA "View Demo" button
 *
 * Requirements: REQ-1, REQ-3, US-1, US-2
 */
import { Link } from 'react-router-dom';
import { LinkIcon } from '@heroicons/react/24/outline';

export function HeroSection() {
  return (
    <section
      className="hero min-h-screen bg-base-200"
      aria-labelledby="hero-headline"
      data-testid="hero-section"
      id="hero"
    >
      <div className="hero-content text-center">
        <div className="max-w-2xl">
          {/* Product Logo/Name */}
          <div
            className="flex items-center justify-center gap-3 mb-8"
            data-testid="product-branding"
          >
            <div className="p-3 bg-primary rounded-xl">
              <LinkIcon className="w-8 h-8 text-primary-content" aria-hidden="true" />
            </div>
            <span className="text-2xl md:text-3xl font-bold text-base-content" data-testid="product-name">
              ShortLink
            </span>
          </div>

          {/* Main Headline - Value Proposition */}
          <h1
            id="hero-headline"
            className="text-5xl font-bold"
            data-testid="hero-headline"
          >
            Shorten Links. Track Clicks. Grow Your Reach.
          </h1>

          {/* Subheadline - Key Benefit */}
          <p className="py-6 text-lg" data-testid="hero-subheadline">
            Create short, memorable links in seconds. Get real-time analytics
            and manage all your links from one powerful dashboard.
          </p>

          {/* Call-to-Action Buttons */}
          <div className="flex flex-col sm:flex-row gap-4 justify-center" data-testid="hero-cta-container">
            <Link
              to="/register"
              className="btn btn-primary"
              data-testid="cta-primary"
            >
              Get Started Free
            </Link>
            <Link
              to="/demo"
              className="btn btn-outline"
              data-testid="cta-secondary"
            >
              View Demo
            </Link>
          </div>
        </div>
      </div>
    </section>
  );
}
