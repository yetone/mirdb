/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Content and Value Proposition
 *
 * Displays the main value proposition with:
 * - Headline with clear value proposition
 * - Subheadline explaining key benefit
 * - Primary CTA "Get Started Free" button
 * - Secondary CTA "View Demo" button
 * - Optional hero graphic/illustration
 *
 * Requirements: REQ-1, REQ-3, US-1, US-2
 */
import { Link } from 'react-router-dom';

export function HeroSection() {
  return (
    <section
      className="hero min-h-screen bg-base-200"
      aria-labelledby="hero-headline"
    >
      <div className="hero-content text-center">
        <div className="max-w-2xl">
          <h1
            id="hero-headline"
            className="text-5xl font-bold"
          >
            Shorten Links. Track Clicks. Grow Your Reach.
          </h1>
          <p className="py-6 text-lg">
            Create short, memorable links in seconds. Get real-time analytics
            and manage all your links from one powerful dashboard.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
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
