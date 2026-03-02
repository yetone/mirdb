/**
 * Hero Section Component.
 * Owner: Scenario 1 - Hero Section Display and Content
 *
 * Displays the main hero area with:
 * - Headline: "Shorten Links. Track Clicks. Grow Your Impact."
 * - Subheadline describing the service
 * - URL shortening form (imported from UrlShortenForm)
 * - Primary CTA: "Sign Up Free"
 * - Secondary CTA: "Try as Guest"
 * - Animated visual element
 *
 * Requirements: REQ-1, REQ-3, REQ-4
 * Min height: 600px
 */
import { Link } from 'react-router-dom';
import { Link as LinkIcon, Scissors, BarChart3 } from 'lucide-react';

export function HeroSection() {
  return (
    <section
      className="hero-section flex flex-col items-center justify-center px-4 py-16 md:py-24"
      style={{ minHeight: '600px' }}
      aria-labelledby="hero-headline"
      data-testid="hero-section"
    >
      <div className="max-w-4xl mx-auto text-center">
        {/* Headline */}
        <h1
          id="hero-headline"
          className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 leading-tight"
        >
          Shorten Links. Track Clicks. Grow Your Impact.
        </h1>

        {/* Subheadline */}
        <p className="text-lg md:text-xl text-base-content/70 mb-8 max-w-2xl mx-auto">
          Free URL shortener with powerful analytics. No account required for basic use.
        </p>

        {/* URL Shortening Form Placeholder - will be implemented by Scenario 2 */}
        <div className="mb-8 max-w-xl mx-auto">
          <div className="flex flex-col sm:flex-row gap-2">
            <input
              type="url"
              placeholder="Paste your long URL here..."
              className="input input-bordered input-lg flex-1 w-full"
              aria-label="URL to shorten"
            />
            <button
              type="button"
              className="btn btn-primary btn-lg"
              aria-label="Shorten URL"
            >
              Shorten
            </button>
          </div>
        </div>

        {/* CTA Buttons */}
        <div className="flex flex-col sm:flex-row gap-4 justify-center items-center mb-12">
          <Link
            to="/register"
            className="btn btn-primary btn-lg"
            role="button"
          >
            Sign Up Free
          </Link>
          <button
            type="button"
            className="btn btn-outline btn-lg"
            role="button"
          >
            Try as Guest
          </button>
        </div>

        {/* Visual Element - Animated Icon Transformation */}
        <div
          className="flex items-center justify-center gap-4 text-base-content/50"
          aria-hidden="true"
        >
          <div className="flex items-center gap-2">
            <LinkIcon className="w-8 h-8" />
            <span className="text-sm">Long URL</span>
          </div>
          <Scissors className="w-6 h-6 text-primary" />
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-primary">Short Link</span>
            <BarChart3 className="w-8 h-8 text-primary" />
          </div>
        </div>
      </div>
    </section>
  );
}
