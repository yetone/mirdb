/**
 * HeroSection Component
 * Owner: Scenario 1 (Hero Section Display)
 *
 * Displays the main hero section of the homepage with:
 * - Product name and tagline
 * - URL shortening input form (using UrlShortenForm from Scenario 2)
 * - Primary CTA (Shorten URL) and Secondary CTAs (Sign Up Free, Log In)
 */

import { UrlShortenForm } from './UrlShortenForm'

export default function HeroSection() {
  return (
    <section
      className="hero min-h-[80vh] bg-gradient-to-br from-primary/10 via-base-200 to-secondary/10"
      data-testid="hero-section"
    >
      <div className="hero-content text-center flex-col gap-8 max-w-4xl px-4">
        <div className="max-w-2xl">
          <h1
            className="text-4xl md:text-6xl font-bold text-base-content mb-4"
            data-testid="product-name"
          >
            URL Shortening Service
          </h1>
          <p
            className="text-xl md:text-2xl text-base-content/70 mb-8"
            data-testid="tagline"
          >
            Shorten Links. Track Insights. Share Smarter.
          </p>
        </div>

        {/* URL Shortening Form - Scenario 2 */}
        <div className="w-full max-w-2xl" data-testid="url-form">
          <UrlShortenForm />
        </div>

        {/* CTA Buttons */}
        <div className="flex flex-col sm:flex-row gap-4 flex-wrap justify-center mt-4">
          <a
            href="/register"
            className="btn btn-secondary btn-lg"
            data-testid="signup-button"
          >
            Sign Up Free
          </a>
          <a
            href="/login"
            className="btn btn-outline btn-lg"
            data-testid="login-button"
          >
            Log In
          </a>
        </div>
      </div>
    </section>
  )
}
