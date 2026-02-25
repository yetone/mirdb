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
      className="hero min-h-[80vh] bg-gradient-to-br from-primary/10 via-base-100 to-secondary/10"
      data-testid="hero-section"
    >
      <div className="hero-content text-center flex-col gap-8">
        <div className="max-w-2xl">
          <h1 className="text-5xl font-bold text-base-content">
            URL Shortening Service
          </h1>
          <p className="py-6 text-xl text-base-content/80">
            Shorten Links. Track Insights. Share Smarter.
          </p>
        </div>

        {/* URL Shortening Form - Scenario 2 */}
        <div className="w-full max-w-xl">
          <UrlShortenForm />
        </div>

        {/* CTA Buttons */}
        <div className="flex gap-4 flex-wrap justify-center">
          <a href="/register" className="btn btn-secondary" data-testid="signup-button">
            Sign Up Free
          </a>
          <a href="/login" className="btn btn-outline" data-testid="login-button">
            Log In
          </a>
        </div>
      </div>
    </section>
  )
}
