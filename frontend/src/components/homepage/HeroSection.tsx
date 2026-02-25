/**
 * HeroSection Component
 * Owner: Scenario 1 (Hero Section Display)
 *
 * Displays the main hero section of the homepage with:
 * - Product name and tagline
 * - URL shortening input form (using UrlShortenForm from Scenario 2)
 * - Primary CTA (Shorten URL) and Secondary CTAs (Sign Up Free, Log In)
 * - Personalized greeting and dashboard access for authenticated users (Scenario 15)
 */

import { Link } from 'react-router-dom'
import { UrlShortenForm } from './UrlShortenForm'

interface HeroSectionProps {
  isAuthenticated?: boolean
  username?: string
  onLogout?: () => void
}

export default function HeroSection({ isAuthenticated = false, username, onLogout }: HeroSectionProps) {
  return (
    <section
      className="hero min-h-[80vh] bg-gradient-to-br from-primary/10 via-base-200 to-secondary/10"
      data-testid="hero-section"
    >
      <div className="hero-content text-center flex-col gap-8 max-w-4xl px-4">
        <div className="max-w-2xl">
          {/* Personalized greeting for authenticated users - Scenario 15 */}
          {isAuthenticated && username && (
            <p
              className="text-lg text-primary mb-2"
              data-testid="user-greeting"
            >
              Welcome back, {username}
            </p>
          )}
          <h1
            className="text-4xl md:text-6xl font-bold text-base-content mb-4"
            data-testid="product-name"
          >
            URL Shortening Service
          </h1>
          <p
            className="text-xl md:text-2xl text-base-content/80 mb-8"
            data-testid="tagline"
          >
            Shorten Links. Track Insights. Share Smarter.
          </p>
        </div>

        {/* URL Shortening Form - Scenario 2 */}
        <div className="w-full max-w-2xl" data-testid="url-form">
          <UrlShortenForm />
        </div>

        {/* CTA Buttons - Different for authenticated vs non-authenticated users */}
        <div className="flex flex-col sm:flex-row gap-4 flex-wrap justify-center mt-4">
          {isAuthenticated ? (
            <>
              <Link
                to="/dashboard"
                className="btn btn-lg bg-[#9d0083] hover:bg-[#800069] text-white border-[#9d0083]"
                data-testid="dashboard-link"
                aria-label="Go to your dashboard"
              >
                Go to Dashboard
              </Link>
              <button
                onClick={onLogout}
                className="btn btn-outline btn-lg"
                data-testid="logout-button"
              >
                Log Out
              </button>
            </>
          ) : (
            <>
              <Link
                to="/register"
                className="btn btn-lg bg-[#9d0083] hover:bg-[#800069] text-white border-[#9d0083]"
                data-testid="signup-button"
                aria-label="Sign up for free account"
              >
                Sign Up Free
              </Link>
              <Link
                to="/login"
                className="btn btn-outline btn-lg"
                data-testid="login-button"
              >
                Log In
              </Link>
            </>
          )}
        </div>
      </div>
    </section>
  )
}
