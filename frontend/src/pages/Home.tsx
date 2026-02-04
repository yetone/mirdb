import { Link } from 'react-router-dom'
import { FeatureCard } from '../components/FeatureCard'
import { FEATURES } from '../constants/features'
import { useTheme } from '../contexts/ThemeContext'

/**
 * Homepage / Landing page component.
 * Owner: Scenario 1 - Homepage Hero Section (hero, subheadline, features)
 * Owner: Scenario 4 - Navigation and CTAs (Login link, Get Started CTA)
 *
 * Displays the value proposition, URL shortening form, and feature highlights.
 * REQ-1: Clear, compelling headline summarizing value proposition
 * REQ-2: Homepage shall include a prominent CTA button leading to registration
 * REQ-4: Display 3-5 key product features with icons and descriptions
 * REQ-5: Homepage shall include navigation links to Login and Register pages
 * REQ-7: Homepage shall support dark mode and inherit the application's current theme
 * REQ-10: Homepage shall support theme toggle button consistent with app-wide design
 */
function Home() {
  const { theme, toggleTheme } = useTheme()
  return (
    <div className="min-h-screen bg-base-100">
      {/* Skip to main content for accessibility */}
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50 focus:p-2 focus:bg-primary focus:text-primary-content focus:rounded"
      >
        Skip to main content
      </a>

      {/* Theme Toggle - REQ-10: Theme toggle button consistent with app-wide design */}
      <header className="navbar bg-base-100/80 backdrop-blur-sm sticky top-0 z-40">
        <div className="container mx-auto flex justify-end px-4">
          <button
            onClick={toggleTheme}
            className="btn btn-ghost btn-circle transition-colors duration-200"
            aria-label={`Switch to ${theme === 'light' ? 'dark' : 'light'} theme`}
            data-testid="theme-toggle"
          >
            {theme === 'light' ? (
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-6 w-6"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"
                />
              </svg>
            ) : (
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="h-6 w-6"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"
                />
              </svg>
            )}
          </button>
        </div>
      </header>

      {/* Hero Section */}
      <main id="main-content" className="container mx-auto px-4 py-16">
        <section
          className="text-center max-w-4xl mx-auto mb-16"
          aria-labelledby="hero-headline"
        >
          {/* Hero Headline - REQ-1 */}
          <h1
            id="hero-headline"
            className="text-4xl md:text-5xl lg:text-6xl font-bold mb-6 text-base-content"
          >
            Shorten URLs. Track Clicks.
          </h1>

          {/* Subheadline */}
          <p
            className="text-xl md:text-2xl text-base-content/70 mb-8"
            data-testid="hero-subheadline"
          >
            The simple way to manage your links. Create short, memorable URLs and
            track every click with powerful analytics.
          </p>

          {/* Placeholder for URL shortening form (Scenario 2) */}
          <div className="max-w-2xl mx-auto mb-8" aria-label="URL shortening form area">
            {/* UrlShortenerForm will be added by Scenario 2 */}
          </div>

          {/* Navigation and CTAs - Scenario 4 */}
          {/* REQ-2: Prominent CTA button leading to registration */}
          {/* REQ-5: Navigation links to Login and Register pages */}
          <nav
            className="flex flex-wrap justify-center items-center gap-4"
            aria-label="Primary navigation"
          >
            <Link
              to="/register"
              className="btn btn-primary btn-lg font-semibold px-8"
              data-testid="cta-get-started"
            >
              Get Started Free
            </Link>
            <Link
              to="/login"
              className="btn btn-outline btn-lg font-semibold"
              data-testid="cta-login"
            >
              Login
            </Link>
          </nav>
        </section>

        {/* Features Section - REQ-4 */}
        <section
          className="py-16"
          aria-labelledby="features-heading"
        >
          <h2
            id="features-heading"
            className="text-3xl font-bold text-center mb-12 text-base-content"
          >
            Why Choose Us?
          </h2>

          <div
            className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
            role="list"
            aria-label="Product features"
          >
            {FEATURES.map((feature) => (
              <div key={feature.id} role="listitem">
                <FeatureCard
                  icon={feature.icon}
                  title={feature.title}
                  description={feature.description}
                />
              </div>
            ))}
          </div>
        </section>
      </main>
    </div>
  )
}

export default Home
