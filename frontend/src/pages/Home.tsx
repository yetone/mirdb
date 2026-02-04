import { Link } from 'react-router-dom'
import { FeatureCard } from '../components/FeatureCard'
import { FEATURES } from '../constants/features'

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
 */
function Home() {
  return (
    <div className="min-h-screen bg-base-100">
      {/* Skip to main content for accessibility */}
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50 focus:p-2 focus:bg-primary focus:text-primary-content focus:rounded"
      >
        Skip to main content
      </a>

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
