import { FeatureCard } from '../components/FeatureCard'
import { FEATURES } from '../constants/features'

/**
 * Homepage / Landing page component.
 * Owner: Scenario 1 - Homepage Hero Section (hero, subheadline, features)
 *
 * Displays the value proposition, URL shortening form, and feature highlights.
 * REQ-1: Clear, compelling headline summarizing value proposition
 * REQ-4: Display 3-5 key product features with icons and descriptions
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
