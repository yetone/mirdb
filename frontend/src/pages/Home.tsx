/**
 * Homepage Page Component
 * Owner: Scenario 8 - Homepage Integration
 *
 * Purpose: Main landing page assembling all homepage sections.
 *
 * Expected sections (in order):
 * 1. Navbar (existing component)
 * 2. HeroSection
 * 3. FeaturesSection
 * 4. HowItWorksSection
 * 5. StatsSection (optional)
 * 6. Footer
 *
 * Expected features:
 * - SEO meta tags
 * - Smooth scroll navigation
 * - Theme support
 * - Responsive layout
 * - BackgroundEffect integration
 */

import { useEffect } from 'react'
import {
  HeroSection,
  FeaturesSection,
  HowItWorksSection,
  Footer,
} from '../components/home'
import { BackgroundEffect } from '../components/BackgroundEffect'

const Home = () => {
  useEffect(() => {
    // Set document title for SEO
    document.title = 'URL Shortener - Shorten URLs, Track Performance, Grow Smarter'

    // Set meta description
    const metaDescription = document.querySelector('meta[name="description"]')
    if (metaDescription) {
      metaDescription.setAttribute(
        'content',
        'Transform long URLs into short, powerful links. Get detailed analytics, track clicks in real-time, and understand your audience better.'
      )
    } else {
      const meta = document.createElement('meta')
      meta.name = 'description'
      meta.content =
        'Transform long URLs into short, powerful links. Get detailed analytics, track clicks in real-time, and understand your audience better.'
      document.head.appendChild(meta)
    }

    // Set Open Graph meta tags for social sharing
    const ogTitle = document.querySelector('meta[property="og:title"]')
    if (!ogTitle) {
      const meta = document.createElement('meta')
      meta.setAttribute('property', 'og:title')
      meta.content = 'URL Shortener - Shorten URLs, Track Performance'
      document.head.appendChild(meta)
    }

    const ogDescription = document.querySelector('meta[property="og:description"]')
    if (!ogDescription) {
      const meta = document.createElement('meta')
      meta.setAttribute('property', 'og:description')
      meta.content =
        'Transform long URLs into short, powerful links with detailed analytics.'
      document.head.appendChild(meta)
    }

    return () => {
      // Reset title when unmounting
      document.title = 'URL Shortener'
    }
  }, [])

  return (
    <div className="min-h-screen bg-base-200" data-testid="home-page">
      {/* Background Effect for visual enhancement */}
      <div className="fixed inset-0 pointer-events-none overflow-hidden -z-10">
        <BackgroundEffect />
      </div>

      {/* Navigation - Note: Navbar is typically rendered at App level in App.tsx */}
      <nav
        className="navbar bg-base-100/80 backdrop-blur-sm sticky top-0 z-50 border-b border-base-content/10"
        data-testid="navbar"
        aria-label="Main navigation"
      >
        <div className="flex-1">
          <a href="/" className="btn btn-ghost text-xl">
            URL Shortener
          </a>
        </div>
        <div className="flex-none gap-2">
          <a href="/login" className="btn btn-ghost btn-sm">
            Login
          </a>
          <a href="/register" className="btn btn-primary btn-sm">
            Sign Up
          </a>
        </div>
      </nav>

      {/* Main Content Area */}
      <main role="main" data-testid="main-content">
        {/* Hero Section - Scenario 1 */}
        <section aria-labelledby="hero-headline">
          <HeroSection />
        </section>

        {/* Features Section - Scenario 2 */}
        <section
          id="features"
          aria-label="Product features"
          data-testid="features-section-wrapper"
        >
          <FeaturesSection />
        </section>

        {/* How It Works Section - Scenario 3 */}
        <section aria-label="How it works">
          <HowItWorksSection />
        </section>
      </main>

      {/* Footer Section - Scenario 7 */}
      <Footer />
    </div>
  )
}

export default Home
