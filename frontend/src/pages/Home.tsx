/**
 * Homepage Component
 * Owner: Scenario 1 - Homepage Accessibility and Routing
 *
 * This is the main landing page component that renders at the root path (/).
 * It composes HeroSection, FeaturesSection, and Footer components.
 *
 * Expected exports:
 * - Home: React.FC - Main homepage component
 *
 * Dependencies:
 * - HeroSection from '../components/home/HeroSection'
 * - FeaturesSection from '../components/home/FeaturesSection' (when available)
 * - Footer from '../components/home/Footer' (when available)
 */

import { HeroSection } from '../components/home/HeroSection'

export function Home() {
  return (
    <div className="min-h-screen bg-base-100">
      <main>
        {/* Hero Section */}
        <HeroSection />

        {/* Features Section - Placeholder for Scenario 6 */}
        <section
          className="py-16 px-4"
          aria-label="Features section"
          data-testid="features-section"
        >
          <div className="max-w-6xl mx-auto text-center">
            <h2 className="text-3xl font-bold mb-8">Features</h2>
            <p className="text-base-content/70">
              Powerful URL shortening with analytics tracking.
            </p>
          </div>
        </section>

        {/* Footer - Placeholder for Scenario 12 */}
        <footer
          className="py-8 px-4 bg-base-200"
          aria-label="Footer"
          data-testid="footer-section"
        >
          <div className="max-w-6xl mx-auto text-center text-base-content/70">
            <p>&copy; {new Date().getFullYear()} URL Shortener. All rights reserved.</p>
          </div>
        </footer>
      </main>
    </div>
  )
}

export default Home
