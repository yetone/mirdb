/**
 * Homepage Component
 * Owner: Scenario 1 - Hero Section Rendering
 * Modified by: Scenario 6 - Responsive Design
 *
 * Main landing page for the URL Shortener service.
 * Integrates all homepage sections and existing components.
 *
 * Requirements:
 * - REQ-1: Clear value proposition in first viewport
 * - REQ-7: Theme adaptation
 * - REQ-8: Responsive design
 * - NFR-1: WCAG 2.1 AA accessibility
 * - NFR-5: SEO optimization
 */
import { HeroSection, CTAFooter, FeaturesSection, FAQSection } from '../components/home'
import { Navbar } from '../components/Navbar'
import type { HomePageProps } from '../types/home'

export function Home({ showFeatures = true, showFAQ = true }: HomePageProps) {
  return (
    <>
      {/* Navigation Bar */}
      <Navbar />

      <main
        className="min-h-screen bg-base-100 pt-16 overflow-x-hidden"
        data-testid="home-page"
        role="main"
        aria-label="Homepage"
      >
        {/* Hero Section - First viewport content */}
        <HeroSection />

        {/* Features Section - Responsive grid layout */}
        {showFeatures && (
          <div id="features-section">
            <FeaturesSection className="px-4 sm:px-6 lg:px-8" />
          </div>
        )}

        {/* FAQ Section - Responsive accordion */}
        {showFAQ && (
          <div id="faq-section">
            <FAQSection className="px-4 sm:px-6 lg:px-8" />
          </div>
        )}

        {/* CTA Footer Section */}
        <CTAFooter />
      </main>
    </>
  )
}

export default Home
