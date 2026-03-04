/**
 * Homepage Component
 * Owner: Scenario 1 - Hero Section Rendering
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
import { HeroSection, CTAFooter } from '../components/home'
import { Navbar } from '../components/Navbar'
import type { HomePageProps } from '../types/home'

export function Home({ showFeatures = true, showFAQ = true }: HomePageProps) {
  return (
    <>
      {/* Navigation Bar */}
      <Navbar />

      <main
        className="min-h-screen bg-base-100 pt-16"
        data-testid="home-page"
        role="main"
        aria-label="Homepage"
      >
        {/* Hero Section - First viewport content */}
        <HeroSection />

        {/* Placeholder sections for anchor navigation */}
        <section id="features-section" data-testid="features-section" className="py-20">
          {/* FeaturesSection (Scenario 3) will replace this placeholder */}
        </section>

        <section id="faq-section" data-testid="faq-section" className="py-20">
          {/* FAQSection (Scenario 4) will replace this placeholder */}
        </section>

        {/* CTA Footer Section */}
        <CTAFooter />
      </main>
    </>
  )
}

export default Home
