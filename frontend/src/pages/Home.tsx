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
import { HeroSection } from '../components/home'
import type { HomePageProps } from '../types/home'

export function Home({ showFeatures = true, showFAQ = true }: HomePageProps) {
  return (
    <main
      className="min-h-screen bg-base-100"
      data-testid="home-page"
      role="main"
      aria-label="Homepage"
    >
      {/* Hero Section - First viewport content */}
      <HeroSection />

      {/* Future sections will be added by other scenarios:
        * - FeaturesSection (Scenario 3)
        * - FAQSection (Scenario 4)
        * - CTAFooter (Scenario 2)
        */}
    </main>
  )
}

export default Home
