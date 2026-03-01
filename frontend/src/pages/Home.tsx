import React from 'react'
import { FeaturesSection, HeroSection } from '../components/homepage'
import Navbar from '../components/Navbar'
import BackgroundEffect from '../components/BackgroundEffect'

/**
 * Homepage / Landing Page
 * Owner: Scenario 1 - Hero Section Display and Content
 *
 * This is the main landing page for the URL Shortening Service.
 * It serves as the entry point for new visitors and should:
 * - Display the product value proposition
 * - Showcase core features
 * - Provide clear CTAs for registration/login
 *
 * Expected sections:
 * - Hero section with tagline and CTAs (Scenario 1)
 * - Features section with feature cards (Scenario 2)
 * - URL preview/demo section (Scenario 9)
 * - Footer with copyright (Scenario 6)
 *
 * Uses existing components:
 * - BackgroundEffect for visual appeal
 * - GlassMorphismCard for feature cards
 * - FuturisticButton for CTAs
 * - Navbar for navigation (via layout)
 */
export const Home: React.FC = () => {
  return (
    <BackgroundEffect>
      <Navbar />
      <div className="home-page min-h-screen" data-testid="home-page">
        {/* Hero Section - Owned by Scenario 1 */}
        <HeroSection />

        {/* Features Section - Owned by Scenario 2 */}
        <FeaturesSection />

        {/* URL Preview Section - Owned by Scenario 9 (stub) */}
        <section className="url-preview-section py-16 px-4" data-testid="url-preview-section">
          {/* UrlPreview component will be added by Scenario 9 */}
        </section>

        {/* Footer - Owned by Scenario 6 (stub) */}
        <footer className="footer-section py-8 px-4 text-center" data-testid="footer-section">
          {/* Footer component will be added by Scenario 6 */}
        </footer>
      </div>
    </BackgroundEffect>
  )
}

export default Home
