/**
 * Home Page (Landing Page)
 * Owner: Scenario 14 - Integration with Existing Components
 *
 * Main landing page that assembles all landing components:
 * - Navbar (existing component)
 * - BackgroundEffect (existing component)
 * - HeroSection (uses FuturisticButton)
 * - FeaturesSection (uses GlassMorphismCard)
 * - HowItWorksSection
 * - SocialProofSection (uses GlassMorphismCard)
 * - CTASection (uses FuturisticButton)
 * - Footer
 *
 * All components integrate with ThemeContext for theming support.
 */

import React from 'react'
import Navbar from '../components/Navbar'
import BackgroundEffect from '../components/BackgroundEffect'
import {
  HeroSection,
  FeaturesSection,
  HowItWorksSection,
  SocialProofSection,
  CTASection,
  Footer,
} from '../components/landing'

export function Home() {
  return (
    <div className="min-h-screen bg-base-100">
      <Navbar />
      <BackgroundEffect />
      <main>
        <HeroSection />
        <FeaturesSection />
        <HowItWorksSection />
        <SocialProofSection />
        <CTASection />
      </main>
      <Footer />
    </div>
  )
}

export default Home
