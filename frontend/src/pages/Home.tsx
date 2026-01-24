/**
 * Home Page (Landing Page)
 *
 * Main landing page that assembles all landing components.
 * Currently includes HeroSection - other sections will be added by their respective scenarios.
 */

import React from 'react'
import Navbar from '../components/Navbar'
import BackgroundEffect from '../components/BackgroundEffect'
import { HeroSection } from '../components/landing'

export function Home() {
  return (
    <div className="min-h-screen bg-base-100">
      <Navbar />
      <BackgroundEffect />
      <main>
        <HeroSection />
        {/* Other sections will be added by their respective scenarios:
         * - FeaturesSection (Scenario 2)
         * - HowItWorksSection (Scenario 4)
         * - SocialProofSection (Scenario 5)
         * - CTASection (Scenario 6)
         * - Footer (Scenario 9)
         */}
      </main>
    </div>
  )
}

export default Home
